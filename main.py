import streamlit as st
import pandas as pd
import requests
import re
import unicodedata
import os
import ast
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="Evoliz Sync - V10.5", layout="wide", page_icon="🛡️")

# --- FONCTIONS DE NETTOYAGE ---
def to_clean_str(val):
    if pd.isna(val) or val == "":
        return ""
    s = str(val).strip()
    if s.endswith('.0'):
        return s[:-2]
    return s

def norm_piv(s): 
    if not s: return ""
    s = str(s).upper().strip()
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s[:20]

def clean_label_tva(label, code):
    if str(code).startswith(('2', '6', '7')):
        s = str(label)
        # Supprimer les taux de TVA (20%, 5,5 %, etc.) avec les signes autour (tirets, parenthèses, slashes)
        s = re.sub(r'[\s\-/\(]*\d+[\.,]?\d*\s?%[\s\-/\)]*', ' ', s, flags=re.IGNORECASE)
        # Supprimer les mentions TVA, EXONERE, EXO avec les signes autour
        s = re.sub(r'[\s\-/\(]*(TVA|EXON[ÉEée]R[ÉEée][EeSs]?|EXO)[\s\-/\)]*', ' ', s, flags=re.IGNORECASE)
        # Supprimer les mots résiduels type EX... en fin de chaîne
        s = re.sub(r'[\s\-/]*\bEX\w*\s*$', '', s, flags=re.IGNORECASE)
        # Nettoyer les signes orphelins en début/fin et les espaces multiples
        s = re.sub(r'^[\s\-/\(\)]+|[\s\-/\(\)]+$', '', s)
        return " ".join(s.split()).upper()
    return str(label).strip().upper()

# --- 3. MOTEUR API ---
def get_correct_id(item, endpoint):
    if endpoint in ["purchase-classifications", "sale-classifications"]:
        return item.get('classificationid') or item.get('id')
    if "affectations" in endpoint:
        return item.get('affectationid') or item.get('id')
    if endpoint == "accounts":
        return item.get('accountid') or item.get('id')
    return item.get('id')

def fetch_evoliz_data(endpoint, headers):
    results = {}
    url = f"https://www.evoliz.io/api/v1/{endpoint}"
    params = {"per_page": 100, "page": 1}
    is_account = endpoint == "accounts"
    try:
        while True:
            r = requests.get(url, headers=headers, params=params, timeout=15)
            if r.status_code != 200:
                break
            data = r.json()
            for item in data.get('data', []):
                item_id = get_correct_id(item, endpoint)
                code = to_clean_str(item.get('code'))
                label = str(item.get('label') or item.get('category_name') or code).strip().upper()
                # accountid : dans account imbriqué (account.accountid)
                acc_id = item.get('accountid')
                if acc_id is None and isinstance(item.get('account'), dict):
                    acc_id = item['account'].get('accountid')
                # vataccount : le compte TVA est dans vataccount.accountid (pas vataccountid !)
                vat_id = None
                if isinstance(item.get('vataccount'), dict):
                    vat_id = item['vataccount'].get('accountid')
                entry = {"id": item_id, "label": label, "code": code, "acc_id": acc_id, "vat_id": vat_id, "_raw_keys": list(item.keys())}
                pivot_code = norm_piv(code) if code else None
                pivot_label = norm_piv(label)
                if is_account:
                    # Comptes : indexer uniquement par code (pas de doublon)
                    if pivot_code:
                        results[pivot_code] = entry
                else:
                    # Flux : indexer par code ET label pour permettre le matching
                    if pivot_code:
                        results[pivot_code] = entry
                    if pivot_label and pivot_label != pivot_code:
                        results[pivot_label] = entry
            if params['page'] >= data.get('meta', {}).get('last_page', 1):
                break
            params['page'] += 1
    except Exception as e:
        st.warning(f"Erreur API ({endpoint}): {e}")
    return results


# =========================================================
# EVOLIZ SYNC V10.5
# =========================================================

FLUX_ENDPOINTS = {
    "ACHAT": "purchase-classifications",
    "VENTE": "sale-classifications",
    "ENTRÉE BQ": "sale-affectations",
    "SORTIE BQ": "purchase-affectations",
}

def count_unique(data_dict):
    """Compte les éléments uniques par id dans un dict potentiellement double-indexé"""
    return len({d['id'] for d in data_dict.values()})

def has_movement(row, flux_cols):
    for c in flux_cols:
        val = pd.to_numeric(row[c], errors='coerce')
        if pd.notna(val) and abs(val) > 0:
            return True
    return False

def inject_account(code, label, headers):
    try:
        r = requests.post("https://www.evoliz.io/api/v1/accounts",
                          headers=headers, json={"code": code, "label": label}, timeout=15)
        if r.status_code in (200, 201):
            return True, r.json()
        # Si le compte existe déjà, considérer comme succès
        if r.status_code == 400 and "already been taken" in r.text:
            return True, f"Déjà existant (code {code})"
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)

def patch_evoliz_item(category, item_id, payload, headers, company_id=None):
    endpoint = ERAZ_ENDPOINTS[category]
    if company_id:
        url = f"https://www.evoliz.io/api/v1/companies/{company_id}/{endpoint}/{item_id}"
    else:
        url = f"https://www.evoliz.io/api/v1/{endpoint}/{item_id}"
    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=15)
        if r.status_code in (200, 204):
            return True, r.json() if r.status_code == 200 else "OK"
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)

def inject_flux(flux_type, code, label, headers, vat_id=None, acc_id=None):
    endpoint = FLUX_ENDPOINTS[flux_type]
    # Le code d'une catégorie/affectation est son libellé (tronqué à 50 car pour l'API)
    payload = {"code": label[:50], "label": label}
    if acc_id and not (isinstance(acc_id, float) and pd.isna(acc_id)):
        payload["accountid"] = int(acc_id)
    if vat_id and not (isinstance(vat_id, float) and pd.isna(vat_id)):
        # L'API Evoliz attend le champ "vataccountid" pour lier un compte TVA
        payload["vataccountid"] = int(vat_id)
    # Debug : log du payload envoyé
    _payload_debug = {k: v for k, v in payload.items() if k not in ('code', 'label')}
    try:
        r = requests.post(f"https://www.evoliz.io/api/v1/{endpoint}",
                          headers=headers, json=payload, timeout=15)
        if r.status_code in (200, 201):
            resp = r.json()
            resp['_sent'] = _payload_debug
            return True, resp
        return False, f"HTTP {r.status_code} [sent:{_payload_debug}]: {r.text[:150]}"
    except Exception as e:
        return False, str(e)

ERAZ_ENDPOINTS = {
    "COMPTE": "accounts",
    "ACHAT": "purchase-classifications",
    "VENTE": "sale-classifications",
    "ENTRÉE BQ": "sale-affectations",
    "SORTIE BQ": "purchase-affectations",
}

def delete_evoliz_item(category, item_id, headers, company_id=None):
    endpoint = ERAZ_ENDPOINTS[category]
    try:
        r = requests.delete(f"https://www.evoliz.io/api/v1/{endpoint}/{item_id}",
                            headers=headers, timeout=15)
        if r.status_code in (200, 204):
            return True, "Supprimé"
        # Si le DELETE échoue (flux utilisé), tenter un PATCH enabled=false
        if category != "COMPTE" and r.status_code in (400, 409, 422):
            patch_url = f"https://www.evoliz.io/api/v1/{endpoint}/{item_id}"
            if company_id:
                patch_url = f"https://www.evoliz.io/api/v1/companies/{company_id}/{endpoint}/{item_id}"
            try:
                r2 = requests.patch(patch_url, headers=headers,
                                    json={"enabled": False}, timeout=15)
                if r2.status_code in (200, 204):
                    return True, "Désactivé (enabled=false)"
                return False, f"DEL HTTP {r.status_code} + PATCH HTTP {r2.status_code}: {r2.text[:150]}"
            except Exception as e2:
                return False, f"DEL HTTP {r.status_code} + PATCH erreur: {e2}"
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)

st.title("🛡️ Evoliz Sync — V10.5")

for key, default in [('nr_v62', pd.DataFrame()), ('audit_matrix_105', pd.DataFrame()),
                         ('rejets_105', pd.DataFrame()), ('prot_105', set()), ('sync_log', []),
                         ('ev_acc_105', {}), ('ev_data_105', {"ACHAT": {}, "VENTE": {}, "ENTRÉE BQ": {}, "SORTIE BQ": {}}),
                         ('token_headers_105', {}), ('company_id_105', None), ('eraz_log', []),
                         ('eraz_counts', {"COMPTE": 0, "ACHAT": 0, "VENTE": 0, "ENTRÉE BQ": 0, "SORTIE BQ": 0}),
                         ('eraz_items', {})]:
    if key not in st.session_state:
        st.session_state[key] = default

APP_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_PATH = os.path.join(APP_DIR, ".evoliz_creds.json")
PARAM_PATH = os.path.join(APP_DIR, "param_local.csv")
BALANCE_PATH_FILE = os.path.join(APP_DIR, ".last_balance_path.txt")

def save_creds(pk, sk):
    import json as _json
    with open(CREDS_PATH, 'w') as f:
        _json.dump({"pk": pk, "sk": sk}, f)

def load_creds():
    import json as _json
    if os.path.exists(CREDS_PATH):
        with open(CREDS_PATH) as f:
            d = _json.load(f)
            return d.get("pk", ""), d.get("sk", "")
    return "", ""

def save_balance_path(path):
    if os.path.isfile(path):
        with open(BALANCE_PATH_FILE, 'w') as f:
            f.write(path)

def load_balance_path():
    if os.path.exists(BALANCE_PATH_FILE):
        with open(BALANCE_PATH_FILE) as f:
            p = f.read().strip()
            if os.path.isfile(p):
                return p
    return ""

m2, m1, m4, m6, m7 = st.tabs([
    "📂 1. Balance & Clés API", "⚙️ 2. Param",
    "🔍 3. Matrice", "📊 4. Synthèse", "🚀 5. Synchro"
])




def load_param_from_excel(file_obj):
    raw_p = pd.read_excel(file_obj, sheet_name="Param", header=None)
    df_p = pd.DataFrame()
    df_p['Racine'] = raw_p.iloc[1:, 0].apply(to_clean_str)
    df_p['Libellé'] = raw_p.iloc[1:, 1]
    df_p['_tags'] = raw_p.iloc[1:].apply(
        lambda r: [norm_piv(r.iloc[i]) for i in range(7, 17) if not pd.isna(r.iloc[i])],
        axis=1
    )
    for flux, tag in [("ACHAT", "ACHAT"), ("VENTE", "VENTE"), ("ENTRÉE BQ", "ENTREE"), ("SORTIE BQ", "SORTIE")]:
        df_p[flux] = df_p['_tags'].apply(lambda tags, t=tag: t in tags)
    df_p = df_p.drop(columns=['_tags'])
    return df_p.dropna(subset=['Racine']).reset_index(drop=True)

def save_param_local(df):
    df.to_csv(PARAM_PATH, index=False)

def load_param_local():
    if os.path.exists(PARAM_PATH):
        return pd.read_csv(PARAM_PATH)
    return pd.DataFrame()

with m1:
    st.subheader("⚙️ Racines & Tags de flux")

    # Chargement auto depuis le fichier local au démarrage
    if st.session_state.nr_v62.empty and os.path.exists(PARAM_PATH):
        st.session_state.nr_v62 = load_param_local()

    # Import depuis Excel
    f_param = st.file_uploader("Importer depuis un Excel (onglet Param)", type=['xlsm', 'xlsx', 'xls'], key="f_param_import")
    if f_param:
        try:
            xl_p = pd.ExcelFile(f_param)
            if "Param" in xl_p.sheet_names:
                st.session_state.nr_v62 = load_param_from_excel(f_param)
                save_param_local(st.session_state.nr_v62)
                st.success(f"{len(st.session_state.nr_v62)} racines importées et sauvegardées localement")
            else:
                st.error("Onglet 'Param' introuvable dans ce fichier")
        except Exception as e:
            st.error(f"Erreur de lecture : {e}")

    # Affichage / édition
    st.session_state.nr_v62 = st.data_editor(
        st.session_state.nr_v62, num_rows="dynamic", use_container_width=True
    )

    # Sauvegarde manuelle après édition
    if st.button("💾 Sauvegarder les paramètres", key="btn_save_param"):
        save_param_local(st.session_state.nr_v62)
        st.success(f"Paramètres sauvegardés dans {PARAM_PATH}")

    # Téléchargement des paramètres
    if not st.session_state.nr_v62.empty:
        st.download_button(
            "📥 Télécharger les paramètres (CSV)",
            data=st.session_state.nr_v62.to_csv(index=False),
            file_name="param_local.csv",
            mime="text/csv",
            key="btn_download_param"
        )

    if os.path.exists(PARAM_PATH):
        st.caption(f"📁 Fichier local : {PARAM_PATH}")

with m2:
    # --- Clés API ---
    st.subheader("🔑 Connexion Evoliz")
    saved_pk, saved_sk = load_creds()
    col_pk, col_sk = st.columns(2)
    pk_105 = col_pk.text_input("Public Key", value=saved_pk, key="pk_105")
    sk_105 = col_sk.text_input("Secret Key", type="password", value=saved_sk, key="sk_105")

    auto_connect = not st.session_state.token_headers_105 and saved_pk and saved_sk
    if auto_connect or st.button("🔗 CONNECTER & LIRE API", type="primary", use_container_width=True, key="btn_connect_105"):
        if pk_105 and sk_105:
            save_creds(pk_105, sk_105)
            with st.spinner("Connexion à Evoliz en cours..."):
                try:
                    r_log = requests.post("https://www.evoliz.io/api/login",
                                          json={"public_key": pk_105, "secret_key": sk_105}, timeout=15)
                except Exception as e:
                    st.error(f"Erreur de connexion : {e}")
                    r_log = None
            if r_log and r_log.status_code in (429, 500, 502, 503, 504):
                st.warning(f"API Evoliz temporairement indisponible (HTTP {r_log.status_code}). Réessayez dans quelques instants.")
            elif r_log and r_log.status_code == 200:
                login_data = r_log.json()
                h = {"Authorization": f"Bearer {login_data.get('access_token')}", "Accept": "application/json"}
                st.session_state.token_headers_105 = h
                cid = login_data.get('companyid')
                if not cid:
                    try:
                        r_co = requests.get("https://www.evoliz.io/api/v1/companies", headers=h, timeout=15)
                        if r_co.status_code == 200:
                            companies = r_co.json().get('data', [])
                            if companies:
                                cid = companies[0].get('companyid') or companies[0].get('id')
                    except:
                        pass
                st.session_state.company_id_105 = cid
                with st.spinner("Lecture des données Evoliz..."):
                    st.session_state.ev_acc_105 = fetch_evoliz_data("accounts", h)
                    st.session_state.ev_data_105 = {
                        "ACHAT": fetch_evoliz_data("purchase-classifications", h),
                        "VENTE": fetch_evoliz_data("sale-classifications", h),
                        "ENTRÉE BQ": fetch_evoliz_data("sale-affectations", h),
                        "SORTIE BQ": fetch_evoliz_data("purchase-affectations", h),
                    }
                st.success(f"Connecté à Evoliz (company: {cid})")
            elif r_log:
                st.error(f"Échec login : HTTP {r_log.status_code}")

    if st.session_state.ev_acc_105:
        with st.expander("📊 Données lues depuis Evoliz"):
            st.metric("Comptes (PCC)", len(st.session_state.ev_acc_105))
            c1, c2 = st.columns(2)
            c1.metric("Achats", count_unique(st.session_state.ev_data_105["ACHAT"]))
            c2.metric("Ventes", count_unique(st.session_state.ev_data_105["VENTE"]))
            c1.metric("Entrées BQ", count_unique(st.session_state.ev_data_105["ENTRÉE BQ"]))
            c2.metric("Sorties BQ", count_unique(st.session_state.ev_data_105["SORTIE BQ"]))

    # --- Balance ---
    st.divider()
    st.subheader("📂 Balance")
    last_bal_path = load_balance_path()
    bal_mode = st.radio("Source Balance", ["📁 Fichier local (chemin)", "📤 Upload"], horizontal=True, key="bal_mode")

    f105 = None
    if bal_mode == "📁 Fichier local (chemin)":
        bal_path = st.text_input("Chemin du fichier Balance", value=last_bal_path, key="bal_path_input")
        if bal_path and os.path.exists(bal_path):
            save_balance_path(bal_path)
            f105 = bal_path
        elif bal_path:
            st.error(f"Fichier introuvable : {bal_path}")
    else:
        f105 = st.file_uploader("Fichier Balance", type=['xlsm', 'xlsx', 'xls'], key="f105_v62")

    if f105:
        xl = None
        # Essayer plusieurs engines pour supporter tous les formats Excel
        for engine, kwargs in [(None, {}), ('openpyxl', {}), ('xlrd', {}), ('xlrd', {'engine_kwargs': {'ignore_workbook_corruption': True}})]:
            try:
                if hasattr(f105, 'seek'):
                    f105.seek(0)
                xl = pd.ExcelFile(f105, engine=engine, **kwargs)
                break
            except Exception:
                pass
        # Fallback : lire comme HTML (certains .xls sont du HTML déguisé)
        if xl is None:
            try:
                if hasattr(f105, 'seek'):
                    f105.seek(0)
                raw = f105.read() if hasattr(f105, 'read') else open(f105, 'rb').read()
                dfs = pd.read_html(raw, header=0)
                if dfs:
                    st.session_state._bal_html_fallback = dfs
                    xl = "HTML"
            except Exception:
                pass
        if xl is None:
            st.error("Impossible de lire ce fichier. Essayez de le ré-enregistrer en .xlsx depuis Excel.")
            st.stop()
        if xl == "HTML":
            sheets = [f"Feuille {i+1}" for i in range(len(st.session_state._bal_html_fallback))]
            sheet_bal = st.selectbox("Onglet Balance", sheets)
            sheet_idx = sheets.index(sheet_bal)
            df_bal_preview = st.session_state._bal_html_fallback[sheet_idx]
        else:
            sheet_bal = st.selectbox("Onglet Balance", xl.sheet_names)
            # Lecture via l'objet ExcelFile déjà ouvert (évite de ré-ouvrir le fichier)
            df_bal_preview = xl.parse(sheet_name=sheet_bal)
        cols_preview = df_bal_preview.columns.tolist()
        comptes_4456 = []
        for _, row in df_bal_preview.iterrows():
            code = to_clean_str(row[cols_preview[0]])
            if code.startswith('4456'):
                label = str(row[cols_preview[1]]).strip().upper()
                comptes_4456.append({"code": code, "label": f"{code} - {label}"})

        st.divider()
        st.subheader("🏷️ Comptes TVA pour catégories d'achat")
        options_4456 = ["— Aucun"] + [c['label'] for c in comptes_4456]

        col_tva1, col_tva2 = st.columns(2)
        with col_tva1:
            sel_tva_6 = st.selectbox("TVA Achats (comptes 6xx)", options_4456, key="sel_tva_6")
        with col_tva2:
            sel_tva_2 = st.selectbox("TVA Investissements (comptes 2xx)", options_4456, key="sel_tva_2")

        vat_label_6, vat_label_2 = None, None
        vat_api_id_6, vat_api_id_2 = None, None
        if sel_tva_6 != "— Aucun":
            vat_label_6 = sel_tva_6
            code_tva_6 = sel_tva_6.split(" - ")[0]
            ev_tva_6 = st.session_state.ev_acc_105.get(norm_piv(code_tva_6))
            vat_api_id_6 = ev_tva_6['id'] if ev_tva_6 else None
        if sel_tva_2 != "— Aucun":
            vat_label_2 = sel_tva_2
            code_tva_2 = sel_tva_2.split(" - ")[0]
            ev_tva_2 = st.session_state.ev_acc_105.get(norm_piv(code_tva_2))
            vat_api_id_2 = ev_tva_2['id'] if ev_tva_2 else None

        st.divider()
        if st.button("🔍 ANALYSER", use_container_width=True, key="btn_analyse_105"):
            st.session_state.vat_label_6 = vat_label_6
            st.session_state.vat_label_2 = vat_label_2
            st.session_state.vat_api_id_6 = vat_api_id_6
            st.session_state.vat_api_id_2 = vat_api_id_2
            df_bal = df_bal_preview
            cols = cols_preview
            flux_cols = [c for c in cols if any(x in str(c).upper() for x in ["DEBIT", "CREDIT", "SOLDE"])]

            rules_df = st.session_state.nr_v62
            sorted_roots = sorted(
                [str(r) for r in rules_df['Racine'].dropna() if str(r).strip()],
                key=len, reverse=True
            )

            seen_pivots, results, rejets = set(), [], []
            seen_codes = set()     # déduplication comptes par code
            prot_comptes = set()   # protections comptes : par numéro de compte
            prot_flux = set()      # protections flux : par label normalisé
            prot_flux_ids = set()  # id des flux reconnus par la matrice

            for _, row in df_bal.iterrows():
                code = to_clean_str(row[cols[0]])
                label = str(row[cols[1]]).strip().upper()
                label_flux = clean_label_tva(label, code)
                pivot_flux = norm_piv(label_flux)
                mvt = has_movement(row, flux_cols) if flux_cols else True

                match_root = next((r for r in sorted_roots if code.startswith(r)), None)

                if match_root is None or code.startswith(('401', '411')):
                    raison = "Tiers 401/411" if match_root else "Pas de racine"
                    rejets.append({"N°": code, "Libellé": label, "Raison": raison})
                    continue

                # Déduplication : si ce code a déjà été traité, ignorer (doublon balance)
                code_norm = norm_piv(code)
                if code_norm in seen_codes:
                    rejets.append({"N°": code, "Libellé": label, "Raison": "Doublon (code déjà traité)"})
                    continue
                seen_codes.add(code_norm)

                match_row = rules_df[rules_df['Racine'].apply(to_clean_str) == match_root].iloc[0]

                ev_account = st.session_state.ev_acc_105.get(code_norm)
                vat_id = ev_account['vat_id'] if ev_account else None
                account_exists = ev_account is not None

                # TVA : libellé pour affichage, id API pour injection
                tva_display = "—"
                tva_api_id = vat_id
                if match_row.get("ACHAT", False):
                    if code.startswith('6') and st.session_state.get('vat_label_6'):
                        tva_display = st.session_state.vat_label_6
                        if not tva_api_id:
                            tva_api_id = st.session_state.get('vat_api_id_6')
                    elif code.startswith('2') and st.session_state.get('vat_label_2'):
                        tva_display = st.session_state.vat_label_2
                        if not tva_api_id:
                            tva_api_id = st.session_state.get('vat_api_id_2')

                # Détection des écarts sur COMPTE existant
                compte_status = "➕"
                compte_patch = None
                if account_exists:
                    compte_status = "✅"
                    api_label_norm = norm_piv(ev_account['label'])
                    bal_label_norm = norm_piv(label)
                    if api_label_norm != bal_label_norm:
                        compte_status = "🔄"
                        compte_patch = {"_patch_cat": "COMPTE", "_patch_id": ev_account['id'],
                                        "_patch_payload": {"label": label},
                                        "_patch_detail": f"Libellé: {ev_account['label']} → {label}"}

                item = {
                    "Sync": compte_status in ("➕", "🔄"),
                    "N°": code, "Libellé": label, "LibFlux": label_flux,
                    "TVA": tva_display,
                    "_vat_id": tva_api_id,
                    "COMPTE": compte_status,
                }
                if compte_patch:
                    item.update(compte_patch)

                prot_comptes.add(norm_piv(code))

                # Résolution de l'accountid attendu pour les flux
                expected_acc_id = ev_account['id'] if ev_account else None

                is_first_pivot = pivot_flux not in seen_pivots
                for flux in ["ACHAT", "VENTE", "ENTRÉE BQ", "SORTIE BQ"]:
                    if match_row.get(flux, False) and mvt and is_first_pivot:
                        # --- RECHERCHE DU FLUX PAR SON CHAMP "code" UNIQUEMENT ---
                        # Le code d'un flux = son libellé. On cherche un flux dont
                        # norm_piv(code) == norm_piv(label_flux attendu par la matrice)
                        flux_store = st.session_state.ev_data_105[flux]
                        ev_flux = None
                        seen_flux_ids = set()
                        for d in flux_store.values():
                            if d['id'] in seen_flux_ids:
                                continue
                            seen_flux_ids.add(d['id'])
                            if norm_piv(d.get('code', '')) == pivot_flux:
                                ev_flux = d
                                break

                        if ev_flux:
                            prot_flux_ids.add(ev_flux['id'])
                            # --- COMPARAISON CHAMP PAR CHAMP ---
                            patch_payload = {}
                            patch_details = []
                            # 1. Code (doit être = label_flux)
                            api_code = ev_flux.get('code', '')
                            if norm_piv(api_code) != norm_piv(label_flux):
                                patch_payload["code"] = label_flux[:50]
                                patch_details.append(f"code: {api_code} → {label_flux}")
                            # 2. Label (doit être = label_flux)
                            api_label = ev_flux.get('label', '')
                            if norm_piv(api_label) != norm_piv(label_flux):
                                patch_payload["label"] = label_flux
                                patch_details.append(f"label: {api_label} → {label_flux}")
                            # 3. Accountid (doit pointer vers le bon compte)
                            current_acc = ev_flux.get('acc_id')
                            if expected_acc_id and current_acc != expected_acc_id:
                                patch_payload["accountid"] = expected_acc_id
                                patch_details.append(f"accountid: {current_acc} → {expected_acc_id}")
                            # 4. Vataccountid (ACHAT uniquement)
                            if flux == "ACHAT" and tva_api_id:
                                current_vat = ev_flux.get('vat_id')
                                if current_vat != tva_api_id:
                                    patch_payload["vataccountid"] = tva_api_id
                                    patch_details.append(f"vataccountid: {current_vat} → {tva_api_id}")

                            if patch_payload:
                                item[flux] = "🔄"
                                item[f"_patch_{flux}"] = {"cat": flux, "id": ev_flux['id'],
                                                          "payload": patch_payload, "detail": " | ".join(patch_details)}
                                item["Sync"] = True
                            else:
                                item[flux] = "✅"
                        else:
                            item[flux] = "➕"
                            item["Sync"] = True
                    else:
                        item[flux] = "—"

                if is_first_pivot and pivot_flux:
                    seen_pivots.add(pivot_flux)
                results.append(item)

            st.session_state.audit_matrix_105 = pd.DataFrame(results)
            st.session_state.rejets_105 = pd.DataFrame(rejets)
            st.session_state.prot_105 = {"comptes": prot_comptes, "flux": prot_flux, "flux_ids": prot_flux_ids}
            st.success(f"Analyse terminée : {len(results)} lignes traitées, {len(rejets)} rejetées → voir onglets Matrice / Rejetées")

with m4:
    sub_matrice, sub_audit, sub_rejets, sub_eraz = st.tabs([
        "📋 Matrice", "🔎 Audit MAJ", "🚫 Rejetées", "🧹 ERAZ"
    ])

    with sub_matrice:
        if not st.session_state.audit_matrix_105.empty:
            st.session_state.audit_matrix_105 = st.data_editor(
                st.session_state.audit_matrix_105,
                use_container_width=True, hide_index=True,
                disabled=["Sync", "N°", "Libellé", "TVA", "COMPTE", "ACHAT", "VENTE", "ENTRÉE BQ", "SORTIE BQ",
                           "_vat_id", "_patch_cat", "_patch_id", "_patch_payload", "_patch_detail",
                           "_patch_ACHAT", "_patch_VENTE", "_patch_ENTRÉE BQ", "_patch_SORTIE BQ"],
                column_config={
                    "Libellé": st.column_config.TextColumn("Libellé Compte", help="Libellé du compte (balance) — non modifiable"),
                    "LibFlux": st.column_config.TextColumn("Libellé Flux", help="Libellé catégorie/affectation — modifiable"),
                    "_vat_id": None, "_patch_cat": None, "_patch_id": None,
                    "_patch_payload": None, "_patch_detail": None,
                    "_patch_ACHAT": None, "_patch_VENTE": None,
                    "_patch_ENTRÉE BQ": None, "_patch_SORTIE BQ": None,
                },
            )

    with sub_audit:
        if not st.session_state.audit_matrix_105.empty:
            df_audit = st.session_state.audit_matrix_105
            api_acc = st.session_state.ev_acc_105
            api_data = st.session_state.ev_data_105
            audit_rows = []

            for idx, row in df_audit.iterrows():
                code = row['N°']

                if row.get('COMPTE') == '🔄':
                    ev_account = api_acc.get(norm_piv(code))
                    target_label = str(row['Libellé']).strip().upper()
                    api_label = ev_account['label'] if ev_account else "—"
                    audit_rows.append({
                        "Catégorie": "COMPTE", "N°": code,
                        "Champ": "label",
                        "Valeur cible": target_label,
                        "Valeur API": api_label,
                    })

                for flux in ["ACHAT", "VENTE", "ENTRÉE BQ", "SORTIE BQ"]:
                    if row.get(flux) == '🔄':
                        patch_info = row.get(f'_patch_{flux}')
                        if isinstance(patch_info, str):
                            try: patch_info = ast.literal_eval(patch_info)
                            except: patch_info = None

                        if isinstance(patch_info, dict):
                            flux_data = api_data.get(flux, {})
                            ev_flux = None
                            fid = patch_info.get('id')
                            if fid:
                                for d in flux_data.values():
                                    if d['id'] == fid:
                                        ev_flux = d
                                        break

                            payload = patch_info.get('payload', {})
                            if isinstance(payload, str):
                                try: payload = ast.literal_eval(payload)
                                except: payload = {}

                            for field, target_val in payload.items():
                                if ev_flux:
                                    if field == "accountid":
                                        api_val = ev_flux.get('acc_id')
                                    elif field == "vataccountid":
                                        api_val = ev_flux.get('vat_id')
                                    elif field == "code":
                                        api_val = ev_flux.get('code')
                                    elif field == "label":
                                        api_val = ev_flux.get('label')
                                    else:
                                        api_val = ev_flux.get(field)
                                else:
                                    api_val = "—"

                                audit_rows.append({
                                    "Catégorie": flux, "N°": code,
                                    "Champ": field,
                                    "Valeur cible": str(target_val),
                                    "Valeur API": str(api_val) if api_val is not None else "—",
                                })

            if audit_rows:
                df_audit_display = pd.DataFrame(audit_rows)
                st.metric("Champs à mettre à jour", len(df_audit_display))
                st.dataframe(df_audit_display, use_container_width=True, hide_index=True)
            else:
                st.info("Aucune mise à jour (🔄) détectée dans la matrice")
        else:
            st.info("Lancez l'analyse d'abord (onglet Balance)")

    with sub_rejets:
        if isinstance(st.session_state.rejets_105, pd.DataFrame) and not st.session_state.rejets_105.empty:
            st.dataframe(st.session_state.rejets_105, use_container_width=True)
        else:
            st.info("Aucun rejet")

    with sub_eraz:
        has_headers = bool(st.session_state.token_headers_105)
        prot = st.session_state.prot_105

        if not has_headers:
            st.warning("Connectez-vous à l'API Evoliz d'abord")
        elif not prot:
            st.info("Lancez l'analyse d'abord (onglet Balance)")
        else:
            prot_comptes = prot.get("comptes", set()) if isinstance(prot, dict) else prot
            prot_flux_ids = prot.get("flux_ids", set()) if isinstance(prot, dict) else set()
            orphans_by_cat = {}

            orphans_by_cat["COMPTE"] = [
                {"pivot": p, "id": d['id'], "Code": d['code'], "Libellé": d['label']}
                for p, d in st.session_state.ev_acc_105.items() if p not in prot_comptes
            ]
            for flux in ["ACHAT", "VENTE", "ENTRÉE BQ", "SORTIE BQ"]:
                seen_ids = set()
                orphans = []
                for p, d in st.session_state.ev_data_105.get(flux, {}).items():
                    if d['id'] not in prot_flux_ids and d['id'] not in seen_ids:
                        orphans.append({"pivot": p, "id": d['id'], "Code": d['code'], "Libellé": d['label']})
                        seen_ids.add(d['id'])
                orphans_by_cat[flux] = orphans

            st.session_state.eraz_items = orphans_by_cat
            total_orphans = sum(len(v) for v in orphans_by_cat.values())
            st.subheader(f"🧹 {total_orphans} éléments API à supprimer")

            for cat, items in orphans_by_cat.items():
                if items:
                    with st.expander(f"{cat} — {len(items)} orphelins"):
                        st.dataframe(pd.DataFrame(items)[['Code', 'Libellé']], use_container_width=True)

            if not total_orphans:
                st.success("Aucun orphelin détecté")

# --- m6 : Synthèse (vérification avant synchro) ---
with m6:
    if not st.session_state.audit_matrix_105.empty:
        df_m = st.session_state.audit_matrix_105
        eraz = st.session_state.eraz_counts
        eraz_items = st.session_state.eraz_items

        stats = []
        for cat in ["COMPTE", "ACHAT", "VENTE", "ENTRÉE BQ", "SORTIE BQ"]:
            lus = len(st.session_state.ev_acc_105) if cat == "COMPTE" else count_unique(st.session_state.ev_data_105.get(cat, {}))
            crees = len(df_m[df_m[cat] == '➕'])
            maj = len(df_m[df_m[cat] == '🔄'])
            a_supprimer = len(eraz_items.get(cat, []))
            supprimes = eraz.get(cat, 0)
            en_matrice = len(df_m[df_m[cat].isin(['✅', '➕', '🔄'])])
            attendu = lus + crees - a_supprimer
            coherent = attendu == en_matrice

            stats.append({
                "Catégorie": cat,
                "📖 Lus API": lus,
                "➕ À créer": crees,
                "🔄 À maj": maj,
                "🗑️ À supprimer": a_supprimer,
                "✅ Supprimés": supprimes,
                "= Attendu": attendu,
                "📊 Matrice": en_matrice,
                "✔️ Cohérent": "✅" if coherent else "❌",
            })
        st.table(pd.DataFrame(stats))
    else:
        st.info("Lancez l'analyse d'abord (onglet Balance)")

# --- m7 : Synchro (DELETE + PATCH + CREATE, dernier onglet) ---
with m7:
    df_sync = st.session_state.audit_matrix_105
    has_headers = bool(st.session_state.token_headers_105)

    if not df_sync.empty and has_headers:
        # Option de filtrage
        sync_scope = st.radio("Périmètre de synchronisation",
                              ["🌐 Tout", "🏷️ Ventes uniquement (comptes 7xx)"],
                              horizontal=True, key="sync_scope")
        ventes_only = sync_scope.startswith("🏷️")

        to_sync = df_sync[df_sync['Sync'] == True]
        if ventes_only:
            to_sync = to_sync[to_sync['N°'].astype(str).str.startswith('7')]

        new_accounts = to_sync[to_sync['COMPTE'] == '➕']
        patch_accounts = to_sync[to_sync['COMPTE'] == '🔄']
        if ventes_only:
            # Flux : ne garder que VENTE
            new_flux = {f: (to_sync[to_sync[f] == '➕'] if f == "VENTE" else pd.DataFrame()) for f in FLUX_ENDPOINTS}
            patch_flux = {f: (to_sync[to_sync[f] == '🔄'] if f == "VENTE" else pd.DataFrame()) for f in FLUX_ENDPOINTS}
        else:
            new_flux = {f: to_sync[to_sync[f] == '➕'] for f in FLUX_ENDPOINTS}
            patch_flux = {f: to_sync[to_sync[f] == '🔄'] for f in FLUX_ENDPOINTS}
        total_create = len(new_accounts) + sum(len(v) for v in new_flux.values())
        total_patch = len(patch_accounts) + sum(len(v) for v in patch_flux.values())

        eraz_items = st.session_state.eraz_items
        if ventes_only:
            # ERAZ : ne supprimer que les comptes 7xx et les flux VENTE
            eraz_items = {
                "COMPTE": [i for i in eraz_items.get("COMPTE", []) if str(i.get('Code', '')).startswith('7')],
                "VENTE": eraz_items.get("VENTE", []),
                "ACHAT": [], "ENTRÉE BQ": [], "SORTIE BQ": [],
            }
        total_delete = sum(len(v) for v in eraz_items.values()) if eraz_items else 0

        st.subheader("📋 Résumé des opérations")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🗑️ Suppressions", total_delete)
        c2.metric("🔄 Mises à jour", total_patch)
        c3.metric("➕ Créations", total_create)
        c4.metric("Total", total_delete + total_patch + total_create)

        total_all = total_delete + total_patch + total_create
        if total_all > 0 and st.button("⚡ LANCER LA SYNCHRONISATION", type="primary", use_container_width=True):
            headers = st.session_state.token_headers_105
            cid = st.session_state.company_id_105
            log = []
            progress = st.progress(0)
            done = [0]  # list pour mutation dans les threads
            eraz_counts = {"COMPTE": 0, "ACHAT": 0, "VENTE": 0, "ENTRÉE BQ": 0, "SORTIE BQ": 0}
            MAX_WORKERS = 5  # 5 workers pour rester sous 100 req/min avec marge

            # Rate limiter : max 90 requêtes par fenêtre de 60s (marge de sécurité)
            _rate_lock = threading.Lock()
            _rate_timestamps = []
            RATE_LIMIT = 90
            RATE_WINDOW = 60

            def rate_wait():
                """Attend si nécessaire pour ne pas dépasser le rate limit API."""
                with _rate_lock:
                    now = time.time()
                    # Purger les timestamps hors fenêtre
                    while _rate_timestamps and _rate_timestamps[0] < now - RATE_WINDOW:
                        _rate_timestamps.pop(0)
                    if len(_rate_timestamps) >= RATE_LIMIT:
                        wait = _rate_timestamps[0] + RATE_WINDOW - now + 0.1
                        if wait > 0:
                            time.sleep(wait)
                        # Re-purger après l'attente
                        now = time.time()
                        while _rate_timestamps and _rate_timestamps[0] < now - RATE_WINDOW:
                            _rate_timestamps.pop(0)
                    _rate_timestamps.append(time.time())

            def safe_dict(val):
                if isinstance(val, dict): return val
                if isinstance(val, str):
                    try: return ast.literal_eval(val)
                    except: return None
                return None

            def update_progress():
                done[0] += 1
                progress.progress(min(done[0] / total_all, 1.0))

            # Wrappers avec rate limiting
            def _del_rate(cat, item_id, hdrs):
                rate_wait()
                return delete_evoliz_item(cat, item_id, hdrs, company_id=cid)

            def _patch_rate(cat, item_id, payload, hdrs, company_id=None):
                rate_wait()
                return patch_evoliz_item(cat, item_id, payload, hdrs, company_id=company_id)

            def _add_account_rate(code, label, hdrs):
                rate_wait()
                return inject_account(code, label, hdrs)

            def _add_flux_rate(flux_type, code, label, hdrs, vat_id=None, acc_id=None):
                rate_wait()
                return inject_flux(flux_type, code, label, hdrs, vat_id=vat_id, acc_id=acc_id)

            # --- 1. DELETE orphelins (parallèle par paquets) ---
            if eraz_items:
                del_tasks = []
                for cat, items in eraz_items.items():
                    for item in items:
                        del_tasks.append((cat, item))
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = {pool.submit(_del_rate, t[0], t[1]['id'], headers): t for t in del_tasks}
                    for f in as_completed(futures):
                        cat, item = futures[f]
                        ok, msg = f.result()
                        log.append({"Action": "🗑️ DEL", "Type": cat, "Code": item['Code'],
                                    "Libellé": item['Libellé'], "Résultat": "✅" if ok else "❌", "Détail": msg})
                        if ok:
                            eraz_counts[cat] += 1
                        update_progress()

            st.session_state.eraz_counts = eraz_counts

            # --- 2. UPDATE comptes (DELETE + POST, séquentiel) ---
            patch_accounts = to_sync[to_sync['COMPTE'] == '🔄'] if 'COMPTE' in to_sync.columns else pd.DataFrame()
            for _, row in patch_accounts.iterrows():
                p_id = row.get('_patch_id')
                p_detail = row.get('_patch_detail', '')
                if p_id:
                    rate_wait()
                    ok_del, _ = delete_evoliz_item("COMPTE", p_id, headers)
                    if ok_del:
                        rate_wait()
                        ok_add, resp = inject_account(row['N°'], row['Libellé'], headers)
                        log.append({"Action": "🔄 UPD", "Type": "COMPTE", "Code": row['N°'],
                                    "Libellé": p_detail, "Résultat": "✅" if ok_add else "❌",
                                    "Détail": f"DEL+POST: {str(resp)[:100]}"})
                    else:
                        log.append({"Action": "🔄 UPD", "Type": "COMPTE", "Code": row['N°'],
                                    "Libellé": p_detail, "Résultat": "❌", "Détail": "Échec DELETE"})
                update_progress()

            # Rafraîchir les comptes après DEL+POST pour avoir les nouveaux id
            if not patch_accounts.empty:
                time.sleep(1)
                st.session_state.ev_acc_105 = fetch_evoliz_data("accounts", headers)

            # --- 3. PATCH flux (parallèle) ---
            patch_tasks = []
            acc_lookup = st.session_state.ev_acc_105
            for flux in FLUX_ENDPOINTS:
                for _, row in patch_flux[flux].iterrows():
                    patch_info = safe_dict(row.get(f'_patch_{flux}'))
                    if patch_info:
                        payload = safe_dict(patch_info.get('payload')) or patch_info.get('payload')
                        # Résoudre l'accountid à jour (le compte a pu être DEL+POST)
                        if isinstance(payload, dict) and 'accountid' in payload:
                            ev_a = acc_lookup.get(norm_piv(row['N°']))
                            if ev_a:
                                payload['accountid'] = ev_a['id']
                        patch_tasks.append((flux, row['N°'], patch_info, payload))
            if patch_tasks:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = {pool.submit(_patch_rate, t[0], t[2]['id'], t[3], headers, company_id=cid): t for t in patch_tasks}
                    for f in as_completed(futures):
                        flux, code, pinfo, _ = futures[f]
                        ok, resp = f.result()
                        log.append({"Action": "🔄 UPD", "Type": flux, "Code": code,
                                    "Libellé": pinfo.get('detail', ''), "Résultat": "✅" if ok else "❌", "Détail": str(resp)[:120]})
                        update_progress()

            # --- 4. CREATE comptes (parallèle) ---
            if not new_accounts.empty:
                acc_tasks = [(row['N°'], row['Libellé']) for _, row in new_accounts.iterrows()]
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = {pool.submit(_add_account_rate, t[0], t[1], headers): t for t in acc_tasks}
                    for f in as_completed(futures):
                        code, label = futures[f]
                        ok, resp = f.result()
                        log.append({"Action": "➕ ADD", "Type": "COMPTE", "Code": code,
                                    "Libellé": label, "Résultat": "✅" if ok else "❌", "Détail": str(resp)[:120]})
                        update_progress()

            # Rafraîchir les comptes pour résoudre les accountid des flux à créer
            time.sleep(2)  # Pause pour laisser l'API digérer les créations
            with st.spinner("Rafraîchissement des comptes..."):
                st.session_state.ev_acc_105 = fetch_evoliz_data("accounts", headers)

            # --- 5. CREATE flux (parallèle) ---
            flux_tasks = []
            acc_lookup = st.session_state.ev_acc_105
            for flux, df_f in new_flux.items():
                for _, row in df_f.iterrows():
                    label_for_flux = row.get('LibFlux', row['Libellé'])
                    row_vat = row.get('_vat_id') if flux == "ACHAT" else None
                    # Résoudre accountid depuis les comptes rafraîchis
                    row_acc_id = None
                    ev_a = acc_lookup.get(norm_piv(row['N°']))
                    if ev_a:
                        row_acc_id = ev_a['id']
                    else:
                        # Fallback : chercher par code brut dans toutes les valeurs
                        code_str = str(row['N°']).strip()
                        for a in acc_lookup.values():
                            if a.get('code', '').strip() == code_str:
                                row_acc_id = a['id']
                                break
                    flux_tasks.append((flux, row['N°'], label_for_flux, row_vat, row_acc_id))
            if flux_tasks:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = {pool.submit(_add_flux_rate, t[0], t[1], t[2], headers, vat_id=t[3], acc_id=t[4]): t for t in flux_tasks}
                    for f in as_completed(futures):
                        flux, code, lbl, _, _ = futures[f]
                        ok, resp = f.result()
                        log.append({"Action": "➕ ADD", "Type": flux, "Code": code,
                                    "Libellé": lbl, "Résultat": "✅" if ok else "❌", "Détail": str(resp)[:120]})
                        update_progress()

            st.session_state.sync_log = log
            ok_count = sum(1 for l in log if l['Résultat'] == '✅')
            st.success(f"Terminé : {ok_count}/{len(log)} opérations réussies")

            with st.spinner("Rafraîchissement des données API..."):
                st.session_state.ev_acc_105 = fetch_evoliz_data("accounts", headers)
                st.session_state.ev_data_105 = {
                    "ACHAT": fetch_evoliz_data("purchase-classifications", headers),
                    "VENTE": fetch_evoliz_data("sale-classifications", headers),
                    "ENTRÉE BQ": fetch_evoliz_data("sale-affectations", headers),
                    "SORTIE BQ": fetch_evoliz_data("purchase-affectations", headers),
                }

        if st.session_state.sync_log:
            df_log = pd.DataFrame(st.session_state.sync_log)

            # --- CR synthétique ---
            st.subheader("📊 Compte-rendu d'exécution")
            ok_total = len(df_log[df_log['Résultat'] == '✅'])
            ko_total = len(df_log[df_log['Résultat'] == '❌'])
            c1, c2, c3 = st.columns(3)
            c1.metric("Total opérations", len(df_log))
            c2.metric("✅ Réussies", ok_total)
            c3.metric("❌ Échouées", ko_total)

            # Tableau croisé Action x Type
            cr_data = []
            for action in ['🗑️ DEL', '🔄 UPD', '➕ ADD']:
                row_cr = {"Action": action}
                for cat in ["COMPTE", "ACHAT", "VENTE", "ENTRÉE BQ", "SORTIE BQ"]:
                    mask = (df_log['Action'] == action) & (df_log['Type'] == cat)
                    ok = len(df_log[mask & (df_log['Résultat'] == '✅')])
                    ko = len(df_log[mask & (df_log['Résultat'] == '❌')])
                    if ok + ko > 0:
                        row_cr[cat] = f"{ok}✅ {ko}❌" if ko else f"{ok}✅"
                    else:
                        row_cr[cat] = "—"
                cr_data.append(row_cr)
            st.table(pd.DataFrame(cr_data))

            # --- Vérification post-exécution ---
            st.subheader("🔎 Vérification post-exécution (GET vs Matrice)")
            df_m = st.session_state.audit_matrix_105
            api_acc = st.session_state.ev_acc_105
            api_data = st.session_state.ev_data_105

            verif = []
            for cat in ["COMPTE", "ACHAT", "VENTE", "ENTRÉE BQ", "SORTIE BQ"]:
                api_src = api_acc if cat == "COMPTE" else api_data.get(cat, {})
                api_count = len(api_src) if cat == "COMPTE" else count_unique(api_src)
                matrice_attendus = len(df_m[df_m[cat].isin(['✅', '➕', '🔄'])])

                if cat == "COMPTE":
                    # Comptes : comparaison par pivot code
                    api_by_id = {d['id']: norm_piv(d['code']) for d in api_src.values()}
                    api_pivots_unique = set(api_by_id.values())
                    matrice_pivots = set(norm_piv(c) for c in df_m[df_m[cat].isin(['✅', '➕', '🔄'])]['N°'])
                else:
                    # Flux : dédupliquer par id, comparer par pivot label (comme la matrice)
                    seen_ids = set()
                    api_pivots_unique = set()
                    for p, d in api_src.items():
                        if d['id'] not in seen_ids:
                            seen_ids.add(d['id'])
                            # Utiliser le pivot label pour comparer (même logique que la matrice)
                            pivot_label = norm_piv(d['label'])
                            api_pivots_unique.add(pivot_label)
                    matrice_pivots = set(
                        norm_piv(clean_label_tva(r['Libellé'], r['N°']))
                        for _, r in df_m[df_m[cat].isin(['✅', '➕', '🔄'])].iterrows()
                    )

                presents = matrice_pivots & api_pivots_unique
                manquants = matrice_pivots - api_pivots_unique
                excedents = api_pivots_unique - matrice_pivots

                status = "✅" if not manquants and not excedents else "❌"
                verif.append({
                    "Catégorie": cat,
                    "API (GET)": api_count,
                    "Matrice": matrice_attendus,
                    "Présents": len(presents),
                    "Manquants": len(manquants),
                    "Excédents": len(excedents),
                    "Statut": status,
                })
            st.table(pd.DataFrame(verif))

            # Détail des écarts si présents
            all_manquants = []
            all_excedents = []
            for cat in ["COMPTE", "ACHAT", "VENTE", "ENTRÉE BQ", "SORTIE BQ"]:
                api_src = api_acc if cat == "COMPTE" else api_data.get(cat, {})

                if cat == "COMPTE":
                    api_labels_by_pivot = {norm_piv(d['code']): d for d in api_src.values()}
                    matrice_pivots = {norm_piv(c): c for c in df_m[df_m[cat].isin(['✅', '➕', '🔄'])]['N°']}
                else:
                    # Dédupliquer par id, indexer par pivot label
                    seen_ids = set()
                    api_labels_by_pivot = {}
                    for d in api_src.values():
                        if d['id'] not in seen_ids:
                            seen_ids.add(d['id'])
                            api_labels_by_pivot[norm_piv(d['label'])] = d
                    matrice_pivots = {
                        norm_piv(clean_label_tva(r['Libellé'], r['N°'])): r.get('LibFlux', r['Libellé'])
                        for _, r in df_m[df_m[cat].isin(['✅', '➕', '🔄'])].iterrows()
                    }

                for p, ref in matrice_pivots.items():
                    if p and p not in api_labels_by_pivot:
                        all_manquants.append({"Catégorie": cat, "Pivot": p, "Référence": ref})
                for p, d in api_labels_by_pivot.items():
                    if p not in matrice_pivots:
                        all_excedents.append({"Catégorie": cat, "Code": d.get('code', ''), "Libellé": d.get('label', '')})

            if all_manquants:
                with st.expander(f"⚠️ {len(all_manquants)} éléments manquants dans l'API"):
                    st.dataframe(pd.DataFrame(all_manquants), use_container_width=True)
            if all_excedents:
                with st.expander(f"⚠️ {len(all_excedents)} éléments excédentaires dans l'API"):
                    st.dataframe(pd.DataFrame(all_excedents), use_container_width=True)
            if not all_manquants and not all_excedents:
                st.success("Parfaite concordance entre l'API et la matrice")

            # --- Logs détaillés ---
            st.subheader("📋 Logs détaillés")
            filtre = st.radio("Filtrer", ["Tout", "✅ Réussies", "❌ Échouées"], horizontal=True, key="log_filter")
            if filtre == "✅ Réussies":
                df_show = df_log[df_log['Résultat'] == '✅']
            elif filtre == "❌ Échouées":
                df_show = df_log[df_log['Résultat'] == '❌']
            else:
                df_show = df_log
            st.dataframe(df_show, use_container_width=True)

            # Export CSV
            csv = df_log.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Exporter les logs (CSV)", csv, "sync_log.csv", "text/csv")
    elif not has_headers:
        st.warning("Connectez-vous à l'API Evoliz d'abord (onglet Connexion API)")
    else:
        st.info("Lancez l'analyse d'abord (onglet Balance)")