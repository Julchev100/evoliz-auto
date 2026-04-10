import streamlit as st
import pandas as pd
import requests
import re
import unicodedata
import os
import ast
import io
import json
import zipfile
import time
import threading
from datetime import datetime as dt_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from openpyxl import Workbook

st.set_page_config(page_title="Banana Import Club", layout="wide", page_icon="🍌")

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

def clean_label_tva(label, code, do_fusion=True):
    if do_fusion and str(code).startswith(('2', '6', '7')):
        s = str(label)
        # 1. Supprimer les taux de TVA (20%, 5,5 %, 10.0%, etc.) avec les signes autour
        s = re.sub(r'[\s\-/\(]*\d+[.,\s]?\d*\s?%[\s\-/\)]*', ' ', s, flags=re.IGNORECASE)
        # 2. Supprimer les mentions TVA et toutes leurs variantes, y compris
        #    tronquées (EXO, INTR, AUTOLIQ...) grâce à des préfixes minimaux.
        #    Le \w* après le préfixe attrape les suffixes quelconques.
        s = re.sub(
            r'[\s\-/\(]*\b('
            r'T[\.\s]?V[\.\s]?A\.?'                   # TVA, T.V.A., T V A
            r'|EXON\w*'                              # EXONERE, EXONEREE, EXONERATION...
            r'|EXO\w*'                               # EXO, EXOTVA, EXO TVA...
            r'|INTRA\w*'                             # INTRA, INTRACOM, INTRACOMMUNAUTAIRE...
            r'|I[\.\s]?C\.?\w*'                        # IC, I.C., I C, I.C
            r'|AUTO[\-\s.]?LIQ\w*'                   # AUTOLIQ, AUTOLIQUIDATION, AUTO-LIQ...
            r'|REVERSE[\s\-]?CHARGE\w*'              # REVERSE CHARGE
            r'|NON[\s\-]?SOUMIS\w*'                  # NON SOUMIS, NON-SOUMISE
            r'|HORS[\s\-]?TAXE\w*'                   # HORS TAXE, HORS-TAXE
            r'|IMPORT\w*'                             # IMPORT, IMPORTATION
            r'|EXPORT\w*'                             # EXPORT, EXPORTATION
            r'|FRANCE'                                # FRANCE
            r'|FR'                                    # FR
            r'|UE'                                    # UE
            r'|CEE'                                   # CEE
            r'|HT'                                    # HT
            r'|TTC'                                   # TTC
            r')\b[\s\-/\)]*', ' ', s, flags=re.IGNORECASE)
        # 3. Normaliser la ponctuation résiduelle (. , ; :) en espaces
        s = re.sub(r'[.,;:]+', ' ', s)
        # 4. Supprimer les mots résiduels type EX... en fin de chaîne
        s = re.sub(r'[\s\-/]*\bEX\w*\s*$', '', s, flags=re.IGNORECASE)
        # 5. Nettoyer les signes orphelins en début/fin et les espaces multiples
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

def fetch_evoliz_data(endpoint, headers, company_id=None):
    results = {}
    if company_id:
        url = f"https://www.evoliz.io/api/v1/companies/{company_id}/{endpoint}"
    else:
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

def has_movement_debit_credit(row, flux_cols):
    """Vérifie le mouvement uniquement sur les colonnes Débit/Crédit (ignore Solde)."""
    for c in flux_cols:
        if "SOLDE" in str(c).upper():
            continue
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

def inject_flux(flux_type, code, label, headers, vat_id=None, acc_id=None, vat_rate=None):
    endpoint = FLUX_ENDPOINTS[flux_type]
    # Le code d'une catégorie/affectation est son libellé (tronqué à 50 car pour l'API)
    payload = {"code": label[:50], "label": label}
    if acc_id and not (isinstance(acc_id, float) and pd.isna(acc_id)):
        payload["accountid"] = int(acc_id)
    if vat_id and not (isinstance(vat_id, float) and pd.isna(vat_id)):
        # L'API Evoliz attend le champ "vataccountid" pour lier un compte TVA
        payload["vataccountid"] = int(vat_id)
    if vat_rate is not None and flux_type == "ACHAT":
        payload["vat_rate"] = float(vat_rate)
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

st.title("🍌 Banana Import Club")

for key, default in [('nr_v62', pd.DataFrame()), ('audit_matrix_105', pd.DataFrame()),
                         ('rejets_105', pd.DataFrame()), ('prot_105', set()), ('sync_log', []),
                         ('ev_acc_105', {}), ('ev_data_105', {"ACHAT": {}, "VENTE": {}, "ENTRÉE BQ": {}, "SORTIE BQ": {}}),
                         ('token_headers_105', {}), ('company_id_105', None), ('companies_list', []), ('eraz_log', []),
                         ('eraz_counts', {"COMPTE": 0, "ACHAT": 0, "VENTE": 0, "ENTRÉE BQ": 0, "SORTIE BQ": 0}),
                         ('eraz_items', {})]:
    if key not in st.session_state:
        st.session_state[key] = default

# --- Configuration du perimetre ---
with st.sidebar:
    st.header("⚙️ Perimetre d'integration")
    scope = st.radio("Type de parametrage", ["Parametrage complet", "Ventes seules"], key="scope_mode")
    st.divider()
    st.subheader("Modules a activer")
    if scope == "Parametrage complet":
        mod_compta = st.checkbox("📂 Comptabilite (Balance, Param, Synchro)", value=True, key="mod_compta")
    else:
        mod_compta = False
    mod_clients = st.checkbox("👥 Injection Clients", value=True, key="mod_clients")
    mod_fournisseurs = st.checkbox("🏭 Injection Fournisseurs", value=True, key="mod_fournisseurs")
    mod_articles = st.checkbox("📦 Articles", value=True, key="mod_articles")
    mod_factures = st.checkbox("🧾 Factures & Avoirs", value=scope == "Parametrage complet", key="mod_factures")
    if mod_compta:
        with st.expander("📂 Parametres comptables", expanded=False):
            _f_param_sb = st.file_uploader("Importer un fichier parametres", type=["xlsm", "xlsx", "xls"], key="imp_param_sb", label_visibility="collapsed")
            if _f_param_sb: st.session_state["imp_file_param"] = _f_param_sb
            show_param = st.checkbox("Voir / éditer les paramètres", value=False, key="show_param")
            if show_param and not st.session_state.nr_v62.empty:
                _edited_sb = st.data_editor(
                    st.session_state.nr_v62, num_rows="dynamic", use_container_width=True, key="param_editor_sb"
                )
                if st.button("💾 Sauvegarder", key="btn_save_param_sb"):
                    st.session_state.nr_v62 = _edited_sb
                    save_param_local(_edited_sb)
                    st.success("Sauvegardé")
                st.download_button(
                    "📥 CSV", data=st.session_state.nr_v62.to_csv(index=False),
                    file_name="param_local.csv", mime="text/csv", key="btn_dl_param_sb"
                )
            fusion_tva = st.checkbox("Fusionner classif. par taux TVA", value=True, key="fusion_tva",
                                      help="Si coche, les classifications 6xx/7xx qui ne different que par le taux de TVA sont regroupees en une seule.")
            tva_achat_rate = st.number_input("Taux TVA achats (%)", value=20.0, step=0.5, min_value=0.0, max_value=100.0, key="tva_achat_rate",
                                              help="Applique aux classifications d'achat (comptes 2xx/6xx).")
    else:
        show_param = False
        fusion_tva = True
        tva_achat_rate = 20.0

# Élargir la sidebar quand on édite les paramètres comptables
if show_param:
    st.markdown("""<style>
        [data-testid="stSidebar"] { min-width: 600px; max-width: 800px; }
    </style>""", unsafe_allow_html=True)

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

# Construction dynamique des onglets
_tab_names = []
_tab_keys = []
# Toujours : Connexion API + Import fichiers
_tab_names.append("🔑 Connexion API"); _tab_keys.append("api")
_tab_names.append("📁 Import fichiers"); _tab_keys.append("import")
if mod_compta:
    _tab_names.append("🔍 Matrice comptable"); _tab_keys.append("matrice")
if mod_clients:
    _tab_names.append("👥 Injection Clients"); _tab_keys.append("clients")
if mod_fournisseurs:
    _tab_names.append("🏭 Injection Fournisseurs"); _tab_keys.append("fournisseurs")
if mod_articles:
    _tab_names.append("📦 Articles"); _tab_keys.append("articles")
if mod_factures:
    _tab_names.append("🧾 Factures & Avoirs"); _tab_keys.append("factures")

_tabs = st.tabs(_tab_names)
_tab_map = {k: t for k, t in zip(_tab_keys, _tabs)}

# Aliases pour compatibilite avec le code existant
m2 = _tab_map.get("api", st.container())
m_import = _tab_map.get("import", st.container())
m1 = st.container()  # Onglet Param supprimé, paramètres dans la sidebar
m4 = _tab_map.get("matrice", st.container())
m6 = _tab_map.get("synthese", st.container())
m7 = _tab_map.get("synchro", st.container())
m_cli = _tab_map.get("clients", st.container())
m_four = _tab_map.get("fournisseurs", st.container())
m_fac = _tab_map.get("factures", st.container())
m_art = _tab_map.get("articles", st.container())
# Flags pour conditionner l'execution du contenu
_has_param_tab = False
_has_matrice_tab = "matrice" in _tab_map
_has_synthese_tab = "synthese" in _tab_map
_has_synchro_tab = "synchro" in _tab_map
_has_clients_tab = "clients" in _tab_map
_has_fournisseurs_tab = "fournisseurs" in _tab_map
_has_factures_tab = "factures" in _tab_map
_has_articles_tab = "articles" in _tab_map

def _get_sheet_names(f):
    """Retourne la liste des onglets d'un fichier Excel, ou None si CSV/mono-feuille HTML."""
    if hasattr(f, 'name') and f.name.lower().endswith('.csv'):
        return None
    fname = getattr(f, 'name', '').lower()
    if fname.endswith('.xls'):
        engines = ["xlrd", None]
    elif fname.endswith(('.xlsx', '.xlsm')):
        engines = ["openpyxl", None]
    else:
        engines = [None, "openpyxl", "xlrd"]
    for engine in engines:
        try:
            f.seek(0)
            xl = pd.ExcelFile(f, engine=engine)
            return xl.sheet_names
        except Exception:
            pass
    # Fallback : certains .xls sont du HTML déguisé
    try:
        f.seek(0)
        raw = f.read() if hasattr(f, 'read') else open(f, 'rb').read()
        f.seek(0)
        dfs = pd.read_html(raw, header=0)
        if dfs and len(dfs) > 1:
            return [f"Feuille {i+1}" for i in range(len(dfs))]
    except Exception:
        pass
    return None

def _sheet_selector(f, label, key):
    """Si le fichier a plusieurs onglets, affiche un selectbox et retourne le nom choisi."""
    sheets = _get_sheet_names(f)
    if sheets and len(sheets) > 1:
        chosen = st.selectbox(f"📑 Onglet à utiliser ({label})", sheets, key=key)
        if chosen.startswith("Feuille "):
            try: return int(chosen.split(" ")[1]) - 1
            except Exception: pass
        return chosen
    return 0

# --- Onglet Import fichiers centralise ---
with m_import:
    st.subheader("📁 Import des fichiers sources")
    st.caption("Centralisez ici tous vos fichiers. Ils seront utilises dans les onglets correspondants.")

    # Layout compact : label | browse | statut sur la même ligne
    # + sélection d'onglet automatique si fichier multi-feuilles
    def _file_row(label, types, session_key, uploader_key):
        _f_uploaded = st.session_state.get(session_key)
        # Détecter les onglets du fichier déjà chargé pour dimensionner la ligne
        _sheets = _get_sheet_names(_f_uploaded) if _f_uploaded else None
        _has_sheets = _sheets and len(_sheets) > 1
        if _has_sheets:
            c1, c2, c3, c4 = st.columns([1.2, 3, 1.8, 1.2])
        else:
            c1, c2, c3 = st.columns([1.5, 4, 1.5])
        c1.markdown(f"**{label}**")
        _f = c2.file_uploader(label, type=types, key=uploader_key, label_visibility="collapsed")
        if _f: st.session_state[session_key] = _f; _f_uploaded = _f
        if _has_sheets:
            _sheet_key = f"_sheet_{session_key}"
            _chosen = c3.selectbox(f"Onglet", _sheets, key=_sheet_key, label_visibility="collapsed")
            if _chosen and str(_chosen).startswith("Feuille "):
                try: _chosen = int(_chosen.split(" ")[1]) - 1
                except Exception: pass
            st.session_state[f"{session_key}_sheet"] = _chosen
            c4.markdown(f"✅ {_f_uploaded.name}" if _f_uploaded else "❌ —")
        else:
            c3.markdown(f"✅ {_f_uploaded.name}" if _f_uploaded else "❌ —")
            if _f_uploaded:
                st.session_state[f"{session_key}_sheet"] = 0

    if mod_compta:
        _file_row("📂 Balance", ["xlsm", "xlsx", "xls"], "imp_file_balance", "imp_balance")
    if mod_clients:
        _file_row("👥 Clients", ["xlsx", "xls", "csv"], "imp_file_clients", "imp_clients")
    if mod_fournisseurs:
        _file_row("🏭 Fournisseurs", ["xlsx", "xls", "csv"], "imp_file_fournisseurs", "imp_fournisseurs")
    if mod_factures:
        _file_row("🧾 Factures", ["xlsx", "xls"], "imp_file_factures", "imp_factures")
    if mod_articles:
        _file_row("📦 Articles", ["xlsx", "xls"], "imp_file_articles", "imp_articles")


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

# Chargement auto des params (necessaire pour la matrice, meme sans onglet Param visible)
if st.session_state.nr_v62.empty and os.path.exists(PARAM_PATH):
    st.session_state.nr_v62 = load_param_local()
_f_param_loaded = st.session_state.get("imp_file_param")
if _f_param_loaded:
    try:
        _xl_p = pd.ExcelFile(_f_param_loaded)
        if "Param" in _xl_p.sheet_names:
            st.session_state.nr_v62 = load_param_from_excel(_f_param_loaded)
            save_param_local(st.session_state.nr_v62)
    except Exception:
        pass

with m2:
    # --- Clés API ---
    st.subheader("🔑 Connexion Evoliz")
    saved_pk, saved_sk = load_creds()
    col_pk, col_sk = st.columns(2)
    pk_105 = col_pk.text_input("Public Key", value=saved_pk, key="pk_105")
    sk_105 = col_sk.text_input("Secret Key", type="password", value=saved_sk, key="sk_105")

    # --- Bloc 1 : Login (récupère le token + liste des dossiers accessibles) ---
    auto_connect = not st.session_state.token_headers_105 and saved_pk and saved_sk
    # Toujours rendre le bouton (même si auto_connect va le déclencher)
    btn_connect = st.button("🔗 CONNECTER", type="primary", use_container_width=True, key="btn_connect_105")
    # Bouton déconnexion si déjà connecté
    if st.session_state.token_headers_105:
        if st.button("🔌 Se déconnecter / changer de clés", key="btn_disconnect"):
            for _k in ('token_headers_105', 'company_id_105', 'companies_list',
                        'ev_acc_105', 'ev_data_105', 'ev_clients_raw', 'ev_articles_raw', 'ev_invoices_raw'):
                if _k in st.session_state:
                    st.session_state[_k] = type(st.session_state[_k])() if isinstance(st.session_state[_k], (dict, list, set)) else None
            st.session_state.token_headers_105 = {}
            st.rerun()
    if auto_connect or btn_connect:
        if pk_105 and sk_105:
            save_creds(pk_105, sk_105)
            r_log = None
            with st.spinner("Connexion à Evoliz en cours..."):
                try:
                    r_log = requests.post("https://www.evoliz.io/api/login",
                                          json={"public_key": pk_105, "secret_key": sk_105}, timeout=15)
                except Exception as e:
                    st.error(f"Erreur de connexion : {e}")
            if r_log is None:
                st.error("Aucune réponse du serveur Evoliz (timeout ou erreur réseau).")
            elif r_log.status_code in (429, 500, 502, 503, 504):
                st.warning(f"API Evoliz temporairement indisponible (HTTP {r_log.status_code}). Réessayez dans quelques instants.")
            elif r_log.status_code == 200:
                login_data = r_log.json()
                h = {"Authorization": f"Bearer {login_data.get('access_token')}", "Accept": "application/json"}
                st.session_state.token_headers_105 = h
                # WARNING: Le token Evoliz expire au bout de 20 minutes.
                # Si vous obtenez des erreurs 401, reconnectez-vous via le bouton "Déconnexion".
                # Découverte des dossiers accessibles (paginé)
                _companies = []
                _co_error = None
                try:
                    _pg = 1
                    while True:
                        r_co = requests.get("https://www.evoliz.io/api/v1/companies", headers=h,
                                            params={"per_page": 100, "page": _pg}, timeout=15)
                        if r_co.status_code == 200:
                            _d = r_co.json()
                            _companies.extend(_d.get('data', []))
                            if _pg >= _d.get("meta", {}).get("last_page", 1):
                                break
                            _pg += 1
                        elif r_co.status_code == 403:
                            _co_error = "scope prescriber_users absent — mode mono-dossier"
                            break
                        else:
                            _co_error = f"HTTP {r_co.status_code}"
                            break
                except Exception as e:
                    _co_error = str(e)
                st.session_state.companies_list = _companies
                if len(_companies) == 1:
                    st.session_state.company_id_105 = _companies[0].get('companyid') or _companies[0].get('id')
                    st.success(f"Connecté à Evoliz — dossier : {_companies[0].get('name', 'N/C')}")
                elif len(_companies) > 1:
                    st.session_state.company_id_105 = None
                    st.success(f"Connecté à Evoliz — {len(_companies)} dossiers accessibles. Sélectionnez un dossier ci-dessous.")
                else:
                    # Pas de scope prescriber_users ou token mono-dossier sans /companies
                    cid = login_data.get('companyid')
                    st.session_state.company_id_105 = cid
                    if _co_error:
                        st.info(f"GET /companies : {_co_error}")
                    if cid:
                        st.success(f"Connecté à Evoliz — mono-dossier (company: {cid})")
                    else:
                        st.warning("Connecté mais aucun dossier détecté. Vérifiez les droits de vos clés API (scope prescriber_users requis pour le multi-dossier).")
            else:
                st.error(f"Échec login : HTTP {r_log.status_code} — {r_log.text[:300]}")
        else:
            st.warning("Renseignez la Public Key et la Secret Key pour vous connecter.")

    # --- Bloc 2 : Sélecteur de dossier (multi-company) ---
    _companies = st.session_state.get('companies_list', [])
    if st.session_state.token_headers_105 and len(_companies) > 1:
        st.divider()
        st.subheader("📁 Sélection du dossier")

        # Filtre par site (home_site.home_site dans le détail company)
        def _get_site(c):
            hs = c.get('home_site')
            if isinstance(hs, dict):
                return hs.get('home_site', '')
            return ''
        _sites = sorted({_get_site(c) for c in _companies})
        _sites = [s for s in _sites if s]

        _filtered = _companies
        if _sites:
            _site_filter = st.selectbox("Filtrer par site", ["— Tous les sites —"] + _sites, key="site_filter")
            if _site_filter != "— Tous les sites —":
                _filtered = [c for c in _companies if _get_site(c) == _site_filter]

        def _get_company_label(c):
            return c.get('company_name') or c.get('name') or f"Company {c.get('companyid')}"
        _company_names = [f"{_get_company_label(c)}" for c in _filtered]
        _company_ids = [c.get('companyid') or c.get('id') for c in _filtered]
        _prev_cid = st.session_state.company_id_105
        _sel_idx = st.selectbox("Dossier Evoliz", range(len(_company_names)),
                                format_func=lambda i: _company_names[i],
                                index=None, placeholder="— Sélectionnez un dossier —",
                                key="company_select")
        if _sel_idx is not None:
            _sel_cid = _company_ids[_sel_idx]
            if _sel_cid != _prev_cid:
                st.session_state.company_id_105 = _sel_cid
                # Réinitialiser les données du précédent dossier
                st.session_state.ev_acc_105 = {}
                st.session_state.ev_data_105 = {"ACHAT": {}, "VENTE": {}, "ENTRÉE BQ": {}, "SORTIE BQ": {}}
                for _k in ("ev_clients_raw", "ev_articles_raw", "ev_invoices_raw"):
                    st.session_state[_k] = []
                st.rerun()


    # --- Affichage du dossier actif ---
    _cid = st.session_state.company_id_105
    if _cid and _companies:
        _current = next((c for c in _companies if (c.get('companyid') or c.get('id')) == _cid), None)
        if _current:
            st.info(f"📂 Dossier actif : **{_get_company_label(_current)}** (ID: {_cid})")

    # --- Bloc 3 : Chargement des données ---
    _is_multi = len(st.session_state.get('companies_list', [])) > 1
    _cid = st.session_state.company_id_105
    _h = st.session_state.token_headers_105
    _data_empty = not (st.session_state.ev_acc_105 or st.session_state.get("ev_clients_raw") or st.session_state.get("ev_articles_raw") or st.session_state.get("ev_invoices_raw"))
    # Chargement auto dès qu'un dossier est sélectionné et que les données sont vides
    _should_load = _h and _cid and _data_empty
    if _should_load:
        _base = f"https://www.evoliz.io/api/v1/companies/{_cid}"
        with st.spinner("Lecture des données comptables Evoliz..."):
            st.session_state.ev_acc_105 = fetch_evoliz_data("accounts", _h, company_id=_cid)
            st.session_state.ev_data_105 = {
                "ACHAT": fetch_evoliz_data("purchase-classifications", _h, company_id=_cid),
                "VENTE": fetch_evoliz_data("sale-classifications", _h, company_id=_cid),
                "ENTRÉE BQ": fetch_evoliz_data("sale-affectations", _h, company_id=_cid),
                "SORTIE BQ": fetch_evoliz_data("purchase-affectations", _h, company_id=_cid),
            }
        with st.spinner("Lecture des clients Evoliz..."):
            _ev_clients = []; _pg = 1
            while True:
                _r = requests.get(f"{_base}/clients", headers=_h, params={"per_page": 100, "page": _pg}, timeout=15)
                if _r.status_code != 200: break
                _d = _r.json(); _ev_clients.extend(_d.get("data", []))
                if _pg >= _d.get("meta", {}).get("last_page", 1): break
                _pg += 1
            st.session_state["ev_clients_raw"] = _ev_clients
        with st.spinner("Lecture des articles Evoliz..."):
            _ev_articles = []; _pg = 1
            while True:
                _r = requests.get(f"{_base}/articles", headers=_h, params={"per_page": 100, "page": _pg}, timeout=15)
                if _r.status_code != 200: break
                _d = _r.json(); _ev_articles.extend(_d.get("data", []))
                if _pg >= _d.get("meta", {}).get("last_page", 1): break
                _pg += 1
            st.session_state["ev_articles_raw"] = _ev_articles
        with st.spinner("Lecture des factures Evoliz (30 derniers jours)..."):
            from datetime import timedelta
            _date_from = (dt_datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            _ev_invoices = []; _pg = 1
            while True:
                _r = requests.get(f"{_base}/invoices", headers=_h, params={"per_page": 100, "page": _pg, "created_after": _date_from}, timeout=15)
                if _r.status_code != 200: break
                _d = _r.json(); _ev_invoices.extend(_d.get("data", []))
                if _pg >= _d.get("meta", {}).get("last_page", 1): break
                _pg += 1
            st.session_state["ev_invoices_raw"] = _ev_invoices
        _company_name = ""
        for _c in st.session_state.get('companies_list', []):
            if (_c.get('companyid') or _c.get('id')) == _cid:
                _company_name = _c.get('name', '')
                break
        st.success(f"Dossier « {_company_name or _cid} » chargé — {len(_ev_clients)} clients, {len(_ev_articles)} articles, {len(_ev_invoices)} factures (30j)")
    elif _h and not _cid and len(_companies) > 1:
        st.info("👆 Sélectionnez un dossier ci-dessus pour charger les données.")

    _has_any_data = (st.session_state.ev_acc_105
                      or st.session_state.get("ev_clients_raw")
                      or st.session_state.get("ev_articles_raw")
                      or st.session_state.get("ev_invoices_raw"))
    if _has_any_data:
        with st.expander("📊 Données lues depuis Evoliz", expanded=True):
            cc1, cc2, cc3 = st.columns(3)
            cc1.metric("👥 Clients", len(st.session_state.get("ev_clients_raw", [])))
            cc2.metric("📦 Articles", len(st.session_state.get("ev_articles_raw", [])))
            cc3.metric("🧾 Factures (30j)", len(st.session_state.get("ev_invoices_raw", [])))
            if mod_compta:
                st.divider()
                st.caption("Pre-comptabilite")
                cp1, cp2, cp3, cp4, cp5 = st.columns(5)
                cp1.metric("Comptes", len(st.session_state.get("ev_acc_105", {})))
                cp2.metric("Achats", count_unique(st.session_state.get("ev_data_105", {}).get("ACHAT", {})))
                cp3.metric("Ventes", count_unique(st.session_state.get("ev_data_105", {}).get("VENTE", {})))
                cp4.metric("Entrees BQ", count_unique(st.session_state.get("ev_data_105", {}).get("ENTRÉE BQ", {})))
                cp5.metric("Sorties BQ", count_unique(st.session_state.get("ev_data_105", {}).get("SORTIE BQ", {})))

# Le code Balance s'execute dans l'onglet Import fichiers (traitement silencieux)
with m_import:
  if mod_compta:
    f105 = st.session_state.get("imp_file_balance")
    if not f105:
        # Fallback : chemin local
        last_bal_path = load_balance_path()
        if last_bal_path:
            f105 = last_bal_path
    if not f105:
        st.info("Chargez vos fichiers ci-dessus.")

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
        # Utiliser l'onglet choisi à l'import (ou le premier par défaut)
        _bal_sheet = st.session_state.get("imp_file_balance_sheet", 0)
        if xl == "HTML":
            sheet_idx = _bal_sheet if isinstance(_bal_sheet, int) else 0
            df_bal_preview = st.session_state._bal_html_fallback[sheet_idx]
        else:
            sheet_bal = _bal_sheet if _bal_sheet != 0 and _bal_sheet in xl.sheet_names else xl.sheet_names[0]
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

        # Présélection par défaut : 1er 44566xxx pour achats, 1er 44562xxx pour immos
        _default_6 = 0  # "— Aucun"
        _default_2 = 0
        for _i, _c in enumerate(comptes_4456):
            if _c['code'].startswith('44566') and _default_6 == 0:
                _default_6 = _i + 1  # +1 car "— Aucun" est en position 0
            if _c['code'].startswith('44562') and _default_2 == 0:
                _default_2 = _i + 1

        col_tva1, col_tva2 = st.columns(2)
        with col_tva1:
            sel_tva_6 = st.selectbox("TVA Deductible (comptes 6xx)", options_4456, index=_default_6, key="sel_tva_6")
        with col_tva2:
            sel_tva_2 = st.selectbox("TVA deductible sur immos (comptes 2xx)", options_4456, index=_default_2, key="sel_tva_2")

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
        # Auto-analyse si fichier charge et pas encore analyse, ou bouton manuel
        _bal_file_id = str(f105) if isinstance(f105, str) else f105.name + str(f105.size)
        _bal_already = st.session_state.get("_bal_analysed_id") == _bal_file_id
        _bal_auto = not _bal_already and not st.session_state.nr_v62.empty
        _bal_manual = st.button("🔍 Re-ANALYSER", use_container_width=True, key="btn_analyse_105")
        if _bal_auto or _bal_manual:
            st.session_state["_bal_analysed_id"] = _bal_file_id
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
                label_flux = clean_label_tva(label, code, fusion_tva)
                pivot_flux = norm_piv(label_flux)
                mvt = has_movement_debit_credit(row, flux_cols) if flux_cols else True

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
    sub_rejets, sub_eraz, sub_audit, sub_matrice, sub_synthese, sub_synchro = st.tabs([
        "🚫 Rejetees", "🧹 Suppressions", "🔎 Mises a jour", "📋 Matrice", "📊 Synthese injection", "🚀 Injection"
    ])
    # Reassigner m6/m7 pour que le code existant ecrive dans les bons sous-onglets
    m6 = sub_synthese
    m7 = sub_synchro

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

            # Init des exclusions de suppression
            if "_skip_delete" not in st.session_state:
                st.session_state._skip_delete = set()

            total_orphans = sum(len(v) for v in orphans_by_cat.values())
            st.subheader(f"🧹 {total_orphans} éléments orphelins")

            for cat, items in orphans_by_cat.items():
                if items:
                    with st.expander(f"{cat} — {len(items)} orphelins", expanded=False):
                        for it in items:
                            _key = (cat, it['id'])
                            _cb = st.checkbox(
                                f"Supprimer `{it['Code']}` — {it['Libellé']}",
                                value=True,
                                key=f"cb_del_{cat}_{it['id']}",
                            )
                            if not _cb:
                                st.session_state._skip_delete.add(_key)
                            else:
                                st.session_state._skip_delete.discard(_key)

            # Compter APRÈS les checkboxes pour avoir l'état à jour
            _filtered_eraz = {}
            for cat, items in orphans_by_cat.items():
                _filtered_eraz[cat] = [it for it in items if (cat, it['id']) not in st.session_state._skip_delete]
            st.session_state.eraz_items = _filtered_eraz

            _n_effective = sum(len(v) for v in _filtered_eraz.values())
            _n_skipped = total_orphans - _n_effective
            if _n_skipped:
                st.info(f"⏭️ {_n_skipped} élément(s) exclus — **{_n_effective}** suppression(s) effectives.")
            else:
                st.success(f"✅ {_n_effective} suppression(s) prévues.")

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

        # --- Visualisation par catégorie : existants + à créer (avec possibilité de désactiver) ---
        st.divider()
        st.subheader("🔍 Détail par catégorie")

        # Init session state pour les créations désactivées
        if "_skip_create" not in st.session_state:
            st.session_state._skip_create = set()  # ensemble de (cat, code) à ne pas créer

        _cat_labels = {
            "COMPTE": "📖 Comptes comptables",
            "ACHAT": "📥 Classifications achat",
            "VENTE": "📤 Classifications vente",
            "ENTRÉE BQ": "📤 Affectations entrées bancaires",
            "SORTIE BQ": "📥 Affectations sorties bancaires",
        }
        for _cat, _cat_label in _cat_labels.items():
            # Existants dans Evoliz
            if _cat == "COMPTE":
                _existing = []
                _seen_ids = set()
                for _v in st.session_state.ev_acc_105.values():
                    if _v['id'] in _seen_ids: continue
                    _seen_ids.add(_v['id'])
                    _existing.append({"Code": _v.get('code', ''), "Libellé": _v.get('label', '')})
            else:
                _flux_data = st.session_state.ev_data_105.get(_cat, {})
                _seen_ids = set()
                _existing = []
                for _v in _flux_data.values():
                    if _v['id'] in _seen_ids: continue
                    _seen_ids.add(_v['id'])
                    _existing.append({"Code": _v.get('code', ''), "Libellé": _v.get('label', ''), "Compte": _v.get('acc_id', '')})

            # À créer (➕) depuis la matrice
            _to_create = []
            for _idx, _row in df_m.iterrows():
                if _row.get(_cat) == '➕':
                    _code = _row.get('N°', '')
                    _label = _row.get('LibFlux', _row.get('Libellé', ''))
                    _key = (_cat, _code, _label)
                    _to_create.append({"Code": _code, "Libellé": _label, "_key": _key, "_idx": _idx})

            # À mettre à jour (🔄) depuis la matrice
            _to_update = []
            for _idx, _row in df_m.iterrows():
                if _row.get(_cat) == '🔄':
                    _to_update.append({"Code": _row.get('N°', ''), "Libellé": _row.get('LibFlux', _row.get('Libellé', ''))})

            _n_exist = len(_existing)
            _n_create = len(_to_create)
            _n_update = len(_to_update)
            _summary = f"{_n_exist} existant(s)"
            if _n_create: _summary += f", {_n_create} à créer"
            if _n_update: _summary += f", {_n_update} à MAJ"

            with st.expander(f"{_cat_label} — {_summary}", expanded=False):
                # Existants
                if _existing:
                    st.caption(f"✅ **{_n_exist} existant(s) dans Evoliz**")
                    st.dataframe(pd.DataFrame(_existing).sort_values("Code"), use_container_width=True, hide_index=True)

                # MAJ
                if _to_update:
                    st.caption(f"🔄 **{_n_update} à mettre à jour**")
                    st.dataframe(pd.DataFrame(_to_update).sort_values("Code"), use_container_width=True, hide_index=True)

                # Créations avec cases à cocher
                if _to_create:
                    st.caption(f"➕ **{_n_create} à créer** — décochez pour désactiver la création")
                    for _item in _to_create:
                        _checked = _item["_key"] not in st.session_state._skip_create
                        _cb = st.checkbox(
                            f"`{_item['Code']}` — {_item['Libellé']}",
                            value=_checked,
                            key=f"cb_create_{_cat}_{_item['_idx']}",
                        )
                        if not _cb and _item["_key"] not in st.session_state._skip_create:
                            st.session_state._skip_create.add(_item["_key"])
                        elif _cb and _item["_key"] in st.session_state._skip_create:
                            st.session_state._skip_create.discard(_item["_key"])

                if not _existing and not _to_create and not _to_update:
                    st.caption("Aucune donnée.")

        # Résumé des créations désactivées
        _n_skipped = len(st.session_state._skip_create)
        if _n_skipped:
            st.warning(f"⚠️ {_n_skipped} création(s) désactivée(s). Ces éléments ne seront pas injectés lors de la synchro.")

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
            new_flux = {f: (to_sync[to_sync[f] == '➕'] if f == "VENTE" else pd.DataFrame()) for f in FLUX_ENDPOINTS}
            patch_flux = {f: (to_sync[to_sync[f] == '🔄'] if f == "VENTE" else pd.DataFrame()) for f in FLUX_ENDPOINTS}
        else:
            new_flux = {f: to_sync[to_sync[f] == '➕'] for f in FLUX_ENDPOINTS}
            patch_flux = {f: to_sync[to_sync[f] == '🔄'] for f in FLUX_ENDPOINTS}

        # Filtrer les créations désactivées par l'utilisateur dans la Synthèse
        _skip = st.session_state.get("_skip_create", set())
        if _skip:
            new_accounts = new_accounts[~new_accounts.apply(
                lambda r: ("COMPTE", r['N°'], r.get('LibFlux', r.get('Libellé', ''))) in _skip, axis=1)]
            for _f in new_flux:
                if not new_flux[_f].empty:
                    new_flux[_f] = new_flux[_f][~new_flux[_f].apply(
                        lambda r, _cat=_f: (_cat, r['N°'], r.get('LibFlux', r.get('Libellé', ''))) in _skip, axis=1)]

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

            def _add_flux_rate(flux_type, code, label, hdrs, vat_id=None, acc_id=None, compte_code=None):
                rate_wait()
                # Appliquer le taux de TVA pour les classifications d'achat (comptes 2xx/6xx)
                _vr = None
                if flux_type == "ACHAT" and compte_code and str(compte_code).startswith(('2', '6')):
                    _vr = tva_achat_rate
                return inject_flux(flux_type, code, label, hdrs, vat_id=vat_id, acc_id=acc_id, vat_rate=_vr)

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
                st.session_state.ev_acc_105 = fetch_evoliz_data("accounts", headers, company_id=cid)

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
                        # Ajouter vat_rate pour les classifications d'achat (comptes 2xx/6xx)
                        if isinstance(payload, dict) and flux == "ACHAT" and str(row['N°']).startswith(('2', '6')):
                            payload['vat_rate'] = float(tva_achat_rate)
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
                st.session_state.ev_acc_105 = fetch_evoliz_data("accounts", headers, company_id=cid)

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
                    flux_tasks.append((flux, row['N°'], label_for_flux, row_vat, row_acc_id, row['N°']))
            if flux_tasks:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = {pool.submit(_add_flux_rate, t[0], t[1], t[2], headers, vat_id=t[3], acc_id=t[4], compte_code=t[5]): t for t in flux_tasks}
                    for f in as_completed(futures):
                        flux, code, lbl, _, _, _ = futures[f]
                        ok, resp = f.result()
                        log.append({"Action": "➕ ADD", "Type": flux, "Code": code,
                                    "Libellé": lbl, "Résultat": "✅" if ok else "❌", "Détail": str(resp)[:120]})
                        update_progress()

            st.session_state.sync_log = log
            ok_count = sum(1 for l in log if l['Résultat'] == '✅')
            st.success(f"Terminé : {ok_count}/{len(log)} opérations réussies")

            with st.spinner("Rafraîchissement des données API..."):
                st.session_state.ev_acc_105 = fetch_evoliz_data("accounts", headers, company_id=cid)
                st.session_state.ev_data_105 = {
                    "ACHAT": fetch_evoliz_data("purchase-classifications", headers, company_id=cid),
                    "VENTE": fetch_evoliz_data("sale-classifications", headers, company_id=cid),
                    "ENTRÉE BQ": fetch_evoliz_data("sale-affectations", headers, company_id=cid),
                    "SORTIE BQ": fetch_evoliz_data("purchase-affectations", headers, company_id=cid),
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
                        norm_piv(clean_label_tva(r['Libellé'], r['N°'], fusion_tva))
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
                        norm_piv(clean_label_tva(r['Libellé'], r['N°'], fusion_tva)): r.get('LibFlux', r['Libellé'])
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


# =========================================================
# BASCULE MEG -> EVOLIZ
# =========================================================

ISO2_MAP = {
    "AFGHANISTAN": "AF", "ALBANIE": "AL", "ALGERIE": "DZ", "ALLEMAGNE": "DE",
    "ANDORRE": "AD", "ANGOLA": "AO", "ARABIE SAOUDITE": "SA", "ARGENTINE": "AR",
    "AUSTRALIE": "AU", "AUTRICHE": "AT", "BELGIQUE": "BE", "BRESIL": "BR",
    "BULGARIE": "BG", "BURKINA FASO": "BF", "CAMEROUN": "CM", "CANADA": "CA",
    "CHILI": "CL", "CHINE": "CN", "COLOMBIE": "CO", "COREE DU SUD": "KR",
    "COSTA RICA": "CR", "COTE D'IVOIRE": "CI", "CROATIE": "HR", "CUBA": "CU",
    "DANEMARK": "DK", "EGYPTE": "EG", "EMIRATS ARABES UNIS": "AE", "ESPAGNE": "ES",
    "ESTONIE": "EE", "ETATS-UNIS": "US", "FINLANDE": "FI", "FRANCE": "FR",
    "GABON": "GA", "GEORGIE": "GE", "GHANA": "GH", "GRECE": "GR",
    "GUADELOUPE": "GP", "GUATEMALA": "GT", "GUINEE": "GN", "GUYANE FRANCAISE": "GF",
    "HAITI": "HT", "HONDURAS": "HN", "HONG KONG": "HK", "HONGRIE": "HU",
    "INDE": "IN", "INDONESIE": "ID", "IRAK": "IQ", "IRAN": "IR", "IRLANDE": "IE",
    "ISLANDE": "IS", "ISRAEL": "IL", "ITALIE": "IT", "JAMAIQUE": "JM", "JAPON": "JP",
    "JORDANIE": "JO", "KAZAKHSTAN": "KZ", "KENYA": "KE", "KOWEIT": "KW",
    "LAOS": "LA", "LETTONIE": "LV", "LIBAN": "LB", "LIBYE": "LY",
    "LIECHTENSTEIN": "LI", "LITUANIE": "LT", "LUXEMBOURG": "LU", "MACEDOINE": "MK",
    "MADAGASCAR": "MG", "MALAISIE": "MY", "MALI": "ML", "MALTE": "MT",
    "MAROC": "MA", "MARTINIQUE": "MQ", "MAURICE": "MU", "MAURITANIE": "MR",
    "MAYOTTE": "YT", "MEXIQUE": "MX", "MOLDAVIE": "MD", "MONACO": "MC",
    "MONGOLIE": "MN", "MONTENEGRO": "ME", "MOZAMBIQUE": "MZ", "NAMIBIE": "NA",
    "NEPAL": "NP", "NICARAGUA": "NI", "NIGER": "NE", "NIGERIA": "NG",
    "NORVEGE": "NO", "NOUVELLE-ZELANDE": "NZ", "OMAN": "OM", "OUGANDA": "UG",
    "PAKISTAN": "PK", "PALESTINE": "PS", "PANAMA": "PA", "PARAGUAY": "PY",
    "PAYS-BAS": "NL", "PEROU": "PE", "PHILIPPINES": "PH", "POLOGNE": "PL",
    "PORTUGAL": "PT", "QATAR": "QA", "REPUBLIQUE TCHEQUE": "CZ", "REUNION": "RE",
    "ROUMANIE": "RO", "ROYAUME-UNI": "GB", "RUSSIE": "RU", "SENEGAL": "SN",
    "SERBIE": "RS", "SINGAPOUR": "SG", "SLOVAQUIE": "SK", "SLOVENIE": "SI",
    "SOUDAN": "SD", "SRI LANKA": "LK", "SUEDE": "SE", "SUISSE": "CH",
    "SYRIE": "SY", "TAIWAN": "TW", "TANZANIE": "TZ", "TCHAD": "TD",
    "THAILANDE": "TH", "TOGO": "TG", "TUNISIE": "TN", "TURQUIE": "TR",
    "UKRAINE": "UA", "URUGUAY": "UY", "VENEZUELA": "VE", "VIET NAM": "VN",
    "YEMEN": "YE", "ZAMBIE": "ZM", "ZIMBABWE": "ZW",
    "GERMANY": "DE", "AUSTRALIA": "AU", "AUSTRIA": "AT", "BELGIUM": "BE",
    "BRAZIL": "BR", "CANADA": "CA", "CHINA": "CN", "DENMARK": "DK",
    "SPAIN": "ES", "FINLAND": "FI", "FRANCE": "FR", "GREECE": "GR",
    "INDIA": "IN", "IRELAND": "IE", "ITALY": "IT", "JAPAN": "JP",
    "LUXEMBOURG": "LU", "MOROCCO": "MA", "MEXICO": "MX", "NORWAY": "NO",
    "NETHERLANDS": "NL", "POLAND": "PL", "PORTUGAL": "PT",
    "UNITED KINGDOM": "GB", "ROMANIA": "RO", "SWEDEN": "SE",
    "SWITZERLAND": "CH", "TURKEY": "TR", "UNITED STATES": "US", "ENGLAND": "GB",
}

# Table NAF rev.2 : code -> libelle
_NAF_PATH = os.path.join(APP_DIR, "naf_rev2.json")
if os.path.exists(_NAF_PATH):
    with open(_NAF_PATH, "r", encoding="utf-8") as _f:
        NAF_LABELS = json.load(_f)
else:
    NAF_LABELS = {}

def _naf_label(code):
    """Retourne le libelle NAF pour un code (ex: '58.29C' -> 'Edition de logiciels applicatifs')."""
    if not code: return ""
    c = str(code).strip()
    if c in NAF_LABELS: return NAF_LABELS[c]
    # Essayer sans la lettre finale (XX.XX)
    if len(c) >= 5 and c[:-1] in NAF_LABELS: return NAF_LABELS[c[:-1]]
    return c  # fallback : retourner le code tel quel

# --- Normalisation forme juridique ---
# Codes Evoliz : https://evoliz.io/documentation#section/Legal-status-list
EVOLIZ_LEGAL = {
    "1": "Association", "2": "Auto entrepreneur", "3": "EIRL",
    "4": "Entreprise Individuelle", "5": "EURL", "6": "GIE",
    "7": "Independant - AGESSA", "8": "Independant - Maison des Artistes",
    "9": "SA", "10": "SARL", "11": "SARL a capital variable",
    "12": "SAS", "13": "SASU", "14": "SNC", "15": "SELARL",
    "16": "Profession liberale", "17": "SCI", "18": "SCS",
    "19": "Societe Civile", "20": "SCM", "21": "SEP", "22": "SCP",
    "23": "SCOP SA", "24": "SCOP SARL", "25": "SCIC SA", "26": "SCIC SARL",
    "27": "SELAFA", "28": "SELAS", "29": "SELCA", "30": "SPRL",
}

# Code INSEE nature_juridique -> label Evoliz
# Ref: https://www.insee.fr/fr/information/2028129
_INSEE_TO_EVOLIZ = {
    # Entrepreneur individuel
    "1000": "Entreprise Individuelle",
    # Auto-entrepreneur / micro
    "1300": "Auto entrepreneur",
    # EIRL
    "1400": "EIRL",
    # GIE
    "6100": "GIE",
    # SA a conseil d'admin
    "5505": "SA", "5510": "SA", "5515": "SA", "5520": "SA",
    "5522": "SA", "5525": "SA", "5530": "SA", "5531": "SA",
    "5532": "SA", "5535": "SA", "5538": "SA",
    # SA a directoire
    "5605": "SA", "5610": "SA", "5615": "SA", "5620": "SA",
    "5622": "SA", "5625": "SA", "5630": "SA", "5631": "SA",
    "5632": "SA", "5635": "SA", "5638": "SA",
    # SAS
    "5710": "SAS",
    # SASU
    "5720": "SASU",
    # SARL
    "5499": "SARL", "5498": "SARL",
    # EURL
    "5498": "EURL",
    # SARL
    "5410": "SARL", "5415": "SARL", "5422": "SARL",
    "5426": "SARL", "5430": "SARL", "5431": "SARL",
    "5432": "SARL", "5442": "SARL", "5443": "SARL",
    "5451": "SARL", "5453": "SARL", "5454": "SARL",
    "5455": "SARL", "5458": "SARL", "5459": "SARL",
    "5460": "SARL",
    # SNC
    "5202": "SNC", "5203": "SNC",
    # SCS
    "5306": "SCS", "5307": "SCS", "5308": "SCS",
    # SCI
    "6540": "SCI",
    # SCM
    "6542": "SCM",
    # SCP
    "6543": "SCP",
    # Societe civile
    "6553": "Societe Civile", "6554": "Societe Civile",
    "6558": "Societe Civile", "6560": "Societe Civile",
    # SEP
    "6539": "SEP",
    # SELARL
    "5485": "SELARL",
    # SELAS
    "5785": "SELAS",
    # SELAFA
    "5585": "SELAFA",
    # SELCA
    "5385": "SELCA",
    # SCOP
    "5451": "SCOP SARL", "5547": "SCOP SA",
    # Association
    "9210": "Association", "9220": "Association", "9221": "Association",
    "9222": "Association", "9223": "Association", "9224": "Association",
    "9230": "Association", "9240": "Association", "9260": "Association",
    # Profession liberale
    "0000": "Profession liberale",
}

def _normalize_forme_juridique(value):
    """Normalise une forme juridique (code INSEE, texte libre) vers un label Evoliz."""
    if not value:
        return ""
    s = str(value).strip()

    # Si c'est un code INSEE numerique
    if s.isdigit():
        # Match exact
        if s in _INSEE_TO_EVOLIZ:
            return _INSEE_TO_EVOLIZ[s]
        # Match par prefixe (ex: 5710 -> 57xx)
        for prefix_len in [4, 3, 2]:
            prefix = s[:prefix_len]
            for code, label in _INSEE_TO_EVOLIZ.items():
                if code.startswith(prefix):
                    return label
        return ""  # code INSEE inconnu, vide pour ne pas bloquer l'import

    # Si c'est du texte, essayer de matcher vers un label Evoliz
    up = s.upper().strip()
    # Mapping texte courant -> Evoliz
    _TEXT_MAP = {
        "SARL": "SARL", "S.A.R.L.": "SARL", "S.A.R.L": "SARL",
        "SAS": "SAS", "S.A.S.": "SAS", "S.A.S": "SAS",
        "SASU": "SASU", "S.A.S.U.": "SASU",
        "SA": "SA", "S.A.": "SA", "S.A": "SA",
        "EURL": "EURL", "E.U.R.L.": "EURL",
        "EIRL": "EIRL", "E.I.R.L.": "EIRL",
        "EI": "Entreprise Individuelle", "ENTREPRISE INDIVIDUELLE": "Entreprise Individuelle",
        "SNC": "SNC", "S.N.C.": "SNC",
        "SCI": "SCI", "S.C.I.": "SCI",
        "SCM": "SCM", "S.C.M.": "SCM",
        "SCP": "SCP", "S.C.P.": "SCP",
        "SCS": "SCS", "S.C.S.": "SCS",
        "SEP": "SEP", "S.E.P.": "SEP",
        "GIE": "GIE", "G.I.E.": "GIE",
        "SELARL": "SELARL", "SELAS": "SELAS", "SELAFA": "SELAFA", "SELCA": "SELCA",
        "SCOP": "SCOP SARL", "SCIC": "SCIC SARL",
        "SPRL": "SPRL",
        "ASSOCIATION": "Association", "ASSOC": "Association", "ASSO": "Association",
        "ASSOCIATION LOI 1901": "Association", "ASSOCIATION DECLAREE": "Association",
        "AUTO ENTREPRENEUR": "Auto entrepreneur", "AUTO-ENTREPRENEUR": "Auto entrepreneur",
        "AUTOENTREPRENEUR": "Auto entrepreneur",
        "MICRO ENTREPRISE": "Auto entrepreneur", "MICRO-ENTREPRISE": "Auto entrepreneur",
        "MICROENTREPRISE": "Auto entrepreneur",
        "PROFESSION LIBERALE": "Profession liberale", "PROF. LIBERALE": "Profession liberale",
        "SOCIETE CIVILE": "Societe Civile",
        "SOCIETE PAR ACTIONS SIMPLIFIEE": "SAS", "SOCIETE PAR ACTIONS SIMPLIFIEES": "SAS",
        "SOCIETE A RESPONSABILITE LIMITEE": "SARL",
        "SOCIETE ANONYME": "SA",
        "SOCIETE EN NOM COLLECTIF": "SNC",
        "SOCIETE CIVILE IMMOBILIERE": "SCI",
        "SOCIETE CIVILE DE MOYENS": "SCM",
        "SOCIETE CIVILE PROFESSIONNELLE": "SCP",
        "SOCIETE EN COMMANDITE SIMPLE": "SCS",
        "GROUPEMENT D'INTERET ECONOMIQUE": "GIE",
        "ENTREPRISE UNIPERSONNELLE": "EURL",
    }
    if up in _TEXT_MAP:
        return _TEXT_MAP[up]
    # Recherche partielle : contient un des mots cles
    for key, val in _TEXT_MAP.items():
        if key in up:
            return val
    # Rien trouve : vide pour ne pas bloquer l'import Evoliz
    return ""

def _lookup_iso2(country):
    if not country: return "FR"
    n = norm_piv(country)
    for name, code in ISO2_MAP.items():
        if norm_piv(name) == n: return code
    for name, code in ISO2_MAP.items():
        if norm_piv(name) in n or n in norm_piv(name): return code
    return "FR"

def _make_wb(headers):
    wb = Workbook(); ws = wb.active; ws.append(headers); return wb, ws

def _wb_bytes(wb):
    buf = io.BytesIO(); wb.save(buf); buf.seek(0); return buf.getvalue()

def _read_meg(f, sheet_name=0):
    for engine in [None, "openpyxl", "xlrd"]:
        try:
            f.seek(0); return pd.read_excel(f, header=0, engine=engine, sheet_name=sheet_name)
        except Exception: pass
    # Fallback HTML (certains .xls sont du HTML déguisé)
    try:
        f.seek(0)
        raw = f.read() if hasattr(f, 'read') else open(f, 'rb').read()
        dfs = pd.read_html(raw, header=0)
        if dfs:
            idx = sheet_name if isinstance(sheet_name, int) else 0
            return dfs[idx] if idx < len(dfs) else dfs[0]
    except Exception: pass
    raise ValueError("Impossible de lire ce fichier.")

def _safe_float(v):
    try: return float(v)
    except (ValueError, TypeError): return 0.0

def _detect_type_from_name(name):
    """Analyse un nom pour determiner le type client Evoliz :
    'Particulier', 'Professionnel' ou 'Administration publique'."""
    if not name:
        return "Professionnel"
    s = name.strip()
    up = s.upper()

    # --- Administration publique ---
    admin_markers = [
        "MAIRIE", "COMMUNE ", "COMMUNAUTE ", "CONSEIL DEPARTEMENTAL",
        "CONSEIL REGIONAL", "CONSEIL GENERAL", "CONSEIL MUNICIPAL",
        "REGION ", "DEPARTEMENT ", "PREFECTURE", "SOUS-PREFECTURE",
        "MINISTERE", "TRESOR PUBLIC", "TRESORERIE", "DGFIP",
        "LYCEE", "COLLEGE", "ECOLE PUBLIQUE", "UNIVERSITE",
        "CENTRE HOSPITALIER", "CHU ", "CHR ", "HOPITAL PUBLIC",
        "CNRS", "INRA", "INSERM", "INSEE", "POLE EMPLOI",
        "CHAMBRE DE COMMERCE", "CCI ", "CHAMBRE DES METIERS",
        "CHAMBRE D'AGRICULTURE", "OFFICE PUBLIC", "OPH ",
        "ETABLISSEMENT PUBLIC", "EPIC ", "EPA ",
        "CAISSE DES DEPOTS", "CDC ", "CPAM ", "CAF ",
        "URSSAF", "MSA ", "CARSAT",
    ]
    for marker in admin_markers:
        if marker in up:
            return "Administration publique"

    # --- Professionnel (societes, structures, commerces) ---
    pro_markers = [
        "SARL", "SAS", "SASU", "SA ", "SCI", "SNC", "EURL", "SELARL", "SELAS",
        "EARL", "GIE", "SCOP", "SCA", "SCS", "SEP", "GAEC", "SELAFA", "SELCA",
        "EI ", "MICRO", "AUTO-ENTREPRENEUR",
        "ASSOCIATION", "ASSOC", "FONDATION",
        "CABINET", "OFFICE", "GROUPE", "HOLDING", "SOCIETE",
        "PHARMACIE", "CLINIQUE", "LABORATOIRE", "LABO",
        "BOULANGERIE", "RESTAURANT", "HOTEL", "GARAGE", "PRESSING",
        "INSTITUT", "AGENCE", "SYNDIC", "COPROPRIETE",
        "INTERNATIONAL", "SERVICES", "CONSULTING", "CONSEIL",
        "TRANSPORT", "LOGISTIQUE", "IMMOBILIER",
        "MUTUELLE", "BANQUE", "CREDIT", "ASSURANCE",
        "&", " ET FILS", " ET CIE", " ET ASSOCIES", " ET COMPAGNIE",
        "CHEZ ", "C/O ",
    ]
    for marker in pro_markers:
        if marker in up:
            return "Professionnel"

    # --- Heuristiques pour Particulier ---
    # Chiffres dans le nom -> probablement pas une personne
    if re.search(r'\d', s):
        return "Professionnel"
    # Un seul mot -> probablement un nom de societe
    words = s.split()
    if len(words) == 1:
        return "Professionnel"
    # Plus de 3 mots -> probablement un nom de societe
    if len(words) > 3:
        return "Professionnel"
    # Plus de 35 caracteres
    if len(s) > 35:
        return "Professionnel"
    # 2-3 mots, uniquement des lettres/tirets -> probablement Prenom Nom
    if all(re.match(r'^[A-Za-z\u00C0-\u00FF\-\']+$', w) for w in words):
        return "Particulier"
    return "Professionnel"

def _normalize_type(type_val, nom_societe=""):
    """Normalise un champ type source vers une valeur Evoliz.
    Si le champ type n'est pas exploitable, analyse le nom."""
    if type_val:
        tv = str(type_val).upper().strip()
        # Valeurs directement reconnues
        if tv in ("PRO", "PROFESSIONNEL", "ENTREPRISE", "SOCIETE", "B2B", "PROFESSIONAL"):
            return "Professionnel"
        if tv in ("PART", "PARTICULIER", "INDIVIDU", "B2C", "PERSONNE PHYSIQUE", "INDIVIDUAL", "PERSONAL"):
            return "Particulier"
        if tv in ("ADMIN", "ADMINISTRATION", "ADMINISTRATION PUBLIQUE", "PUBLIC",
                   "COLLECTIVITE", "COLLECTIVITE TERRITORIALE", "ETAT"):
            return "Administration publique"
        # Valeur non reconnue -> essayer d'analyser le contenu
        # Certains fichiers mettent le type juridique dans le champ type
        for marker in ["SARL", "SAS", "SASU", "SA", "SCI", "EURL", "SNC", "GIE"]:
            if marker in tv:
                return "Professionnel"
        for marker in ["MAIRIE", "COMMUNE", "PREFECTURE", "MINISTERE", "LYCEE", "COLLEGE"]:
            if marker in tv:
                return "Administration publique"
    # Type vide ou non exploitable -> analyser le nom
    return _detect_type_from_name(nom_societe)

def _parse_date(v):
    if isinstance(v, dt_datetime): return v
    if pd.isna(v): return ""
    for fmt in ["%d/%m/%Y", "%Y-%m-%d"]:
        try: return dt_datetime.strptime(str(v).strip(), fmt)
        except (ValueError, TypeError): pass
    return v

H_CLIENT = ["Code *","Date de creation","Societe / Nom *","Type *","Civilite","Forme juridique","Siren","APE / NAF","TVA intracommunautaire","Numero Immatriculation","Banque","RIB","IBAN","BIC","Adresse","Complement d'adresse","Complement d'adresse (suite)","Code postal *","Ville *","Pays","Code pays (ISO 2) *","Siret","Nb adresses livraison","Telephone","Portable","Fax","Site web","Montant de l'encours garanti","Commentaires","Desactive","Taux de penalite","Aucun Taux de penalite","Frais de recouvrement","Taux d'escompte","Aucun Taux d'escompte","Conditions de reglement","Mode de paiement","Duree de validite","Mode de saisie des prix","Taux de TVA","Remise globale","Article d'exoneration","ZRR","Code ZRR","Devise"]
H_FOURNISSEUR = ["Code *","Date de creation","Raison sociale *","Forme juridique","Siret","APE / NAF","TVA intracommunautaire","RIB","IBAN","BIC","Adresse","Adresse (suite)","Code postal","Ville","Pays","Code pays (ISO 2)","Telephone","Portable","Fax","Site web","Classification","Code classification","Commentaires","Desactive","Conditions de reglement","Mode de paiement","Axe 1","Code Axe 1"]
H_CONTACT = ["Nom Client","Code client *","Civilite","Nom *","Prenom","E-mail","Metier/Fonction","Consentement *","Libelle Telephone","Telephone","Libelle Telephone 2","Telephone 2","Libelle Telephone 3","Telephone 3","Desactive"]
H_FACTURE = ["N facture externe *","Date facture *","Client","Code client *","Nom adresse de livraison","Code adresse de livraison","Total TVA","Total HT","Total TTC","Total regle","Etat","Date Etat","Date de creation","Objet","Date d'echeance","Date d'execution","Taux de penalite","Frais de recouvrement","Taux d'escompte","Conditions de reglement *","Mode de paiement","Remise globale","Acompte","Nombre de relance","Commentaires","N facture","Annule","Catalogue","Ref.","Designation *","Qte *","Unite","PU HT *","Remise","TVA","Total TVA","Total HT","Classification vente","Code Classification vente","Prix d'achat HT","Createur"]
H_AVOIR = ["N avoir externe *","Date avoir *","Client","Code client *","Nom adresse de livraison","Code adresse de livraison","Total TVA","Total HT","Total TTC","Etat","Date de creation avoir","Objet","Date d'echeance","Taux de penalite","Frais de recouvrement","Taux d'escompte","Conditions de reglement *","Mode de paiement","Remise globale","Acompte","Commentaires","Annule","Catalogue","Ref.","Designation *","Qte *","Unite","PU HT *","Remise","TVA","Total TVA","Total HT","Classification vente","Code Classification vente","Createur"]
H_PAIEMENT = ["Facture n *","Date paiement *","Date de creation","Client","Code client","Libelle *","Mode de paiement *","Montant *","Commentaires","Createur"]
H_ARTICLE = ["Reference *","Nature","Classification vente","Code Classification vente","Designation *","Quantite","Poids par unite","Unite","PU HT","PU TTC","TVA","Saisie en TTC","Prix d'achat HT","Classification achat","Code Classification achat","Coefficient multiplicateur","% Marque","% Marge","Marge brute","Fournisseur","Code fournisseur","Ref. Fournisseur","Article stocke","Qte stockee","Desactive","Createur"]

GABARIT_CLIENT_PATH = os.path.join(APP_DIR, ".gabarit_client_meg.xlsx")

# --- Auto-mapping colonnes ---
# Champs Evoliz client -> mots-cles pour detection automatique
_EVOLIZ_FIELD_KEYWORDS = {
    "Code":                 ["code", "ref", "id client", "identifiant", "numero client", "n client", "code client"],
    "Societe / Nom":        ["societe", "raison sociale", "nom", "denomination", "entreprise", "client", "name", "company"],
    "Type":                 ["type", "nature", "categorie"],
    "Civilite":             ["civilite", "titre", "civ"],
    "Forme juridique":      ["forme juridique", "statut juridique", "forme legale", "forme jur"],
    "Siren":                ["siren"],
    "APE / NAF":            ["ape", "naf", "code activite"],
    "TVA intracommunautaire": ["tva intra", "tva", "n tva", "vat", "num tva"],
    "Adresse":              ["adresse", "adresse 1", "rue", "voie", "address", "adresse facturation", "auto adresse rue"],
    "Complement d'adresse": ["complement", "adresse 2", "adresse2", "complement adresse"],
    "Code postal":          ["code postal", "cp", "postal", "zip", "auto code postal"],
    "Ville":                ["ville", "commune", "city", "localite", "auto ville"],
    "Pays":                 ["pays", "country"],
    "Code pays (ISO 2)":    ["iso", "code pays", "country code", "iso2", "auto pays iso2"],
    "Siret":                ["siret"],
    "Telephone":            ["telephone", "tel", "phone", "tel fixe", "tel bureau"],
    "Portable":             ["portable", "mobile", "gsm", "cell"],
    "Fax":                  ["fax", "telecopie"],
    "Site web":             ["site", "web", "url", "site web", "website"],
    "Commentaires":         ["commentaire", "note", "observation", "memo", "comment"],
    "E-mail":               ["email", "e-mail", "mail", "courriel", "adresse mail"],
    "Prenom":               ["prenom", "first name", "firstname"],
    "Nom contact":          ["nom contact", "nom", "last name", "lastname", "nom de famille"],
}

def _auto_map_columns(src_columns):
    """Propose un mapping automatique des colonnes source vers les champs Evoliz."""
    mapping = {}

    for evoliz_field, keywords in _EVOLIZ_FIELD_KEYWORDS.items():
        best_match = None
        best_score = 0
        for src_col in src_columns:
            sn = norm_piv(str(src_col))
            if not sn:
                continue
            for kw in keywords:
                kn = norm_piv(kw)
                if not kn:
                    continue
                score = 0
                if kn == sn:
                    # Match exact normalise
                    score = 100
                elif kn in sn and len(kn) >= len(sn) * 0.6:
                    # Keyword contenu dans source, mais assez long pour eviter faux positifs
                    score = 85
                elif sn in kn and len(sn) >= len(kn) * 0.6:
                    # Source contenu dans keyword, assez long
                    score = 80
                elif kn in sn:
                    # Keyword court contenu dans source plus longue
                    score = 50 + len(kn) * 2
                if score > best_score:
                    best_score = score
                    best_match = src_col
        if best_match and best_score >= 50:
            mapping[evoliz_field] = best_match
    return mapping

# --- Onglet Injection Clients ---
with m_cli:
    _is_supplier = False
    _entity_label = "clients"
    _entity_api = "clients"
    _entity_id_field = "clientid"
    _entity_name_field = "Societe / Nom"
    _H_ENTITY = H_CLIENT
    st.subheader("👥 Injection Clients")
    st.caption("1. Importez un fichier clients  2. Consolidation avec Evoliz  3. Enrichissement Sirene  4. Injection")

    # --- Etape 1 : Upload fichier ---
    f_meg_cli = st.session_state.get("imp_file_clients")
    if not f_meg_cli:
        st.info("Importez d'abord un fichier clients dans l'onglet 📁 Import fichiers.")

    if f_meg_cli:
        # Lecture du fichier
        if f_meg_cli.name.lower().endswith(".csv"):
            # Essayer plusieurs encodages (utf-8, latin-1, cp1252)
            df_src = None
            for _enc in ["utf-8", "latin-1", "cp1252"]:
                for _sep in [None, ";", ","]:
                    try:
                        f_meg_cli.seek(0)
                        _kw = {"header": 0, "encoding": _enc}
                        if _sep: _kw["sep"] = _sep
                        else: _kw["sep"] = None; _kw["engine"] = "python"
                        df_src = pd.read_csv(f_meg_cli, **_kw)
                        if len(df_src.columns) > 1: break
                        df_src = None
                    except Exception:
                        df_src = None
                if df_src is not None: break
            if df_src is None:
                st.error("Impossible de lire ce fichier CSV. Verifiez l'encodage.")
                f_meg_cli = None
        else:
            _cli_sheet = st.session_state.get("imp_file_clients_sheet", 0)
            df_src = _read_meg(f_meg_cli, sheet_name=_cli_sheet)

        st.caption(f"Fichier lu : **{len(df_src)} lignes**, **{len(df_src.columns)} colonnes**")

        # --- Detection et parsing adresse -> colonnes virtuelles ---
        # Si une colonne ressemble a une adresse mais qu'il n'y a pas de colonne CP/Ville,
        # on parse l'adresse pour creer des colonnes synthetiques
        fwd_test = _auto_map_columns(df_src.columns.tolist())
        has_adr_col = "Adresse" in fwd_test
        has_cp_col = "Code postal" in fwd_test
        has_ville_col = "Ville" in fwd_test
        has_pays_col = "Pays" in fwd_test or "Code pays (ISO 2)" in fwd_test

        if has_adr_col and (not has_cp_col or not has_ville_col):
            adr_src_col = fwd_test["Adresse"]
            parsed_rues, parsed_cps, parsed_villes, parsed_pays = [], [], [], []
            for _, row in df_src.iterrows():
                adr = to_clean_str(row.get(adr_src_col, ""))
                m_adr = re.search(r'^(.*?)\s+(\d{5})\s+(.+)$', adr) if adr else None
                if m_adr:
                    parsed_rues.append(m_adr.group(1).strip())
                    parsed_cps.append(m_adr.group(2))
                    reste = m_adr.group(3).strip().upper()
                    ville_p, pays_p = reste, ""
                    if "FRANCE" in ville_p:
                        ville_p = ville_p.replace("FRANCE", "").strip()
                        pays_p = "FR"
                    else:
                        for cn, cc in ISO2_MAP.items():
                            if cn in ville_p:
                                ville_p = ville_p.replace(cn, "").strip()
                                pays_p = cc
                                break
                    parsed_villes.append(ville_p)
                    parsed_pays.append(pays_p if pays_p else "FR")
                else:
                    parsed_rues.append(adr)
                    parsed_cps.append("")
                    parsed_villes.append("")
                    parsed_pays.append("")

            # Ajouter les colonnes virtuelles au DataFrame
            added_cols = []
            if not has_cp_col and any(parsed_cps):
                df_src["[Auto] Code postal"] = parsed_cps
                added_cols.append("Code postal")
            if not has_ville_col and any(parsed_villes):
                df_src["[Auto] Ville"] = parsed_villes
                added_cols.append("Ville")
            if not has_pays_col and any(parsed_pays):
                df_src["[Auto] Pays ISO2"] = parsed_pays
                added_cols.append("Pays ISO2")
            # Remplacer l'adresse originale par la rue seule (in-place)
            df_src[adr_src_col] = parsed_rues
            added_cols.append(f"Adresse nettoyee (rue seule dans '{adr_src_col}')")

            if added_cols:
                st.info(f"🔄 Colonnes generees depuis l'adresse : **{', '.join(added_cols)}**")

        st.dataframe(df_src.head(5), use_container_width=True, hide_index=True)

        # --- Auto-mapping inverse ---
        if _is_supplier:
            all_evoliz_fields = ["— Ignorer",
                "Raison sociale", "Code", "Code postal", "Ville", "Code pays (ISO 2)",
                "Forme juridique", "Siren", "Siret", "APE / NAF", "TVA intracommunautaire",
                "Adresse", "Adresse (suite)", "Pays",
                "Telephone", "Portable", "Fax", "E-mail", "Site web",
                "Classification", "Code classification", "Commentaires"]
            required_set = {"Raison sociale", "Code"}
        else:
            all_evoliz_fields = ["— Ignorer",
                "Societe / Nom", "Code", "Type", "Code postal", "Ville", "Code pays (ISO 2)",
                "Civilite", "Prenom", "Nom contact",
                "Forme juridique", "Siren", "Siret", "APE / NAF", "TVA intracommunautaire",
                "Adresse", "Complement d'adresse", "Pays",
                "Telephone", "Portable", "Fax", "E-mail", "Site web", "Commentaires"]
            required_set = {"Societe / Nom", "Code", "Code postal", "Ville", "Code pays (ISO 2)"}

        # Recalculer le mapping si fichier change ou si colonnes ont change (ajout auto)
        current_cols_sig = ",".join(df_src.columns.tolist())
        if (st.session_state.get("meg_last_file") != f_meg_cli.name
                or st.session_state.get("meg_last_cols_sig") != current_cols_sig):
            # Auto-detect : pour chaque colonne source, quel champ Evoliz ?
            fwd = _auto_map_columns(df_src.columns.tolist())  # evoliz_field -> src_col
            rev = {}  # src_col -> evoliz_field
            for ef, sc in fwd.items():
                if sc not in rev:  # premiere correspondance gagne
                    rev[sc] = ef
            st.session_state["meg_col_mapping_rev"] = rev
            st.session_state["meg_last_file"] = f_meg_cli.name
            st.session_state["meg_last_cols_sig"] = current_cols_sig

        rev_map = st.session_state["meg_col_mapping_rev"]

        st.divider()
        st.subheader("🔗 Mapping des colonnes du fichier")
        st.caption("Pour chaque colonne de votre fichier, indiquez a quel champ Evoliz elle correspond.")

        # Affichage en 2 colonnes : colonne source | champ Evoliz cible
        mapping_rev = {}  # src_col -> evoliz_field
        n_cols = len(df_src.columns)
        cols_per_row = 2
        for i in range(0, n_cols, cols_per_row):
            ui_cols = st.columns(cols_per_row)
            for j in range(cols_per_row):
                if i + j >= n_cols:
                    break
                src_col = df_src.columns[i + j]
                default_ev = rev_map.get(src_col, "— Ignorer")
                idx = all_evoliz_fields.index(default_ev) if default_ev in all_evoliz_fields else 0
                with ui_cols[j]:
                    chosen = st.selectbox(
                        f"📄 **{src_col}**",
                        all_evoliz_fields,
                        index=idx,
                        key=f"rmap_{i+j}",
                    )
                    mapping_rev[src_col] = chosen

        # Inverser : evoliz_field -> src_col
        mapping = {}
        for sc, ef in mapping_rev.items():
            if ef != "— Ignorer" and ef not in mapping:
                mapping[ef] = sc

        # --- Controle des champs obligatoires ---
        st.divider()
        st.subheader("📋 Controle des champs obligatoires")

        required_fields_ordered = list(required_set)
        ctrl_rows = []
        for ef in required_fields_ordered:
            served = ef in mapping
            # Determiner la source si auto-genere
            if served:
                statut = "✅ Mappe"
                source = mapping[ef]
            elif ef == "Code":
                statut = "🔄 Auto-genere"
                source = "Genere depuis le nom"
            elif ef == "Code pays (ISO 2)":
                statut = "🔄 Auto-genere"
                source = "FR par defaut"
            else:
                statut = "❌ Manquant"
                source = "Completable via Sirene"
            ctrl_rows.append({
                "Champ obligatoire": f"🔴 {ef}",
                "Statut": statut,
                "Source": source,
            })
        df_ctrl = pd.DataFrame(ctrl_rows)

        def _color_ctrl(row):
            s = row["Statut"]
            if "Mappe" in s: return ["background-color: #d4edda"] * len(row)
            if "Auto" in s: return ["background-color: #e8f4fd"] * len(row)
            if "Manquant" in s: return ["background-color: #f8d7da"] * len(row)
            return [""] * len(row)

        st.dataframe(df_ctrl.style.apply(_color_ctrl, axis=1), use_container_width=True, hide_index=True)

        real_missing = [ef for ef in required_fields_ordered
                        if ef not in mapping and ef not in ("Code", "Code pays (ISO 2)")]
        if real_missing:
            st.warning(f"Champs manquants : **{', '.join(real_missing)}** — completables via l'enrichissement Sirene.")
        else:
            st.success("Tous les champs obligatoires sont couverts.")


        st.session_state["meg_col_mapping_final"] = mapping

    # --- Etape 2 : Consolidation fichier (+ Evoliz si connecté) ---
    has_api = bool(st.session_state.get("token_headers_105"))
    if f_meg_cli:
        _cli_file_id = f_meg_cli.name + str(f_meg_cli.size) + ("_api" if has_api else "")
        _already_consolidated = st.session_state.get("meg_consol_file_id") == _cli_file_id
        _no_data = st.session_state.get("meg_df_clients") is None
        _auto_run = not _already_consolidated or _no_data
        _manual_run = st.button("🔄 Re-consolider" + (" avec Evoliz" if has_api else ""), use_container_width=True, key="btn_consolider")
        if _auto_run or _manual_run:
            st.session_state["meg_consol_file_id"] = _cli_file_id
            with st.spinner("Consolidation en cours..."):
                mapping = st.session_state.get("meg_col_mapping_final", {})
                headers = st.session_state.token_headers_105 if has_api else None
                cid = st.session_state.company_id_105 if has_api else None

                def _get(row, field):
                    col = mapping.get(field)
                    if not col or col == "— Ignorer" or col not in df_src.columns: return ""
                    return to_clean_str(row.get(col, ""))

                # Lire les entités Evoliz (clients ou fournisseurs) — seulement si API connectée
                ev_clients = []; ev_by_name = {}; ev_by_siren = {}
                if headers and cid:
                    url_cli = f"https://www.evoliz.io/api/v1/companies/{cid}/{_entity_api}"
                    page = 1
                    while True:
                        r = requests.get(url_cli, headers=headers, params={"per_page": 100, "page": page}, timeout=15)
                        if r.status_code != 200: break
                        d = r.json()
                        for it in d.get("data", []):
                            adr = it.get("address") or {}
                            entry = {
                                _entity_id_field: it.get(_entity_id_field), "code": (it.get("code") or "").strip(),
                                "name": (it.get("name") or "").strip(), "type": (it.get("type") or ""),
                                "vat_number": (it.get("vat_number") or ""), "business_number": (it.get("business_number") or ""),
                                "business_identification_number": (it.get("business_identification_number") or ""),
                                "legalform": (it.get("legal_status") or {}).get("label", "") if isinstance(it.get("legal_status"), dict) else "",
                                "activity_number": (it.get("activity_number") or ""),
                                "phone": (it.get("phone") or ""), "mobile": (it.get("mobile") or ""),
                                "fax": (it.get("fax") or ""), "website": (it.get("website") or ""),
                                "addr": (adr.get("addr") or ""), "postcode": (adr.get("postcode") or ""),
                                "town": (adr.get("town") or ""), "iso2": (adr.get("iso2") or ""),
                            }
                            ev_clients.append(entry)
                            n = norm_piv(entry["name"])
                            if n: ev_by_name[n] = entry
                            s = entry.get("business_identification_number", "").strip()
                            if s and s != "N/C": ev_by_siren[s] = entry
                        if page >= d.get("meta", {}).get("last_page", 1): break
                        page += 1

                # Construire la liste consolidee
                ci = {h.split(" *")[0]: i for i, h in enumerate(_H_ENTITY)}
                consol_rows = []; seen_ev_ids = set()

                for _, row in df_src.iterrows():
                    nom = _get(row, _entity_name_field)
                    prenom = _get(row, "Prenom"); nom_contact = _get(row, "Nom contact")
                    if not nom and not _is_supplier and (prenom or nom_contact): nom = f"{prenom} {nom_contact}".strip()
                    if not nom: continue
                    _raw_code = _get(row, "Code")
                    # Si le code est un UUID ou trop long, generer depuis le nom
                    if _raw_code and len(_raw_code) <= 20 and "-" not in _raw_code:
                        code = _raw_code
                    else:
                        code = norm_piv(nom)[:15]
                    type_c = _normalize_type(_get(row, "Type"), nom)
                    cp = _get(row, "Code postal"); ville = _get(row, "Ville")
                    pays = _get(row, "Pays"); iso2 = _get(row, "Code pays (ISO 2)")
                    adresse = _get(row, "Adresse")
                    if adresse and (not cp or not ville):
                        m_adr = re.search(r'^(.*?)\s+(\d{5})\s+(.+)$', adresse)
                        if m_adr:
                            adresse = m_adr.group(1).strip()
                            if not cp: cp = m_adr.group(2)
                            if not ville:
                                v = m_adr.group(3).strip().upper()
                                if "FRANCE" in v: v = v.replace("FRANCE","").strip(); iso2 = iso2 or "FR"
                                ville = v
                    if iso2: iso2 = iso2.upper()
                    elif pays: iso2 = _lookup_iso2(pays)
                    else: iso2 = "FR"

                    entry = {
                        "Code": code, _entity_name_field: nom, "Type": type_c if not _is_supplier else "",
                        "Siren": _get(row, "Siren"), "Siret": _get(row, "Siret"),
                        "TVA intracommunautaire": _get(row, "TVA intracommunautaire") or ("NC" if type_c != "Particulier" else ""),
                        "Forme juridique": _normalize_forme_juridique(_get(row, "Forme juridique")),
                        "APE / NAF": _get(row, "APE / NAF"),
                        "Adresse": adresse, "Complement d'adresse": _get(row, "Complement d'adresse"),
                        "Code postal": cp or "NC", "Ville": ville or "NC", "Code pays (ISO 2)": iso2,
                        "Telephone": _get(row, "Telephone"), "Portable": _get(row, "Portable"),
                        "Fax": _get(row, "Fax"), "Site web": _get(row, "Site web"),
                        "Commentaires": _get(row, "Commentaires"),
                    }
                    siren_val = entry["Siren"]
                    ev = ev_by_name.get(norm_piv(nom))
                    if not ev and siren_val: ev = ev_by_siren.get(siren_val)
                    if ev:
                        seen_ev_ids.add(ev[_entity_id_field])
                        entry["_source"] = "📄+☁️ Doublon"
                        entry["_entityid"] = ev[_entity_id_field]
                    else:
                        entry["_source"] = "📄 Nouveau"
                        entry["_entityid"] = None
                    consol_rows.append(entry)

                for ev in ev_clients:
                    if ev[_entity_id_field] not in seen_ev_ids:
                        consol_rows.append({
                            "Code": ev["code"], _entity_name_field: ev["name"], "Type": ev.get("type", ""),
                            "Siren": ev["business_identification_number"], "Siret": ev["business_number"],
                            "TVA intracommunautaire": ev["vat_number"], "Forme juridique": ev["legalform"],
                            "APE / NAF": ev["activity_number"],
                            "Adresse": ev["addr"], "Complement d'adresse": "",
                            "Code postal": ev["postcode"], "Ville": ev["town"], "Code pays (ISO 2)": ev["iso2"],
                            "Telephone": ev["phone"], "Portable": ev["mobile"],
                            "Fax": ev["fax"], "Site web": ev["website"], "Commentaires": "",
                            "_source": "☁️ Evoliz seul", "_entityid": ev[_entity_id_field],
                        })

                wb_c, ws_c = _make_wb(_H_ENTITY)
                for cr in consol_rows:
                    ro = [None] * len(_H_ENTITY)
                    for field, idx_c in ci.items():
                        if field in cr: ro[idx_c] = cr[field]
                    ws_c.append(ro)
                cb = _wb_bytes(wb_c)
                with open(GABARIT_CLIENT_PATH, "wb") as f: f.write(cb)
                df_preview_c = pd.read_excel(io.BytesIO(cb), header=0)

                st.session_state["meg_df_clients"] = df_preview_c
                st.session_state["meg_df_clients_original"] = df_preview_c.copy()
                st.session_state["meg_consol_sources"] = {i: cr["_source"] for i, cr in enumerate(consol_rows)}
                st.session_state["meg_consol_ev_ids"] = {i: cr["_entityid"] for i, cr in enumerate(consol_rows)}
                st.session_state["meg_consol_stats"] = {
                    "fichier": sum(1 for cr in consol_rows if "Nouveau" in cr["_source"]),
                    "doublons": sum(1 for cr in consol_rows if "Doublon" in cr["_source"]),
                    "evoliz_seul": sum(1 for cr in consol_rows if "Evoliz seul" in cr["_source"]),
                    "total_evoliz": len(ev_clients),
                }
                for _k in ["meg_sirene_cells","meg_sirene_info","meg_sirene_log","meg_sirene_stats","meg_enrichir_flags","meg_sirene_suggestions"]:
                    st.session_state[_k] = set() if "cells" in _k else ({} if "info" in _k or "flags" in _k else ([] if "log" in _k else None))
                st.rerun()
    if f_meg_cli and not has_api and st.session_state.get("meg_df_clients") is not None:
        st.info("Connectez-vous à l'API Evoliz pour détecter les doublons et injecter.")

    # --- Init session ---
    for _k, _d in [("meg_sirene_cells", set()), ("meg_sirene_log", []), ("meg_sirene_stats", None),
                    ("meg_sirene_info", {}), ("meg_editor_ver", 0), ("meg_consol_sources", {}), ("meg_consol_stats", None)]:
        if _k not in st.session_state: st.session_state[_k] = _d

    # --- Affichage consolide ---
    if "meg_df_clients" in st.session_state and st.session_state["meg_df_clients"] is not None:
        df_preview_c = st.session_state["meg_df_clients"]
        sirene_cells = st.session_state.get("meg_sirene_cells", set())
        sirene_info = st.session_state.get("meg_sirene_info", {})
        sources = st.session_state.get("meg_consol_sources", {})
        consol_stats = st.session_state.get("meg_consol_stats")
        enrichir_flags = st.session_state.get("meg_enrichir_flags", {})

        if consol_stats:
            st.divider()
            st.subheader("📊 Consolidation")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📄 Nouveaux", consol_stats.get("fichier",0))
            c2.metric("📄+☁️ Doublons", consol_stats.get("doublons",0))
            c3.metric("☁️ Evoliz seul", consol_stats.get("evoliz_seul",0))
            c4.metric("Total", len(df_preview_c))

        cols_show = ["Code *","Societe / Nom *","Type *","Siren","Adresse","Code postal *","Ville *",
                     "Code pays (ISO 2) *","Telephone","TVA intracommunautaire","Forme juridique","APE / NAF","Siret"]
        cols_show = [c for c in cols_show if c in df_preview_c.columns]
        df_show = df_preview_c[cols_show].copy()
        df_show.insert(0, "Source", df_show.index.map(lambda i: sources.get(i, "")))

        # Convertir Siren/Siret en str (Pandas les lit parfois en float)
        for _col_str in ["Siren", "Siret"]:
            if _col_str in df_show.columns:
                df_show[_col_str] = df_show[_col_str].apply(lambda v: to_clean_str(v) if not pd.isna(v) else "")

        has_enriched = sirene_cells and any(any((i, c) in sirene_cells for c in cols_show) for i in sirene_info)
        if has_enriched:
            for idx in df_show.index:
                for col in cols_show:
                    if (idx, col) in sirene_cells:
                        val = df_show.at[idx, col]
                        if val is not None and str(val).strip() and not str(val).startswith("🟢"):
                            df_show.at[idx, col] = f"🟢 {val}"
            df_show.insert(1, "✅", df_show.index.map(
                lambda i: enrichir_flags.get(i, True) if i in sirene_info and any((i, c) in sirene_cells for c in cols_show) else None))
            df_show.insert(2, "🟢 Nom trouve", df_show.index.map(lambda i: f"🟢 {v}" if (v := sirene_info.get(i, {}).get("nom", "")) else ""))
            df_show.insert(3, "🟢 APE trouve", df_show.index.map(lambda i: f"🟢 {v}" if (v := sirene_info.get(i, {}).get("activite", "")) else ""))

        st.subheader(f"👥 {len(df_preview_c)} client(s) consolides")

        # Colonnes éditables : Siren, Type
        _editable_cols = []
        if "Siren" in df_show.columns:
            _editable_cols.append("Siren")
        if "Type *" in df_show.columns:
            _editable_cols.append("Type *")
        _disabled_cols = [c for c in df_show.columns if c not in _editable_cols and c != "✅"]

        if has_enriched:
            st.caption("🟢 = enrichi Sirene. Decochez ✅ pour garder les donnees d'origine. Modifiez le Siren pour relancer la recherche.")
            _col_config = {"✅": st.column_config.CheckboxColumn("✅", default=True, width="small")}
            if "Siren" in df_show.columns:
                _col_config["Siren"] = st.column_config.TextColumn("Siren", width="medium")
            if "Type *" in df_show.columns:
                _col_config["Type *"] = st.column_config.SelectboxColumn("Type *", options=["Particulier", "Professionnel", "Administration publique"], width="medium")
            edited = st.data_editor(df_show, use_container_width=True, hide_index=True,
                disabled=_disabled_cols,
                column_config=_col_config,
                key=f"meg_enrichir_editor_{st.session_state.get('meg_editor_ver', 0)}")
            if "✅" in edited.columns:
                for i in edited.index:
                    v = edited.at[i, "✅"]
                    if v is not None: enrichir_flags[i] = bool(v)
                st.session_state["meg_enrichir_flags"] = enrichir_flags
        else:
            _col_config = {}
            if "Siren" in df_show.columns:
                _col_config["Siren"] = st.column_config.TextColumn("Siren", width="medium")
            if "Type *" in df_show.columns:
                _col_config["Type *"] = st.column_config.SelectboxColumn("Type *", options=["Particulier", "Professionnel", "Administration publique"], width="medium")
            st.caption("Modifiez le Siren pour relancer la recherche sur ce numéro.")
            edited = st.data_editor(df_show, use_container_width=True, hide_index=True,
                disabled=_disabled_cols,
                column_config=_col_config,
                key=f"meg_editor_siren_{st.session_state.get('meg_editor_ver', 0)}")

        # Persister les modifications de Type dans le dataframe source
        if "Type *" in df_show.columns and "Type *" in edited.columns:
            for i in edited.index:
                _old_type = str(df_show.at[i, "Type *"]).replace("🟢 ", "").strip() if i in df_show.index else ""
                _new_type = str(edited.at[i, "Type *"]).strip()
                if _new_type != _old_type and _new_type in ("Particulier", "Professionnel", "Administration publique"):
                    df_preview_c.at[i, "Type *"] = _new_type
                    st.session_state["meg_df_clients"] = df_preview_c

        # Détecter les SIREN modifiés et relancer la recherche
        if "Siren" in df_show.columns and "Siren" in edited.columns:
            _siren_changed = []
            for i in edited.index:
                _old = str(df_show.at[i, "Siren"]).replace("🟢 ", "").strip() if i in df_show.index else ""
                _new = str(edited.at[i, "Siren"]).strip()
                if _new != _old and _new and len(_new) >= 9 and _new.isdigit():
                    _siren_changed.append((i, _new))
            if _siren_changed:
                with st.spinner(f"Recherche Sirene pour {len(_siren_changed)} SIREN modifié(s)..."):
                    df_e = df_preview_c.copy()
                    new_sc = set(sirene_cells); new_si = dict(sirene_info)
                    for idx, siren_val in _siren_changed:
                        try:
                            r = requests.get("https://recherche-entreprises.api.gouv.fr/search",
                                             params={"q": siren_val, "per_page": 1, "page": 1}, timeout=10)
                            if r.status_code != 200: continue
                            results = r.json().get("results", [])
                            if not results: st.warning(f"SIREN {siren_val} : aucun résultat"); continue
                            ent = results[0]; siege = ent.get("siege", {})
                            siren = ent.get("siren", ""); siret = siege.get("siret", "")
                            nom_t = ent.get("nom_complet", ent.get("nom_raison_sociale", ""))
                            new_si[idx] = {"nom": nom_t, "activite": _naf_label(siege.get("activite_principale", "")),
                                           "ville": siege.get("libelle_commune", "")}
                            def _upd(col, val):
                                if col in df_e.columns and val:
                                    df_e.at[idx, col] = val; new_sc.add((idx, col))
                            _upd("Siren", siren); _upd("Siret", siret)
                            if siren and "Type *" in df_e.columns:
                                df_e.at[idx, "Type *"] = "Professionnel"; new_sc.add((idx, "Type *"))
                            _upd("APE / NAF", ent.get("activite_principale", ""))
                            _upd("Forme juridique", _normalize_forme_juridique(ent.get("nature_juridique", "")))
                            if siren and "TVA intracommunautaire" in df_e.columns:
                                tv = f"FR{(12 + 3 * (int(siren) % 97)) % 97:02d}{siren}"
                                _upd("TVA intracommunautaire", tv)
                            pts = [siege.get("numero_voie", ""), siege.get("type_voie", ""), siege.get("libelle_voie", "")]
                            _upd("Adresse", " ".join(p for p in pts if p))
                            _upd("Code postal *", siege.get("code_postal", ""))
                            _upd("Ville *", siege.get("libelle_commune", ""))
                            st.success(f"SIREN {siren_val} → **{nom_t}** ({siege.get('libelle_commune', '')})")
                        except Exception as exc:
                            st.error(f"SIREN {siren_val} : {exc}")
                    st.session_state["meg_df_clients"] = df_e
                    st.session_state["meg_sirene_cells"] = new_sc
                    st.session_state["meg_sirene_info"] = new_si
                    st.session_state["meg_editor_ver"] = st.session_state.get("meg_editor_ver", 0) + 1
                    st.rerun()

        # --- CR enrichissement ---
        if st.session_state.get("meg_sirene_stats"):
            stats = st.session_state["meg_sirene_stats"]
            with st.expander("📋 CR enrichissement Sirene", expanded=False):
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("✅ Enrichis", stats["enriched"]); c2.metric("🟰 Complets", stats["already_complete"])
                c3.metric("🔍 Non trouves", stats["not_found"]); c4.metric("⏭️ Ignores", stats["skipped"])
                lr = st.session_state.get("meg_sirene_log", [])
                if lr:
                    df_log = pd.DataFrame(lr)
                    def _cl(row):
                        s = row.get("Statut","")
                        if "Enrichi" in s: return ["background-color:#d4edda"]*len(row)
                        if "Non trouve" in s: return ["background-color:#fff3cd"]*len(row)
                        if "Erreur" in s or "HTTP" in s: return ["background-color:#f8d7da"]*len(row)
                        return [""]*len(row)
                    st.dataframe(df_log.style.apply(_cl, axis=1), use_container_width=True, hide_index=True)

        # --- Enrichissement SIRENE ---
        st.divider()
        enrich_all = st.checkbox("Inclure aussi les Particuliers", value=False, key="sirene_all")
        if st.button("🔍 Enrichir via Sirene", use_container_width=True, key="btn_sirene"):
            st.session_state["meg_enrichir_flags"] = {}
            df_e = df_preview_c.copy()
            enriched = skipped = already_complete = not_found_count = 0
            log_rows = []; new_sc = set(sirene_cells); new_si = dict(sirene_info)
            progress = st.progress(0, text="Recherche Sirene...")
            for idx in df_e.index:
                nom = str(df_e.at[idx, "Societe / Nom *"]).strip() if "Societe / Nom *" in df_e.columns else ""
                type_c = str(df_e.at[idx, "Type *"]).strip() if "Type *" in df_e.columns else ""
                code = str(df_e.at[idx, "Code *"]).strip() if "Code *" in df_e.columns else ""
                if not nom: skipped += 1; log_rows.append({"Client":code,"Nom":nom,"Statut":"⏭️ Vide","Trouve":"","Detail":""}); progress.progress((idx+1)/len(df_e)); continue
                if type_c == "Particulier" and not enrich_all: skipped += 1; log_rows.append({"Client":code,"Nom":nom,"Statut":"⏭️ Part.","Trouve":"","Detail":""}); progress.progress((idx+1)/len(df_e)); continue
                progress.progress((idx+1)/len(df_e), text=f"{idx+1}/{len(df_e)} — {nom[:40]}")
                try:
                    sq = " ".join(nom.replace("nan","").split()).strip()
                    if not sq: skipped += 1; continue
                    r = requests.get("https://recherche-entreprises.api.gouv.fr/search", params={"q":sq,"per_page":5,"page":1}, timeout=10)
                    if r.status_code == 429: time.sleep(2); r = requests.get("https://recherche-entreprises.api.gouv.fr/search", params={"q":sq,"per_page":5,"page":1}, timeout=10)
                    if r.status_code != 200: log_rows.append({"Client":code,"Nom":nom,"Statut":f"❌ HTTP {r.status_code}","Trouve":"","Detail":sq}); time.sleep(0.15); continue
                    results = r.json().get("results", [])
                    if not results: not_found_count += 1; log_rows.append({"Client":code,"Nom":nom,"Statut":"🔍 Non trouve","Trouve":"","Detail":sq}); time.sleep(0.15); continue
                    # Verifier si le 1er resultat correspond bien (nom normalise similaire)
                    best = results[0]
                    best_name = best.get("nom_complet", best.get("nom_raison_sociale",""))
                    nom_n = norm_piv(nom); best_n = norm_piv(best_name)
                    # Match si le nom normalise est contenu ou > 60% de similarite
                    auto_match = (nom_n == best_n or nom_n in best_n or best_n in nom_n
                                  or (len(nom_n) > 3 and len(best_n) > 3 and
                                      len(set(nom_n) & set(best_n)) / max(len(set(nom_n)), len(set(best_n))) > 0.6))
                    if not auto_match:
                        # Pas de match auto : stocker les suggestions
                        suggestions = []
                        for res in results[:5]:
                            rn = res.get("nom_complet", res.get("nom_raison_sociale",""))
                            rs = res.get("siren","")
                            rsie = (res.get("siege") or {})
                            rv = rsie.get("libelle_commune","")
                            ra = rsie.get("activite_principale","")
                            suggestions.append({"nom": rn, "siren": rs, "ville": rv, "activite": _naf_label(ra), "_raw": res})
                        if "meg_sirene_suggestions" not in st.session_state:
                            st.session_state["meg_sirene_suggestions"] = {}
                        st.session_state["meg_sirene_suggestions"][idx] = {"client": nom, "code": code, "search": sq, "suggestions": suggestions}
                        not_found_count += 1
                        log_rows.append({"Client":code,"Nom":nom,"Statut":"🔎 Suggestions","Trouve":f"{len(suggestions)} proposition(s)","Detail":"; ".join(s['nom'] for s in suggestions[:3])})
                        time.sleep(0.15); continue
                    ent = best; siege = ent.get("siege",{}); siren = ent.get("siren",""); siret = siege.get("siret","")
                    nom_t = ent.get("nom_complet", ent.get("nom_raison_sociale",""))
                    new_si[idx] = {"nom": nom_t or "", "activite": _naf_label(siege.get("activite_principale", ent.get("activite_principale",""))), "ville": siege.get("libelle_commune","")}
                    champs = []
                    def _sc(col, val, lbl):
                        if col in df_e.columns and val and (not to_clean_str(df_e.at[idx,col]) or to_clean_str(df_e.at[idx,col])=="NC"):
                            df_e.at[idx,col]=val; new_sc.add((idx,col)); champs.append(f"{lbl}={val}")
                    _sc("Siren",siren,"SIREN"); _sc("Siret",siret,"SIRET")
                    # Si on a un SIREN, c'est forcement un professionnel
                    if siren and "Type *" in df_e.columns and df_e.at[idx, "Type *"] != "Professionnel":
                        df_e.at[idx, "Type *"] = "Professionnel"; new_sc.add((idx, "Type *")); champs.append("Type=Professionnel")
                    _sc("APE / NAF",ent.get("activite_principale",""),"NAF")
                    _sc("Forme juridique",_normalize_forme_juridique(ent.get("nature_juridique","")),"Forme")
                    if siren and "TVA intracommunautaire" in df_e.columns:
                        cur = to_clean_str(df_e.at[idx,"TVA intracommunautaire"])
                        if not cur or cur=="NC":
                            tv = f"FR{(12+3*(int(siren)%97))%97:02d}{siren}"; df_e.at[idx,"TVA intracommunautaire"]=tv; new_sc.add((idx,"TVA intracommunautaire")); champs.append(f"TVA={tv}")
                    if not to_clean_str(df_e.at[idx,"Adresse"]) if "Adresse" in df_e.columns else True:
                        pts = [siege.get("numero_voie",""),siege.get("type_voie",""),siege.get("libelle_voie","")]
                        a = " ".join(p for p in pts if p)
                        if a and "Adresse" in df_e.columns: df_e.at[idx,"Adresse"]=a; new_sc.add((idx,"Adresse")); champs.append(f"Adr={a[:25]}")
                    _sc("Code postal *",siege.get("code_postal",""),"CP"); _sc("Ville *",siege.get("libelle_commune",""),"Ville")
                    if champs: enriched += 1; log_rows.append({"Client":code,"Nom":nom,"Statut":"✅ Enrichi","Trouve":nom_t,"Detail":", ".join(champs)})
                    else: already_complete += 1; log_rows.append({"Client":code,"Nom":nom,"Statut":"🟰 Complet","Trouve":nom_t,"Detail":""})
                    time.sleep(0.15)
                except Exception as exc: log_rows.append({"Client":code,"Nom":nom,"Statut":"❌ Erreur","Trouve":"","Detail":str(exc)[:60]})
            progress.empty()
            st.session_state["meg_df_clients"] = df_e; st.session_state["meg_sirene_cells"] = new_sc; st.session_state["meg_sirene_info"] = new_si
            st.session_state["meg_sirene_log"] = log_rows; st.session_state["meg_sirene_stats"] = {"enriched":enriched,"already_complete":already_complete,"not_found":not_found_count,"skipped":skipped}
            wb_n, ws_n = _make_wb(_H_ENTITY)
            for _, rw in df_e.iterrows(): ws_n.append([rw.iloc[i] if not pd.isna(rw.iloc[i]) else None for i in range(len(rw))])
            with open(GABARIT_CLIENT_PATH,"wb") as f: f.write(_wb_bytes(wb_n))
            st.session_state["meg_editor_ver"] = st.session_state.get("meg_editor_ver",0)+1
            st.rerun()

        # --- Suggestions Sirene (clients non trouves automatiquement) ---
        suggestions = st.session_state.get("meg_sirene_suggestions", {})
        if suggestions:
            st.divider()
            st.subheader(f"🔎 {len(suggestions)} client(s) a confirmer manuellement")
            st.caption("Pour chaque client non identifie automatiquement, selectionnez la bonne entreprise ou ignorez.")

            applied = 0
            for idx, sug_data in sorted(suggestions.items()):
                client_nom = sug_data["client"]
                client_code = sug_data["code"]
                sugs = sug_data["suggestions"]
                options = ["— Ignorer"] + [f"{s['nom']} | SIREN {s['siren']} | {s['ville']} | {s['activite']}" for s in sugs]
                chosen = st.selectbox(f"**{client_nom}** (code: {client_code})", options, key=f"sug_{idx}")

                if chosen != "— Ignorer":
                    chosen_idx = options.index(chosen) - 1
                    if st.button(f"✅ Appliquer pour {client_nom}", key=f"sug_apply_{idx}"):
                        # Appliquer l'enrichissement du choix
                        ent = sugs[chosen_idx]["_raw"]
                        siege = ent.get("siege", {})
                        siren = ent.get("siren", ""); siret = siege.get("siret", "")
                        nom_t = ent.get("nom_complet", ent.get("nom_raison_sociale", ""))

                        sirene_info[idx] = {"nom": nom_t, "activite": _naf_label(siege.get("activite_principale", ent.get("activite_principale", ""))), "ville": siege.get("libelle_commune", "")}
                        sirene_cells_tmp = set(sirene_cells)

                        def _apply(col, val):
                            if col in df_preview_c.columns and val:
                                cur = to_clean_str(df_preview_c.at[idx, col])
                                if not cur or cur == "NC":
                                    df_preview_c.at[idx, col] = val
                                    sirene_cells_tmp.add((idx, col))
                        _apply("Siren", siren); _apply("Siret", siret)
                        if siren and "Type *" in df_preview_c.columns and df_preview_c.at[idx, "Type *"] != "Professionnel":
                            df_preview_c.at[idx, "Type *"] = "Professionnel"; sirene_cells_tmp.add((idx, "Type *"))
                        _apply("APE / NAF", ent.get("activite_principale", ""))
                        _apply("Forme juridique", _normalize_forme_juridique(ent.get("nature_juridique", "")))
                        if siren and "TVA intracommunautaire" in df_preview_c.columns:
                            cur = to_clean_str(df_preview_c.at[idx, "TVA intracommunautaire"])
                            if not cur or cur == "NC":
                                tv = f"FR{(12+3*(int(siren)%97))%97:02d}{siren}"
                                df_preview_c.at[idx, "TVA intracommunautaire"] = tv
                                sirene_cells_tmp.add((idx, "TVA intracommunautaire"))
                        if not to_clean_str(df_preview_c.at[idx, "Adresse"]) if "Adresse" in df_preview_c.columns else True:
                            pts = [siege.get("numero_voie",""), siege.get("type_voie",""), siege.get("libelle_voie","")]
                            a = " ".join(p for p in pts if p)
                            if a and "Adresse" in df_preview_c.columns:
                                df_preview_c.at[idx, "Adresse"] = a; sirene_cells_tmp.add((idx, "Adresse"))
                        _apply("Code postal *", siege.get("code_postal", ""))
                        _apply("Ville *", siege.get("libelle_commune", ""))

                        st.session_state["meg_df_clients"] = df_preview_c
                        st.session_state["meg_sirene_cells"] = sirene_cells_tmp
                        st.session_state["meg_sirene_info"] = sirene_info
                        # Retirer des suggestions
                        del st.session_state["meg_sirene_suggestions"][idx]
                        # Regenerer le workbook
                        wb_n, ws_n = _make_wb(_H_ENTITY)
                        for _, rw in df_preview_c.iterrows():
                            ws_n.append([rw.iloc[i] if not pd.isna(rw.iloc[i]) else None for i in range(len(rw))])
                        with open(GABARIT_CLIENT_PATH, "wb") as f: f.write(_wb_bytes(wb_n))
                        st.session_state["meg_editor_ver"] = st.session_state.get("meg_editor_ver", 0) + 1
                        applied += 1
                        st.rerun()

        # --- Etape 4 : Injection ---
        sirene_cells_per_row = {r for (r,_) in sirene_cells}
        st.divider()
        st.subheader(f"🚀 Injection {_entity_label} dans Evoliz")
        inject_btn = st.button(f"🚀 Injecter les {_entity_label}", type="primary", use_container_width=True, key="btn_inject_clients", disabled=not has_api)
        if inject_btn and has_api:
            headers = st.session_state.token_headers_105; cid = st.session_state.company_id_105
            url_cli = f"https://www.evoliz.io/api/v1/companies/{cid}/{_entity_api}" if cid else f"https://www.evoliz.io/api/v1/{_entity_api}"
            ev_ids = st.session_state.get("meg_consol_ev_ids", {})
            df_final = df_preview_c.copy(); df_orig = st.session_state.get("meg_df_clients_original")
            for idx in df_final.index:
                if df_orig is not None and idx in sirene_cells_per_row and not enrichir_flags.get(idx, True):
                    if idx < len(df_orig): df_final.iloc[idx] = df_orig.iloc[idx]
            ci_h = {h.split(" *")[0]: i for i, h in enumerate(_H_ENTITY)}
            _name_col = "Raison sociale" if _is_supplier else "Societe / Nom"
            created=updated=up_to_date=skipped_inj=0; errors=[]; inject_log=[]
            progress = st.progress(0, text="Injection...")
            for idx, row in df_final.iterrows():
                nom = to_clean_str(row.iloc[ci_h[_name_col]]) if ci_h.get(_name_col) is not None else ""
                if not nom: skipped_inj += 1; continue
                code = to_clean_str(row.iloc[ci_h["Code"]]) if ci_h.get("Code") is not None else ""
                type_c = to_clean_str(row.iloc[ci_h["Type"]]) if ci_h.get("Type") is not None else "Professionnel"
                cp = to_clean_str(row.iloc[ci_h["Code postal"]]) if ci_h.get("Code postal") is not None else ""
                ville = to_clean_str(row.iloc[ci_h["Ville"]]) if ci_h.get("Ville") is not None else ""
                _iso2_raw = to_clean_str(row.iloc[ci_h["Code pays (ISO 2)"]]) if ci_h.get("Code pays (ISO 2)") is not None else ""
                iso2v = _iso2_raw.upper()[:2] if _iso2_raw and len(_iso2_raw) >= 2 and _iso2_raw.isalpha() else "FR"
                payload = {"name":nom,"address":{"postcode":cp if cp and cp!="NC" else "00000","town":ville if ville and ville!="NC" else "NC","iso2":iso2v}}
                if not _is_supplier:
                    payload["type"] = type_c
                if code: payload["code"] = code[:20]
                tva_v = to_clean_str(row.iloc[ci_h["TVA intracommunautaire"]]) if ci_h.get("TVA intracommunautaire") is not None else ""
                payload["vat_number"] = tva_v if tva_v and tva_v != "NC" else "N/C"
                siret_v = to_clean_str(row.iloc[ci_h["Siret"]]) if ci_h.get("Siret") is not None else ""
                payload["business_number"] = siret_v if siret_v and siret_v != "NC" else "N/C"
                for fld, ak in [("Forme juridique","legalform"),("Siren","business_identification_number"),("APE / NAF","activity_number"),("Telephone","phone"),("Portable","mobile"),("Fax","fax"),("Site web","website"),("Commentaires","comment")]:
                    v = to_clean_str(row.iloc[ci_h[fld]]) if ci_h.get(fld) is not None else ""
                    if v and v != "NC": payload[ak] = v
                adr = to_clean_str(row.iloc[ci_h["Adresse"]]) if ci_h.get("Adresse") is not None else ""
                if adr: payload["address"]["addr"] = adr
                client_id = ev_ids.get(idx)
                if client_id:
                    r = requests.patch(f"{url_cli}/{client_id}", headers=headers, json=payload, timeout=15)
                    if r.status_code in (200,204): updated += 1; inject_log.append({"Code":code,"Nom":nom,"Action":"🔄 MAJ","Statut":"✅ OK","Detail":""})
                    else: errors.append(f"MAJ '{nom}': HTTP {r.status_code}"); inject_log.append({"Code":code,"Nom":nom,"Action":"🔄 MAJ","Statut":f"❌ {r.status_code}","Detail":r.text[:80]})
                else:
                    r = requests.post(url_cli, headers=headers, json=payload, timeout=15)
                    if r.status_code in (200,201): created += 1; inject_log.append({"Code":code,"Nom":nom,"Action":"➕ Creation","Statut":"✅ OK","Detail":""})
                    elif r.status_code == 400 and "already been taken" in r.text: updated += 1; inject_log.append({"Code":code,"Nom":nom,"Action":"🔄 Existant","Statut":"✅ OK","Detail":""})
                    else: errors.append(f"Creation '{nom}': HTTP {r.status_code}"); inject_log.append({"Code":code,"Nom":nom,"Action":"➕ Creation","Statut":f"❌ {r.status_code}","Detail":r.text[:80]})
                progress.progress((idx+1)/len(df_final), text=f"{idx+1}/{len(df_final)} - {nom[:30]}"); time.sleep(0.15)
            progress.empty()
            st.divider(); st.subheader("📊 Synthese")
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("➕ Crees",created); c2.metric("🔄 MAJ",updated); c3.metric("⏭️ Ignores",skipped_inj); c4.metric("❌ Erreurs",len(errors))
            if created+updated > 0: st.success(f"Injection : {created} cree(s), {updated} mis a jour")
            elif errors: st.error(f"{len(errors)} erreur(s)")
            if inject_log:
                df_il = pd.DataFrame(inject_log)
                def _ci(row):
                    a=str(row.get("Action","")); s=str(row.get("Statut",""))
                    if "OK" in s and "Creation" in a: return ["background-color:#d4edda"]*len(row)
                    if "OK" in s: return ["background-color:#e8f4fd"]*len(row)
                    if "❌" in s: return ["background-color:#f8d7da"]*len(row)
                    return [""]*len(row)
                st.dataframe(df_il.style.apply(_ci, axis=1), use_container_width=True, hide_index=True)


# --- Onglet Injection Fournisseurs ---
with m_four:
    st.subheader("🏭 Injection Fournisseurs")
    st.caption("1. Importez un fichier fournisseurs  2. Mapping colonnes  3. Consolidation Evoliz  4. Injection")

    f_meg_four = st.session_state.get("imp_file_fournisseurs")
    if not f_meg_four:
        st.info("Importez d'abord un fichier fournisseurs dans l'onglet 📁 Import fichiers.")

    if f_meg_four:
        # Lecture fichier
        if f_meg_four.name.lower().endswith(".csv"):
            df_four = None
            for _enc in ["utf-8", "latin-1", "cp1252"]:
                for _sep in [None, ";", ","]:
                    try:
                        f_meg_four.seek(0)
                        _kw = {"header": 0, "encoding": _enc}
                        if _sep: _kw["sep"] = _sep
                        else: _kw["sep"] = None; _kw["engine"] = "python"
                        df_four = pd.read_csv(f_meg_four, **_kw)
                        if len(df_four.columns) > 1: break
                        df_four = None
                    except Exception:
                        df_four = None
                if df_four is not None: break
            if df_four is None:
                st.error("Impossible de lire ce fichier CSV.")
                f_meg_four = None
        else:
            _four_sheet = st.session_state.get("imp_file_fournisseurs_sheet", 0)
            df_four = _read_meg(f_meg_four, sheet_name=_four_sheet)

        if f_meg_four and df_four is not None:
            st.caption(f"Fichier lu : **{len(df_four)} lignes**, **{len(df_four.columns)} colonnes**")
            st.dataframe(df_four.head(5), use_container_width=True, hide_index=True)

            # --- Mapping colonnes ---
            all_four_fields = ["— Ignorer",
                "Raison sociale", "Code", "Code postal", "Ville", "Code pays (ISO 2)",
                "Forme juridique", "Siret", "APE / NAF", "TVA intracommunautaire",
                "Adresse", "Adresse (suite)", "Pays",
                "Telephone", "Portable", "Fax", "E-mail", "Site web",
                "Classification", "Code classification", "Commentaires"]

            # Auto-mapping
            _four_file_sig = f_meg_four.name + ",".join(df_four.columns.tolist())
            if st.session_state.get("_four_last_sig") != _four_file_sig:
                fwd_f = _auto_map_columns(df_four.columns.tolist())
                # Adapter les noms : "Societe / Nom" -> "Raison sociale"
                rev_f = {}
                for ef, sc in fwd_f.items():
                    mapped = ef
                    if ef == "Societe / Nom": mapped = "Raison sociale"
                    if ef == "Complement d'adresse": mapped = "Adresse (suite)"
                    if mapped in all_four_fields and sc not in rev_f:
                        rev_f[sc] = mapped
                st.session_state["_four_col_rev"] = rev_f
                st.session_state["_four_last_sig"] = _four_file_sig

            rev_map_f = st.session_state.get("_four_col_rev", {})

            st.divider()
            st.subheader("🔗 Mapping des colonnes")
            mapping_rev_f = {}
            n_cols_f = len(df_four.columns)
            for i in range(0, n_cols_f, 2):
                ui_cols_f = st.columns(2)
                for j in range(2):
                    if i + j >= n_cols_f: break
                    src_col = df_four.columns[i + j]
                    default_ev = rev_map_f.get(src_col, "— Ignorer")
                    idx_f = all_four_fields.index(default_ev) if default_ev in all_four_fields else 0
                    with ui_cols_f[j]:
                        chosen_f = st.selectbox(f"📄 **{src_col}**", all_four_fields, index=idx_f, key=f"fmap_{i+j}")
                        mapping_rev_f[src_col] = chosen_f

            mapping_f = {}
            for sc, ef in mapping_rev_f.items():
                if ef != "— Ignorer" and ef not in mapping_f:
                    mapping_f[ef] = sc

            # Controle champs obligatoires
            st.divider()
            st.subheader("📋 Controle des champs obligatoires")
            _four_required = ["Raison sociale", "Code"]
            ctrl_f = []
            for ef in _four_required:
                if ef in mapping_f:
                    ctrl_f.append({"Champ": ef, "Statut": "✅ Mappé", "Source": mapping_f[ef]})
                elif ef == "Code":
                    ctrl_f.append({"Champ": ef, "Statut": "🔄 Auto-généré", "Source": "Généré depuis le nom"})
                else:
                    ctrl_f.append({"Champ": ef, "Statut": "❌ Manquant", "Source": ""})
            st.dataframe(pd.DataFrame(ctrl_f), use_container_width=True, hide_index=True)

            # --- Consolidation + Injection ---
            has_api_f = bool(st.session_state.get("token_headers_105"))
            _four_file_id = f_meg_four.name + str(f_meg_four.size)
            _four_already = st.session_state.get("_four_consol_id") == _four_file_id
            _four_auto = not _four_already
            _four_manual = st.button("🔄 Consolider avec Evoliz", use_container_width=True, key="btn_consol_four")

            if has_api_f and (_four_auto or _four_manual):
                st.session_state["_four_consol_id"] = _four_file_id
                headers_f = st.session_state.token_headers_105
                cid_f = st.session_state.company_id_105
                if not cid_f:
                    st.error("Aucun dossier sélectionné.")
                    st.stop()

                with st.spinner("Lecture des fournisseurs Evoliz..."):
                    ev_four = []; ev_f_by_name = {}
                    url_four = f"https://www.evoliz.io/api/v1/companies/{cid_f}/suppliers"
                    page_f = 1
                    while True:
                        r_f = requests.get(url_four, headers=headers_f, params={"per_page": 100, "page": page_f}, timeout=15)
                        if r_f.status_code != 200: break
                        d_f = r_f.json()
                        for it in d_f.get("data", []):
                            adr = it.get("address") or {}
                            entry_f = {
                                "supplierid": it.get("supplierid"), "code": (it.get("code") or "").strip(),
                                "name": (it.get("name") or "").strip(),
                                "business_number": (it.get("business_number") or ""),
                                "business_identification_number": (it.get("business_identification_number") or ""),
                                "vat_number": (it.get("vat_number") or ""),
                                "legalform": (it.get("legal_status") or {}).get("label", "") if isinstance(it.get("legal_status"), dict) else "",
                                "activity_number": (it.get("activity_number") or ""),
                                "phone": (it.get("phone") or ""), "mobile": (it.get("mobile") or ""),
                                "fax": (it.get("fax") or ""), "website": (it.get("website") or ""),
                                "addr": (adr.get("addr") or ""), "postcode": (adr.get("postcode") or ""),
                                "town": (adr.get("town") or ""), "iso2": (adr.get("iso2") or ""),
                            }
                            ev_four.append(entry_f)
                            n_f = norm_piv(entry_f["name"])
                            if n_f: ev_f_by_name[n_f] = entry_f
                        if page_f >= d_f.get("meta", {}).get("last_page", 1): break
                        page_f += 1

                # Construction liste consolidée
                consol_four = []; seen_four_ids = set()
                for _, row in df_four.iterrows():
                    nom = to_clean_str(row.get(mapping_f.get("Raison sociale", ""), "")) if "Raison sociale" in mapping_f else ""
                    if not nom: continue
                    _raw_code = to_clean_str(row.get(mapping_f.get("Code", ""), "")) if "Code" in mapping_f else ""
                    code = _raw_code if _raw_code and len(_raw_code) <= 20 else norm_piv(nom)[:15]
                    entry_c = {"Code": code, "Raison sociale": nom, "_source": "📄 Nouveau", "_entityid": None}
                    for fld in ["Siret", "APE / NAF", "TVA intracommunautaire", "Forme juridique",
                                "Adresse", "Adresse (suite)", "Code postal", "Ville", "Code pays (ISO 2)",
                                "Telephone", "Portable", "Fax", "Site web", "Commentaires"]:
                        col = mapping_f.get(fld)
                        entry_c[fld] = to_clean_str(row.get(col, "")) if col and col in df_four.columns else ""
                    if not entry_c.get("Code pays (ISO 2)"): entry_c["Code pays (ISO 2)"] = "FR"
                    ev = ev_f_by_name.get(norm_piv(nom))
                    if ev:
                        seen_four_ids.add(ev["supplierid"])
                        entry_c["_source"] = "📄+☁️ Doublon"
                        entry_c["_entityid"] = ev["supplierid"]
                    consol_four.append(entry_c)

                for ev in ev_four:
                    if ev["supplierid"] not in seen_four_ids:
                        consol_four.append({
                            "Code": ev["code"], "Raison sociale": ev["name"],
                            "Siret": ev["business_number"], "TVA intracommunautaire": ev["vat_number"],
                            "Forme juridique": ev["legalform"], "APE / NAF": ev["activity_number"],
                            "Adresse": ev["addr"], "Adresse (suite)": "",
                            "Code postal": ev["postcode"], "Ville": ev["town"], "Code pays (ISO 2)": ev["iso2"],
                            "Telephone": ev["phone"], "Portable": ev["mobile"],
                            "Fax": ev["fax"], "Site web": ev["website"], "Commentaires": "",
                            "_source": "☁️ Evoliz seul", "_entityid": ev["supplierid"],
                        })

                st.session_state["_four_consol"] = consol_four
                st.session_state["_four_consol_stats"] = {
                    "fichier": sum(1 for c in consol_four if "Nouveau" in c["_source"]),
                    "doublons": sum(1 for c in consol_four if "Doublon" in c["_source"]),
                    "evoliz_seul": sum(1 for c in consol_four if "Evoliz seul" in c["_source"]),
                    "total_evoliz": len(ev_four),
                }
                st.rerun()

            # Affichage consolidation
            consol_four = st.session_state.get("_four_consol", [])
            stats_f = st.session_state.get("_four_consol_stats")
            if stats_f:
                st.divider()
                st.subheader("📊 Consolidation")
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("📄 Nouveaux", stats_f["fichier"])
                c2.metric("📄+☁️ Doublons", stats_f["doublons"])
                c3.metric("☁️ Evoliz seul", stats_f["evoliz_seul"])
                c4.metric("☁️ Total Evoliz", stats_f["total_evoliz"])

            if consol_four:
                df_four_preview = pd.DataFrame([{k: v for k, v in c.items() if not k.startswith("_")} for c in consol_four])
                _sources_f = [c["_source"] for c in consol_four]
                df_four_preview.insert(0, "Source", _sources_f)
                st.dataframe(df_four_preview, use_container_width=True, hide_index=True)

                # Injection
                st.divider()
                st.subheader("🚀 Injection fournisseurs dans Evoliz")
                inject_four_btn = st.button("🚀 Injecter les fournisseurs", type="primary", use_container_width=True, key="btn_inject_four", disabled=not has_api_f)
                if inject_four_btn and has_api_f:
                    headers_f = st.session_state.token_headers_105; cid_f = st.session_state.company_id_105
                    url_four = f"https://www.evoliz.io/api/v1/companies/{cid_f}/suppliers"
                    created_f=updated_f=skipped_f=0; errors_f=[]; inject_log_f=[]
                    progress_f = st.progress(0, text="Injection fournisseurs...")
                    for i, cr in enumerate(consol_four):
                        nom = cr.get("Raison sociale", "")
                        if not nom: skipped_f += 1; continue
                        code = cr.get("Code", "")
                        cp = cr.get("Code postal", ""); ville = cr.get("Ville", "")
                        iso2 = cr.get("Code pays (ISO 2)", "FR").upper()[:2] if cr.get("Code pays (ISO 2)") else "FR"
                        payload_f = {
                            "name": nom,
                            "address": {"postcode": cp or "00000", "town": ville or "NC", "iso2": iso2},
                        }
                        if code: payload_f["code"] = code[:20]
                        tva_v = cr.get("TVA intracommunautaire", "")
                        payload_f["vat_number"] = tva_v if tva_v and tva_v != "NC" else "N/C"
                        siret_v = cr.get("Siret", "")
                        payload_f["business_number"] = siret_v if siret_v and siret_v != "NC" else "N/C"
                        for fld, ak in [("Forme juridique","legalform"),("APE / NAF","activity_number"),
                                        ("Telephone","phone"),("Portable","mobile"),("Fax","fax"),
                                        ("Site web","website"),("Commentaires","comment")]:
                            v = cr.get(fld, "")
                            if v and v != "NC": payload_f[ak] = v
                        adr = cr.get("Adresse", "")
                        if adr: payload_f["address"]["addr"] = adr

                        entity_id = cr.get("_entityid")
                        if entity_id:
                            r_f = requests.patch(f"{url_four}/{entity_id}", headers=headers_f, json=payload_f, timeout=15)
                            if r_f.status_code in (200, 204): updated_f += 1; inject_log_f.append({"Code": code, "Nom": nom, "Action": "🔄 MAJ", "Statut": "✅ OK", "Detail": ""})
                            else: errors_f.append(f"MAJ '{nom}': HTTP {r_f.status_code}"); inject_log_f.append({"Code": code, "Nom": nom, "Action": "🔄 MAJ", "Statut": f"❌ {r_f.status_code}", "Detail": r_f.text[:80]})
                        else:
                            r_f = requests.post(url_four, headers=headers_f, json=payload_f, timeout=15)
                            if r_f.status_code in (200, 201): created_f += 1; inject_log_f.append({"Code": code, "Nom": nom, "Action": "➕ Creation", "Statut": "✅ OK", "Detail": ""})
                            elif r_f.status_code == 400 and "already been taken" in r_f.text: updated_f += 1; inject_log_f.append({"Code": code, "Nom": nom, "Action": "🔄 Existant", "Statut": "✅ OK", "Detail": ""})
                            else: errors_f.append(f"Creation '{nom}': HTTP {r_f.status_code}"); inject_log_f.append({"Code": code, "Nom": nom, "Action": "➕ Creation", "Statut": f"❌ {r_f.status_code}", "Detail": r_f.text[:80]})
                        progress_f.progress((i + 1) / len(consol_four), text=f"{i + 1}/{len(consol_four)} - {nom[:30]}")
                        time.sleep(0.15)
                    progress_f.empty()
                    st.divider(); st.subheader("📊 Synthese")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("➕ Créés", created_f); c2.metric("🔄 MAJ", updated_f); c3.metric("⏭️ Ignorés", skipped_f); c4.metric("❌ Erreurs", len(errors_f))
                    if created_f + updated_f > 0: st.success(f"Injection : {created_f} créé(s), {updated_f} mis à jour")
                    elif errors_f: st.error(f"{len(errors_f)} erreur(s)")
                    if inject_log_f:
                        df_il_f = pd.DataFrame(inject_log_f)
                        st.dataframe(df_il_f, use_container_width=True, hide_index=True)


# --- Onglet Bascule Factures ---
with m_fac:
    st.subheader("🧾 Bascule Factures, Avoirs & Paiements MEG")
    f_meg_fac = st.session_state.get("imp_file_factures")
    f_meg_cli2 = None  # utilise le gabarit genere a l'etape clients
    if not f_meg_fac:
        st.info("Importez d'abord un fichier factures dans l'onglet 📁 Import fichiers.")
    c1, c2 = st.columns(2)
    meg_tva1 = c1.number_input("TVA 1 (%)", value=20.0, step=0.5, key="meg_tva1")
    meg_tva2 = c2.number_input("TVA 2 (%)", value=0.0, step=0.5, key="meg_tva2")
    c3, c4 = st.columns(2)
    meg_pf = c3.text_input("Prefixe Facture", value="FAC", key="meg_pf")
    meg_pa = c4.text_input("Prefixe Avoir", value="AVR", key="meg_pa")
    if f_meg_fac and st.button("Generer Gabarit Facture, Avoir & Paiement", key="btn_meg_fac"):
        df_cli = None
        if f_meg_cli2: df_cli = _read_meg(f_meg_cli2)
        elif os.path.exists(GABARIT_CLIENT_PATH): df_cli = pd.read_excel(GABARIT_CLIENT_PATH, header=0)
        if df_cli is None:
            st.error("Generez d'abord le Gabarit Client ou uploadez-le.")
        else:
            with st.spinner("Traitement en cours..."):
                df_f = _read_meg(f_meg_fac)
                st.dataframe(df_f.head(10), use_container_width=True)
                cl_lk = {}
                for _, r in df_cli.iterrows():
                    cd=to_clean_str(r.iloc[0]); nm=to_clean_str(r.iloc[2]) if len(r)>2 else ""; fm=to_clean_str(r.iloc[5]) if len(r)>5 else ""
                    lk=f"{fm} {nm}".strip()
                    if lk: cl_lk[lk]=cd
                    if nm: cl_lk[nm]=cd
                def _fcc(name):
                    cn=name.strip()
                    if cn in cl_lk: return cl_lk[cn]
                    for k,v in cl_lk.items():
                        if cn in k or k in cn: return v
                    return "Client non present"
                wb_f,ws_f=_make_wb(H_FACTURE); wb_a,ws_a=_make_wb(H_AVOIR); wb_p,ws_p=_make_wb(H_PAIEMENT)
                has_av=has_pa=tva_err=False
                fi={h.split(" *")[0]:i for i,h in enumerate(H_FACTURE)}
                ai={h.split(" *")[0]:i for i,h in enumerate(H_AVOIR)}
                pi={h.split(" *")[0]:i for i,h in enumerate(H_PAIEMENT)}
                ta=[meg_tva1,meg_tva2]
                def _gr(rate):
                    for t in ta:
                        if abs(rate-t)<=0.1: return t
                    return rate
                def _af(r,ht,tv=0):
                    o=[None]*len(H_FACTURE); o[fi["N facture externe"]]=to_clean_str(r.iloc[1]); o[fi["Date facture"]]=_parse_date(r.iloc[2])
                    cn=to_clean_str(r.iloc[3]); o[fi["Client"]]=cn; o[fi["Code client"]]=_fcc(cn)
                    o[fi["Commentaires"]]=to_clean_str(r.iloc[4]); o[fi["Conditions de reglement"]]="A reception"
                    o[fi["Designation"]]="NC"; o[fi["Qte"]]=1; o[fi["PU HT"]]=round(float(ht),2); o[fi["TVA"]]=tv
                    ws_f.append(o); return ws_f.max_row
                def _aa(r,ht,tv=0):
                    has_av=True
                    o=[None]*len(H_AVOIR); o[ai["N avoir externe"]]=to_clean_str(r.iloc[1]); o[ai["Date avoir"]]=_parse_date(r.iloc[2])
                    cn=to_clean_str(r.iloc[3]); o[ai["Client"]]=cn; o[ai["Code client"]]=_fcc(cn)
                    o[ai["Commentaires"]]=to_clean_str(r.iloc[4]); o[ai["Conditions de reglement"]]="A reception"
                    o[ai["Designation"]]="NC"; o[ai["Qte"]]=1; o[ai["PU HT"]]=round(float(ht),2); o[ai["TVA"]]=tv
                    ws_a.append(o); return ws_a.max_row
                def _ap(r,paid):
                    has_pa=True
                    o=[None]*len(H_PAIEMENT); o[pi["Facture n"]]=to_clean_str(r.iloc[1]); o[pi["Date paiement"]]=_parse_date(r.iloc[2])
                    o[pi["Libelle"]]="Reglement client"; o[pi["Mode de paiement"]]="Autres"; o[pi["Montant"]]=round(float(paid),2)
                    ws_p.append(o)
                for _,row in df_f.iterrows():
                    ht=_safe_float(row.iloc[5]); ttc=_safe_float(row.iloc[6]); paid=_safe_float(row.iloc[7])
                    doc=to_clean_str(row.iloc[1]); stat=to_clean_str(row.iloc[0])
                    if ht==0: continue
                    is_f=doc.startswith(meg_pf); is_a=doc.startswith(meg_pa)
                    if ttc-ht==0:
                        if is_f: _af(row,ht)
                        if is_a: _aa(row,ht)
                    else:
                        tva_am=ttc-ht; tva_r=round(tva_am/ht*100,2); tva_r=_gr(tva_r)
                        if tva_r==meg_tva1 or tva_r==meg_tva2:
                            if is_f: _af(row,ht,tva_r)
                            if is_a: _aa(row,ht,tva_r)
                        else:
                            if meg_tva2/100-meg_tva1/100!=0: ht2=round((ttc-ht*(1+meg_tva1/100))/(meg_tva2/100-meg_tva1/100),2)
                            else: ht2=0
                            ht1=ht-ht2; tc=round(ht1*meg_tva1/100+ht2*meg_tva2/100,2)
                            for hv,tv in [(ht1,meg_tva1),(ht2,meg_tva2)]:
                                if is_f:
                                    lr=_af(row,hv,tv)
                                    if tva_am!=tc: ws_f.cell(row=lr,column=fi["TVA"]+1).value="TVA a verifier"; tva_err=True
                                if is_a:
                                    lr=_aa(row,hv,tv)
                                    if tva_am!=tc: ws_a.cell(row=lr,column=ai["TVA"]+1).value="TVA a verifier"; tva_err=True
                    ttc9=_safe_float(row.iloc[8]) if len(row)>8 else ttc
                    if is_f and stat!="Annulee" and ttc9!=ttc and paid!=0: _ap(row,paid)
                if tva_err: st.warning("Des erreurs de TVA detectees. Verifiez la colonne TVA.")
                zb = io.BytesIO()
                with zipfile.ZipFile(zb,"w",zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr("3. Gabarit Facture.xlsx",_wb_bytes(wb_f))
                    if has_pa: zf.writestr("4. Gabarit Paiement.xlsx",_wb_bytes(wb_p))
                    if has_av: zf.writestr("5. Gabarit Avoir.xlsx",_wb_bytes(wb_a))
                zb.seek(0)
                st.success("Gabarits generes")
                st.download_button("Telecharger le ZIP",data=zb.getvalue(),file_name="Gabarit_Facture_Paiement_Avoir.zip",mime="application/zip",key="dl_meg_fac")

# --- Onglet Bascule Articles ---
with m_art:
    st.subheader("📦 Bascule Articles MEG")
    art_mode = st.radio("Mode", ["Gabarit Excel","Envoi direct API Evoliz"], horizontal=True, key="art_mode")
    f_meg_art = st.session_state.get("imp_file_articles")
    if not f_meg_art:
        st.info("Importez d'abord un fichier articles dans l'onglet 📁 Import fichiers.")
    if art_mode == "Gabarit Excel":
        if f_meg_art and st.button("Generer Gabarit Article", key="btn_meg_art"):
            with st.spinner("Traitement..."):
                df=_read_meg(f_meg_art); st.dataframe(df.head(10),use_container_width=True)
                wb_ar,ws_ar=_make_wb(H_ARTICLE); ari={h.split(" *")[0]:i for i,h in enumerate(H_ARTICLE)}
                for _,row in df.iterrows():
                    r=[None]*len(H_ARTICLE); r[ari["Reference"]]=to_clean_str(row.iloc[0]); r[ari["Designation"]]=to_clean_str(row.iloc[1])
                    r[ari["Code Classification vente"]]=to_clean_str(row.iloc[4]) if len(row)>4 else ""
                    r[ari["PU HT"]]=row.iloc[6] if len(row)>6 else ""; r[ari["TVA"]]=row.iloc[7] if len(row)>7 else ""
                    ws_ar.append(r)
                zb=io.BytesIO()
                with zipfile.ZipFile(zb,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("Gabarit Article.xlsx",_wb_bytes(wb_ar))
                zb.seek(0); st.success("Gabarit Article genere")
                st.download_button("Telecharger le ZIP",data=zb.getvalue(),file_name="Gabarit_Article.zip",mime="application/zip",key="dl_meg_art")
    else:
        has_h = bool(st.session_state.token_headers_105)
        if not has_h: st.warning("Connectez-vous d'abord a l'API Evoliz (onglet Balance & Cles API)")
        if f_meg_art and has_h and st.button("Envoyer les articles vers Evoliz",type="primary",key="btn_meg_art_api"):
            headers = st.session_state.token_headers_105; cid = st.session_state.company_id_105
            if not cid: st.error("companyid non disponible. Reconnectez-vous.")
            else:
                df=_read_meg(f_meg_art); st.dataframe(df.head(10),use_container_width=True)
                with st.spinner("Lecture articles et classifications existants..."):
                    url_art=f"https://www.evoliz.io/api/v1/companies/{cid}/articles"
                    existing={}; page=1
                    while True:
                        r=requests.get(url_art,headers=headers,params={"per_page":100,"page":page},timeout=15)
                        if r.status_code!=200: break
                        d=r.json()
                        for it in d.get("data",[]):
                            ref=(it.get("reference_clean") or it.get("reference") or "").strip().upper()
                            if ref: existing[ref]=it.get("articleid")
                        if page>=d.get("meta",{}).get("last_page",1): break
                        page+=1
                    sale_cl={}; url_sc=f"https://www.evoliz.io/api/v1/companies/{cid}/sale-classifications"; page=1
                    while True:
                        r=requests.get(url_sc,headers=headers,params={"per_page":100,"page":page},timeout=15)
                        if r.status_code!=200: break
                        d=r.json()
                        for it in d.get("data",[]):
                            c=str(it.get("code","")).strip().upper(); sid=it.get("classificationid") or it.get("id")
                            if c and sid: sale_cl[c]=sid
                        if page>=d.get("meta",{}).get("last_page",1): break
                        page+=1
                    st.info(f"{len(existing)} articles existants, {len(sale_cl)} classifications de vente")
                articles=[]
                for _,row in df.iterrows():
                    ref=to_clean_str(row.iloc[0]); des=to_clean_str(row.iloc[1])
                    if not ref and not des: continue
                    art={"reference":ref or "NOREF","designation":des or ref}
                    if len(row)>6:
                        pu=_safe_float(row.iloc[6])
                        if pu: art["unit_price"]=round(pu,2)
                    if len(row)>7:
                        tv=_safe_float(row.iloc[7])
                        if tv: art["vat_rate"]=round(tv,2)
                    if len(row)>4:
                        cc=to_clean_str(row.iloc[4]).upper()
                        if cc and cc in sale_cl: art["sale_classificationid"]=sale_cl[cc]
                    articles.append(art)
                if not articles: st.warning("Aucun article a envoyer.")
                else:
                    created=updated=0; errors=[]
                    prog=st.progress(0,text="Envoi des articles...")
                    for i,art in enumerate(articles):
                        ru=art["reference"].upper()
                        if ru in existing:
                            r=requests.patch(f"{url_art}/{existing[ru]}",headers=headers,json=art,timeout=15)
                            if r.status_code in (200,204): updated+=1
                            else: errors.append(f"MAJ '{art['reference']}': HTTP {r.status_code}")
                        else:
                            r=requests.post(url_art,headers=headers,json=art,timeout=15)
                            if r.status_code in (200,201): created+=1
                            elif r.status_code==400 and "already been taken" in r.text: updated+=1
                            else: errors.append(f"Creation '{art['reference']}': HTTP {r.status_code}")
                        prog.progress((i+1)/len(articles),text=f"Article {i+1}/{len(articles)}")
                    prog.empty()
                    parts=[]
                    if created: parts.append(f"{created} cree(s)")
                    if updated: parts.append(f"{updated} mis a jour")
                    if parts: st.success(f"Articles Evoliz : {', '.join(parts)}")
                    if errors:
                        with st.expander(f"{len(errors)} erreur(s)",expanded=True):
                            for e in errors: st.error(e)