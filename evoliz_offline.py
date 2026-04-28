"""
Mode hors-ligne : generation des gabarits d'import Evoliz au format xlsx.

Lit les modeles officiels depuis ./templates/, recopie leurs en-tetes a l'identique
(y compris les indications de format type "(Texte)" / "(00,00 en EUR)") et produit
un fichier xlsx pret a importer dans Evoliz, sans appel API.

Modules couverts :
    - matrice comptable (5 gabarits)
    - clients
    - fournisseurs
    - articles
    - factures de vente
    - factures d'achat (depenses)
"""

import io
import os
import re
import unicodedata
import zipfile
from datetime import datetime as dt_datetime

import pandas as pd
import xlrd
from openpyxl import Workbook


def _norm(s):
    """Normalisation pour matcher les en-tetes : sans accent, sans symboles, lowercase, espaces compactes."""
    if s is None:
        return ""
    s = str(s).split("\n", 1)[0].strip()
    if s.endswith(" *"):
        s = s[:-2].strip()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = s.replace("°", "").replace("º", "").replace("'", " ").replace("´", " ")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

APP_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(APP_DIR, "templates")
SHEET_NAME = "Evoliz.com"

TEMPLATE_FILES = {
    "comptes":          "modele_import_evoliz_Compte_comptable.xls",
    "classif_achats":   "modele_import_evoliz_Classification_d'achats.xls",
    "classif_ventes":   "modele_import_evoliz_Classification_de_ventes(2).xls",
    "affect_entree":    "modele_import_evoliz_Affectation_diverse_entree.xls",
    "affect_sortie":    "modele_import_evoliz_Affectation_diverse_sortie.xls",
    "clients":          "modele_import_evoliz_Client(4).xls",
    "fournisseurs":     "modele_import_evoliz_Fournisseur(3).xls",
    "articles":         "modele_import_evoliz_Article(6).xls",
    "factures_vente":   "modele_import_evoliz_Facture(1).xls",
    "factures_achat":   "modele_import_evoliz_Depense.xls",
}

# Friendly labels for UI
TEMPLATE_LABELS = {
    "comptes":          "Comptes comptables",
    "classif_achats":   "Classifications d'achats",
    "classif_ventes":   "Classifications de ventes",
    "affect_entree":    "Affectations entree banque",
    "affect_sortie":    "Affectations sortie banque",
    "clients":          "Clients",
    "fournisseurs":     "Fournisseurs",
    "articles":         "Articles",
    "factures_vente":   "Factures de vente",
    "factures_achat":   "Factures d'achat",
}


# --------------------------------------------------------------------------- #
#  Lecture des en-tetes du template                                            #
# --------------------------------------------------------------------------- #
def _template_path(key):
    return os.path.join(TEMPLATES_DIR, TEMPLATE_FILES[key])


_HEADERS_CACHE = {}

def load_template_headers(key):
    """Retourne la liste des en-tetes (tels quels, avec '\\n(Texte)' & co) du template."""
    if key in _HEADERS_CACHE:
        return list(_HEADERS_CACHE[key])
    path = _template_path(key)
    wb = xlrd.open_workbook(path, ignore_workbook_corruption=True)
    sheet = wb.sheet_by_index(0)
    headers = [sheet.cell_value(0, c) for c in range(sheet.ncols)]
    _HEADERS_CACHE[key] = list(headers)
    return list(headers)


def _header_index(headers):
    """Mappe la forme normalisee (accent-insensible, lowercase) de l'en-tete -> indice."""
    return {_norm(h): i for i, h in enumerate(headers)}


# --------------------------------------------------------------------------- #
#  Construction du xlsx                                                        #
# --------------------------------------------------------------------------- #
def make_xlsx(key, rows, sheet_name=SHEET_NAME):
    """Construit un xlsx avec les en-tetes du template + les lignes fournies.

    rows : liste de listes (alignees sur l'ordre des en-tetes) OU liste de dicts
           (cles = nom court de l'en-tete, sans '\\n' ni ' *').
    Retourne : bytes xlsx.
    """
    headers = load_template_headers(key)
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name
    ws.append(headers)

    if rows and isinstance(rows[0], dict):
        idx = _header_index(headers)
        for r in rows:
            line = [None] * len(headers)
            for k, v in r.items():
                col = idx.get(_norm(k))
                if col is not None:
                    line[col] = v
            ws.append(line)
    else:
        for r in rows or []:
            ws.append(list(r) + [None] * (len(headers) - len(r)))

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


def _to_str(v):
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        return s[:-2]
    return s


def _to_num(v):
    try:
        f = float(str(v).replace(",", ".").replace(" ", "").replace(" ", ""))
        return f
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
#  Mappers : data canonique de l'app -> lignes prêtes pour les gabarits        #
# --------------------------------------------------------------------------- #
def map_clients(df_preview_c, sources_map=None):
    """df_preview_c : DataFrame canonique des clients (cf. onglet Injection Clients).
    Colonnes attendues (si presentes) : Code, Societe / Nom, Type, Civilite,
    Forme juridique, Siren, APE / NAF, TVA intracommunautaire, Adresse,
    Complement d'adresse, Complement d'adresse (suite), Code postal, Ville,
    Code pays (ISO 2), Siret, Telephone, Portable, Fax, Site web, Commentaires.
    Les colonnes peuvent finir par ' *' (champs obligatoires) -> on tolere les deux.
    """
    rows = []
    if df_preview_c is None or len(df_preview_c) == 0:
        return rows

    def col(row, *names):
        for n in names:
            for cand in (n, f"{n} *"):
                if cand in row.index:
                    v = row[cand]
                    if pd.notna(v) and _to_str(v) not in ("", "NC"):
                        return _to_str(v)
        return ""

    for _, row in df_preview_c.iterrows():
        nom = col(row, "Societe / Nom", "Raison sociale", "Nom")
        if not nom:
            continue
        rows.append({
            "Code":                       col(row, "Code"),
            "Societe / Nom":              nom,
            "Type":                       col(row, "Type") or "Professionnel",
            "Civilite":                   col(row, "Civilite"),
            "Forme juridique":            col(row, "Forme juridique"),
            "SIREN":                      col(row, "Siren", "SIREN"),
            "APE / NAF":                  col(row, "APE / NAF"),
            "TVA intracommunautaire":     col(row, "TVA intracommunautaire"),
            "Adresse":                    col(row, "Adresse"),
            "Complement d'adresse":       col(row, "Complement d'adresse", "Complément d'adresse"),
            "Complement d'adresse (suite)": col(row, "Complement d'adresse (suite)", "Complément d'adresse (suite)"),
            "Code postal":                col(row, "Code postal"),
            "Ville":                      col(row, "Ville"),
            "Code pays (ISO 2)":          col(row, "Code pays (ISO 2)") or "FR",
            "SIRET":                      col(row, "Siret", "SIRET"),
            "Telephone":                  col(row, "Telephone", "Téléphone"),
            "Portable":                   col(row, "Portable"),
            "Autre (ex : Fax)":           col(row, "Fax"),
            "Site web":                   col(row, "Site web"),
            "Commentaires":               col(row, "Commentaires"),
        })
    return rows


def map_fournisseurs(consol_four):
    """consol_four : list[dict] (cf. onglet Fournisseurs, _four_consol)."""
    rows = []
    for c in consol_four or []:
        nom = _to_str(c.get("Raison sociale", "") or c.get("Societe / Nom", ""))
        if not nom:
            continue
        rows.append({
            "Code":                       _to_str(c.get("Code", "")),
            "Raison sociale":             nom,
            "Forme juridique":            _to_str(c.get("Forme juridique", "")),
            "Siret":                      _to_str(c.get("Siret", "")),
            "APE / NAF":                  _to_str(c.get("APE / NAF", "")),
            "TVA intracommunautaire":     _to_str(c.get("TVA intracommunautaire", "")),
            "Adresse":                    _to_str(c.get("Adresse", "")),
            "Adresse (suite)":            _to_str(c.get("Complement d'adresse", "") or c.get("Adresse (suite)", "")),
            "Code postal":                _to_str(c.get("Code postal", "")),
            "Ville":                      _to_str(c.get("Ville", "")),
            "Code pays (ISO 2)":          (_to_str(c.get("Code pays (ISO 2)", "")) or "FR")[:2].upper(),
            "Telephone":                  _to_str(c.get("Telephone", "")),
            "Portable":                   _to_str(c.get("Portable", "")),
            "Autre (ex : Fax)":           _to_str(c.get("Fax", "")),
            "Site web":                   _to_str(c.get("Site web", "")),
            "Commentaires":               _to_str(c.get("Commentaires", "")),
        })
    return rows


def map_articles(consol_art):
    """consol_art : list[dict] (cf. onglet Articles, _art_consol)."""
    rows = []
    for a in consol_art or []:
        ref = _to_str(a.get("Reference", ""))
        des = _to_str(a.get("Designation", "") or a.get("Description", ""))
        if not ref or not des:
            continue
        cl_v = _to_str(a.get("Classification vente", "")).lstrip("⏳ ").strip()
        rows.append({
            "Reference":                  ref,
            "Nature":                     _to_str(a.get("Nature", "")) or "Produit",
            "Code Classification vente":  cl_v,
            "Designation":                des,
            "Quantite":                   _to_num(a.get("Quantite", "")) or _to_num(a.get("Qte", "")),
            "Poids par unite":            _to_num(a.get("Poids", "")),
            "Unite":                      _to_str(a.get("Unite", "")),
            "PU HT":                      _to_num(a.get("PU HT", "")),
            "PU TTC":                     _to_num(a.get("PU TTC", "")),
            "TVA":                        _to_num(a.get("TVA %", "") or a.get("TVA", "")),
            "Prix d'achat HT":            _to_num(a.get("Prix d'achat", "")),
            "Code Classification achat":  _to_str(a.get("Classification achat", "")),
            "Code fournisseur":           _to_str(a.get("Code fournisseur", "")),
            "Ref. Fournisseur":           _to_str(a.get("Ref. fournisseur", "")),
            "Article stocke":             "Oui" if str(a.get("Gestion stock", "")).strip().lower() in ("true", "1", "oui", "yes", "vrai") else "",
            "Qte stockee":                _to_num(a.get("Qte stockee", "")),
        })
    return rows


# --- COMPTABILITE : a partir de la matrice (DataFrame audit_matrix_105) ----- #
def map_comptes(audit_matrix):
    """Tous les comptes presents dans la matrice -> Compte_comptable.
    Dedup par code complet uniquement. Si deux comptes (codes differents) ont
    le m^me libelle, on suffixe le second par '(code)' (Evoliz refuse deux
    comptes au libelle identique)."""
    rows = []
    if audit_matrix is None or len(audit_matrix) == 0:
        return rows
    seen_codes = set()
    seen_labels = {}        # label_norm -> code de la 1ere occurrence
    for _, r in audit_matrix.iterrows():
        code = _to_str(r.get("COMPTE_CODE", "") or r.get("Code", "") or r.get("COMPTE", ""))
        if not code:
            for k in ("Code compte", "code", "Compte"):
                if k in r.index and _to_str(r[k]):
                    code = _to_str(r[k]); break
        label = _to_str(r.get("LIBELLE", "") or r.get("Libelle", "") or r.get("LABEL", "") or r.get("label", ""))
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        final_label = label or code
        ln = _norm(final_label)
        if ln and ln in seen_labels:
            final_label = f"{final_label} ({code})"
            ln = _norm(final_label)
        seen_labels[ln] = code
        rows.append({"Code comptable": code, "Libelle": final_label})
    return rows


def map_classif_achats(audit_matrix, default_tva_rate=20.0):
    """Lignes ACHAT de la matrice -> Classification d'achats.
    Si la colonne 'TVA' est presente (libelle "44566... - Description"), le code 4456
    est extrait et place dans 'Code Compte TVA associe'.
    Dedup par Code de classification ET par Code Compte comptable."""
    rows = []
    if audit_matrix is None or len(audit_matrix) == 0:
        return rows
    seen_codes = set()
    seen_comptes = set()
    for _, r in audit_matrix.iterrows():
        if not _truthy(r, "ACHAT"):
            continue
        code = _to_str(r.get("COMPTE_CODE", "") or r.get("Code", "") or r.get("COMPTE", ""))
        label = _to_str(r.get("LIBELLE", "") or r.get("Libelle", "") or r.get("LABEL", ""))
        if not code or code in seen_codes or code in seen_comptes:
            continue
        seen_codes.add(code); seen_comptes.add(code)
        tva_label = _to_str(r.get("TVA", ""))
        tva_code = ""
        if tva_label and tva_label not in ("—", "-", "NC"):
            tva_code = tva_label.split(" - ")[0].strip() if " - " in tva_label else tva_label
        out = {
            "Code": code,
            "Libelle": label or code,
            "Code Compte comptable": code,
            "Taux de TVA": default_tva_rate,
        }
        if tva_code:
            out["Code Compte TVA associe"] = tva_code
        rows.append(out)
    return rows


def map_classif_ventes(audit_matrix):
    return _map_simple_classif(audit_matrix, "VENTE")


def map_affect_entree(audit_matrix):
    return _map_simple_classif(audit_matrix, "ENTRÉE BQ")


def map_affect_sortie(audit_matrix):
    return _map_simple_classif(audit_matrix, "SORTIE BQ")


def _map_simple_classif(audit_matrix, flux_col):
    """Dedup par Code de classification ET par Code Compte comptable."""
    rows = []
    if audit_matrix is None or len(audit_matrix) == 0:
        return rows
    seen_codes = set()
    seen_comptes = set()
    for _, r in audit_matrix.iterrows():
        if not _truthy(r, flux_col):
            continue
        code = _to_str(r.get("COMPTE_CODE", "") or r.get("Code", "") or r.get("COMPTE", ""))
        label = _to_str(r.get("LIBELLE", "") or r.get("Libelle", "") or r.get("LABEL", ""))
        if not code or code in seen_codes or code in seen_comptes:
            continue
        seen_codes.add(code); seen_comptes.add(code)
        rows.append({
            "Code": code,
            "Libelle": label or code,
            "Code Compte comptable": code,
        })
    return rows


# Alias retro-compat
_map_affect = _map_simple_classif


def _truthy(row, col):
    if col not in row.index:
        return False
    v = row[col]
    if pd.isna(v):
        return False
    s = str(v).strip().lower()
    return s in ("true", "1", "oui", "yes", "x", "✅", "➕", "🔄") or "à créer" in s or "a creer" in s


# --- FACTURES (vente / achat) ---------------------------------------------- #
def _fmt_date(v):
    if not v or (isinstance(v, float) and pd.isna(v)):
        return ""
    if isinstance(v, dt_datetime):
        return v.strftime("%d/%m/%Y")
    s = str(v).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return dt_datetime.strptime(s, fmt).strftime("%d/%m/%Y")
        except ValueError:
            continue
    return s


def map_factures_vente(df_f, code_client_lookup=None):
    """df_f : DataFrame factures (1 ligne = 1 facture). Cherche Numero, Date, Client,
    Montant HT/TTC/TVA, Code client, Designation, Quantite, PU HT, TVA %.
    code_client_lookup : callable optionnel(nom_client) -> code Evoliz."""
    rows = []
    if df_f is None or len(df_f) == 0:
        return rows
    for _, r in df_f.iterrows():
        n_ext = _first(r, "N facture externe", "Numero", "Numero facture", "N facture", "Reference")
        date  = _fmt_date(_first(r, "Date facture", "Date"))
        nom_c = _first(r, "Client", "Nom client", "Societe")
        code_c = _first(r, "Code client", "Code")
        if not code_c and code_client_lookup and nom_c:
            code_c = code_client_lookup(nom_c) or ""
        des   = _first(r, "Designation", "Libelle", "Description") or "NC"
        qte   = _to_num(_first(r, "Qte", "Quantite")) or 1
        pu    = _to_num(_first(r, "PU HT", "Prix unitaire HT")) or _to_num(_first(r, "Total HT", "HT"))
        tva   = _to_num(_first(r, "TVA", "Taux TVA", "TVA %")) or 0
        if not n_ext or pu is None:
            continue
        rows.append({
            "N facture externe": _to_str(n_ext),
            "Date facture": date,
            "Code client": _to_str(code_c),
            "Conditions de reglement": "A reception",
            "Designation": _to_str(des),
            "Qte": qte,
            "PU HT": pu,
            "TVA": tva,
        })
    return rows


def map_factures_achat(df_f, code_fourn_lookup=None):
    """df_f : DataFrame depenses. Cherche Numero, Date, Fournisseur, Code fourn,
    Libelle, Total TTC, Total HT."""
    rows = []
    if df_f is None or len(df_f) == 0:
        return rows
    for _, r in df_f.iterrows():
        ref   = _first(r, "Reference d'origine de la piece", "Reference d'origine", "N facture", "Numero", "Reference")
        date  = _fmt_date(_first(r, "Date depense", "Date facture", "Date"))
        nom_f = _first(r, "Fournisseur", "Nom fournisseur", "Societe")
        code_f = _first(r, "Code Fournisseur", "Code fournisseur", "Code")
        if not code_f and code_fourn_lookup and nom_f:
            code_f = code_fourn_lookup(nom_f) or ""
        lib   = _first(r, "Libelle", "Designation", "Description") or "NC"
        ttc   = _to_num(_first(r, "Total TTC", "TTC", "Montant TTC"))
        ht    = _to_num(_first(r, "Total HT", "HT", "Montant HT"))
        if not ref or (ttc is None and ht is None):
            continue
        rows.append({
            "Reference d'origine de la piece": _to_str(ref),
            "Date depense": date,
            "Code Fournisseur": _to_str(code_f),
            "Libelle": _to_str(lib),
            "Total TTC": ttc if ttc is not None else "",
            "Total HT":  ht  if ht  is not None else "",
        })
    return rows


def _first(row, *names):
    for n in names:
        for cand in (n, f"{n} *"):
            if cand in row.index:
                v = row[cand]
                if pd.notna(v) and _to_str(v) not in ("", "NC"):
                    return v
    return ""


# --------------------------------------------------------------------------- #
#  Helpers haut niveau pour Streamlit                                          #
# --------------------------------------------------------------------------- #
def make_zip(items):
    """items : list[(filename, bytes)] -> bytes (zip)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in items:
            zf.writestr(name, data)
    buf.seek(0)
    return buf.getvalue()


def build_compta_zip(audit_matrix, default_tva_rate=20.0):
    """Construit le zip des 5 gabarits comptables a partir de la matrice."""
    items = [
        ("1_Comptes_comptables.xlsx",        make_xlsx("comptes",         map_comptes(audit_matrix))),
        ("2_Classifications_achats.xlsx",    make_xlsx("classif_achats",  map_classif_achats(audit_matrix, default_tva_rate))),
        ("3_Classifications_ventes.xlsx",    make_xlsx("classif_ventes",  map_classif_ventes(audit_matrix))),
        ("4_Affectations_entree_banque.xlsx", make_xlsx("affect_entree",  map_affect_entree(audit_matrix))),
        ("5_Affectations_sortie_banque.xlsx", make_xlsx("affect_sortie",  map_affect_sortie(audit_matrix))),
    ]
    return make_zip(items)


# --------------------------------------------------------------------------- #
#  Rendu Streamlit pour le mode hors-ligne (un module = une fonction)          #
# --------------------------------------------------------------------------- #
def _read_any(f, sheet_name=0):
    """Lit un fichier Excel/CSV avec fallback multi-engines."""
    if f is None:
        return None
    if hasattr(f, 'name') and f.name.lower().endswith('.csv'):
        try:
            f.seek(0)
            return pd.read_csv(f, sep=None, engine='python')
        except Exception:
            f.seek(0)
            return pd.read_csv(f)
    for engine, kwargs in [(None, {}), ("openpyxl", {}), ("xlrd", {})]:
        try:
            f.seek(0)
            return pd.read_excel(f, header=0, engine=engine, sheet_name=sheet_name, **kwargs)
        except Exception:
            continue
    return None


def _auto_map(src_cols, target_keywords):
    """Mapping auto basique : pour chaque champ cible, trouve la 1ere col source qui contient un keyword."""
    out = {}
    src_norm = {c: _norm(c) for c in src_cols}
    for tgt, kws in target_keywords.items():
        for c, n in src_norm.items():
            if any(k in n for k in kws):
                out[tgt] = c; break
    return out


# Mots-cles de detection par champ Evoliz (par module)
_KW_CLIENTS = {
    "Code":                   ["code", "ref client", "id client", "numero client"],
    "Societe / Nom":          ["societe", "raison sociale", "nom", "client"],
    "Type":                   ["type"],
    "Civilite":               ["civilite", "civ"],
    "Forme juridique":        ["forme juridique"],
    "Siren":                  ["siren"],
    "APE / NAF":              ["ape", "naf"],
    "TVA intracommunautaire": ["tva intra", "vat", "n tva"],
    "Adresse":                ["adresse"],
    "Code postal":            ["code postal", " cp", "zip", "postal"],
    "Ville":                  ["ville", "commune", "city"],
    "Code pays (ISO 2)":      ["iso", "code pays"],
    "Siret":                  ["siret"],
    "Telephone":              ["telephone", "tel"],
    "Portable":               ["portable", "mobile"],
    "Site web":               ["site", "web", "url"],
    "Commentaires":           ["commentaire", "note"],
}
_KW_FOURN = {
    "Code":                   ["code"],
    "Raison sociale":         ["raison sociale", "societe", "nom"],
    "Forme juridique":        ["forme juridique"],
    "Siret":                  ["siret"],
    "APE / NAF":              ["ape", "naf"],
    "TVA intracommunautaire": ["tva", "vat"],
    "Adresse":                ["adresse"],
    "Code postal":            ["code postal", " cp", "postal"],
    "Ville":                  ["ville"],
    "Code pays (ISO 2)":      ["iso", "code pays"],
    "Telephone":              ["telephone", "tel"],
    "Portable":               ["portable", "mobile"],
    "Site web":               ["site", "web"],
    "Commentaires":           ["commentaire"],
}
_KW_ARTICLES = {
    "Reference":            ["reference", "ref", "code article", "code"],
    "Designation":          ["designation", "libelle", "description", "nom"],
    "Nature":               ["nature"],
    "Quantite":             ["qte", "quantite"],
    "Unite":                ["unite"],
    "PU HT":                ["pu ht", "prix ht", "prix unitaire ht"],
    "PU TTC":               ["pu ttc", "prix ttc"],
    "TVA %":                ["tva"],
    "Prix d'achat":         ["prix d'achat", "pa ht", "prix achat"],
    "Classification vente": ["classif vente", "classification vente", "code classif"],
    "Code fournisseur":     ["code fournisseur", "fournisseur"],
}
_KW_FACT_VENTE = {
    "N facture externe":         ["n facture", "numero facture", "n fact", "ref facture"],
    "Date facture":              ["date facture", "date"],
    "Code client":               ["code client"],
    "Designation":               ["designation", "libelle", "objet"],
    "Qte":                       ["qte", "quantite"],
    "PU HT":                     ["pu ht", "ht", "montant ht"],
    "TVA":                       ["tva"],
    "Conditions de reglement":   ["condition", "reglement"],
}
_KW_FACT_ACHAT = {
    "Reference d'origine de la piece": ["n facture", "numero", "reference", "ref piece"],
    "Date depense":                    ["date"],
    "Code Fournisseur":                ["code fournisseur", "code fourn"],
    "Libelle":                         ["libelle", "designation", "objet"],
    "Total TTC":                       ["ttc", "total ttc", "montant ttc"],
    "Total HT":                        ["ht", "total ht", "montant ht"],
}


def _render_simple_module(st, module_label, module_key, source_file, target_keywords, build_rows_fn,
                           filename, help_msg=None):
    """UI generique : preview + auto-map + telechargement gabarit."""
    if source_file is None:
        st.info(f"Importez d'abord un fichier {module_label.lower()} dans l'onglet **📁 Import fichiers**.")
        return
    df = _read_any(source_file)
    if df is None or len(df) == 0:
        st.error("Impossible de lire ce fichier.")
        return
    st.caption(f"📄 {len(df)} ligne(s) lue(s) — colonnes detectees : {len(df.columns)}")
    with st.expander("Apercu source", expanded=False):
        st.dataframe(df.head(20), use_container_width=True)

    auto = _auto_map(list(df.columns), target_keywords)
    st.markdown("**Mapping des colonnes**")
    if help_msg:
        st.caption(help_msg)
    src_cols = ["(non mappe)"] + list(df.columns)
    user_map = {}
    cols = st.columns(2)
    for i, tgt in enumerate(target_keywords.keys()):
        cur = auto.get(tgt, "(non mappe)")
        idx = src_cols.index(cur) if cur in src_cols else 0
        user_map[tgt] = cols[i % 2].selectbox(tgt, src_cols, index=idx, key=f"map_{module_key}_{tgt}")

    # Construit un DataFrame normalise selon le mapping
    df_norm = pd.DataFrame(index=df.index)
    for tgt, src in user_map.items():
        if src and src != "(non mappe)":
            df_norm[tgt] = df[src].values
        else:
            df_norm[tgt] = ""

    # Genere les rows + xlsx
    rows = build_rows_fn(df_norm)
    st.success(f"✅ {len(rows)} ligne(s) prete(s) pour le gabarit Evoliz **{TEMPLATE_LABELS[module_key]}**")
    if rows:
        with st.expander(f"Apercu des {min(20, len(rows))} premieres lignes du gabarit", expanded=False):
            st.dataframe(pd.DataFrame(rows[:20]), use_container_width=True)
    st.warning(f"⚠️ **Avant l'import dans Evoliz** : videz les **{TEMPLATE_LABELS[module_key]}** existants "
               "dans le dossier cible pour eviter les doublons avec les donnees preexistantes qui feraient echouer l'import.")
    data = make_xlsx(module_key, rows)
    st.download_button(
        f"📥 Telecharger le gabarit Evoliz {TEMPLATE_LABELS[module_key]} (.xlsx)",
        data=data, file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"dl_offline_{module_key}", type="primary", use_container_width=True,
    )


def render_clients_offline(st, source_file):
    st.subheader("👥 Clients — Generation du gabarit Evoliz (hors-ligne)")
    _render_simple_module(st, "Clients", "clients", source_file, _KW_CLIENTS,
                          lambda dfn: map_clients(dfn),
                          "Gabarit_Evoliz_Clients.xlsx",
                          help_msg="Mapping auto base sur le nom des colonnes ; ajustez si besoin.")


def render_fournisseurs_offline(st, source_file):
    st.subheader("🏭 Fournisseurs — Generation du gabarit Evoliz (hors-ligne)")
    def _build(dfn):
        return map_fournisseurs([{c: r[c] for c in dfn.columns} for _, r in dfn.iterrows()])
    _render_simple_module(st, "Fournisseurs", "fournisseurs", source_file, _KW_FOURN,
                          _build,
                          "Gabarit_Evoliz_Fournisseurs.xlsx")


def render_articles_offline(st, source_file):
    st.subheader("📦 Articles — Generation du gabarit Evoliz (hors-ligne)")
    def _build(dfn):
        items = [{c: r[c] for c in dfn.columns} for _, r in dfn.iterrows()]
        return map_articles(items)
    _render_simple_module(st, "Articles", "articles", source_file, _KW_ARTICLES,
                          _build,
                          "Gabarit_Evoliz_Articles.xlsx")


def render_factures_offline(st, source_file):
    st.subheader("🧾 Factures — Generation du gabarit Evoliz (hors-ligne)")
    if source_file is None:
        st.info("Importez d'abord un fichier factures dans l'onglet **📁 Import fichiers**.")
        return
    type_fac = st.radio("Type de factures dans ce fichier", ["Vente", "Achat (depense)", "Mixte"], horizontal=True, key="fac_type_offline")
    if type_fac == "Vente":
        _render_simple_module(st, "Factures de vente", "factures_vente", source_file, _KW_FACT_VENTE,
                              lambda dfn: map_factures_vente(dfn),
                              "Gabarit_Evoliz_Factures_Vente.xlsx")
    elif type_fac == "Achat (depense)":
        _render_simple_module(st, "Depenses (factures d'achat)", "factures_achat", source_file, _KW_FACT_ACHAT,
                              lambda dfn: map_factures_achat(dfn),
                              "Gabarit_Evoliz_Factures_Achat.xlsx")
    else:
        st.caption("Mode mixte : indiquez la colonne qui distingue achat/vente.")
        df = _read_any(source_file)
        if df is None: return
        col_split = st.selectbox("Colonne 'sens' (Achat / Vente)", ["(aucune)"] + list(df.columns), key="fac_split_col")
        if col_split == "(aucune)":
            st.warning("Selectionnez une colonne contenant 'Achat' ou 'Vente' (sinon utilisez les onglets dedies).")
            return
        s = df[col_split].astype(str).str.upper().str.strip()
        df_v = df[s.str.contains("VENT", na=False)].reset_index(drop=True)
        df_a = df[s.str.contains("ACHA", na=False)].reset_index(drop=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**Vente : {len(df_v)} ligne(s)**")
            if len(df_v):
                rows = map_factures_vente(_normalize_with_kw(df_v, _KW_FACT_VENTE))
                st.download_button("📥 Gabarit ventes", make_xlsx("factures_vente", rows),
                                   "Gabarit_Evoliz_Factures_Vente.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   key="dl_off_fv", use_container_width=True)
        with c2:
            st.markdown(f"**Achat : {len(df_a)} ligne(s)**")
            if len(df_a):
                rows = map_factures_achat(_normalize_with_kw(df_a, _KW_FACT_ACHAT))
                st.download_button("📥 Gabarit achats", make_xlsx("factures_achat", rows),
                                   "Gabarit_Evoliz_Factures_Achat.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   key="dl_off_fa", use_container_width=True)


def _normalize_with_kw(df, target_keywords):
    auto = _auto_map(list(df.columns), target_keywords)
    df_n = pd.DataFrame(index=df.index)
    for tgt in target_keywords:
        if tgt in auto:
            df_n[tgt] = df[auto[tgt]].values
        else:
            df_n[tgt] = ""
    return df_n


def render_offline_welcome(st, access_label=""):
    """Page d'accueil de l'onglet API quand le mode hors-ligne est actif."""
    st.subheader("📥 Mode hors-ligne actif")
    if access_label:
        st.caption(f"Acces : **{access_label}**")
    st.markdown(
        """
**Aucune connexion a l'API Evoliz n'est requise.**

Les onglets ci-dessus permettent de generer les **gabarits xlsx d'import Evoliz**
a partir de vos fichiers sources, prets a etre importes manuellement dans Evoliz
(Imports & Exports → Importer).

#### Marche a suivre
1. Onglet **📁 Import fichiers** : importer les fichiers sources (clients, fournisseurs, articles, factures, balance).
2. Pour chaque module : ouvrir l'onglet, ajuster le mapping des colonnes, telecharger le gabarit xlsx.
3. Importer le(s) gabarit(s) dans Evoliz.

#### Modules disponibles
- 🔍 **Matrice comptable** : 5 gabarits (comptes, classifs achat/vente, affectations entree/sortie banque)
- 👥 **Clients**
- 🏭 **Fournisseurs**
- 📦 **Articles**
- 🧾 **Factures & Avoirs** : ventes, achats (depenses), ou les deux
"""
    )


def render_compta_offline(st, balance_file, param_local_df, default_tva_rate=20.0):
    """Pour la matrice : a partir d'une balance + parametres comptables, genere les 5 gabarits."""
    st.subheader("🔍 Matrice comptable — Generation des 5 gabarits Evoliz (hors-ligne)")
    if balance_file is None:
        st.info("Importez d'abord une balance dans l'onglet **📁 Import fichiers**.")
        return
    if param_local_df is None or len(param_local_df) == 0:
        st.warning("⚠️ Parametres comptables (param_local) absents. Importez-les via la sidebar.")
        return
    df_b = _read_any(balance_file)
    if df_b is None:
        st.error("Impossible de lire la balance.")
        return
    # On cherche les 2 premieres colonnes qui ressemblent a Code + Libelle
    cols = list(df_b.columns)
    idx_code = idx_lib = None
    for i, c in enumerate(cols):
        nc = _norm(c)
        if idx_code is None and ("code" in nc or "compte" in nc or "n compte" in nc or "n compte" in nc):
            idx_code = i
        if idx_lib is None and ("libelle" in nc or "intitule" in nc or "designation" in nc or "label" in nc):
            idx_lib = i
    if idx_code is None: idx_code = 0
    if idx_lib is None: idx_lib = 1 if len(cols) > 1 else 0

    cc, cl = st.columns(2)
    col_code = cc.selectbox("Colonne du code compte", cols, index=idx_code, key="compta_off_code")
    col_lib  = cl.selectbox("Colonne du libelle",     cols, index=idx_lib,  key="compta_off_lib")

    # Construire la "matrice" minimale (COMPTE_CODE, LIBELLE, ACHAT/VENTE/ENTREE/SORTIE)
    pl = param_local_df.copy()
    pl["Racine"] = pl["Racine"].astype(str)
    pl_sorted = pl.sort_values("Racine", key=lambda s: s.str.len(), ascending=False)

    def _flux_for(code):
        """Retourne (achat, vente, entree, sortie) pour un code, en cherchant la racine la plus longue."""
        sc = str(code).strip()
        for _, r in pl_sorted.iterrows():
            if sc.startswith(str(r["Racine"]).strip()):
                return (
                    bool(r.get("ACHAT", False)),
                    bool(r.get("VENTE", False)),
                    bool(r.get("ENTRÉE BQ", False)),
                    bool(r.get("SORTIE BQ", False)),
                )
        return (False, False, False, False)

    rows = []
    for _, r in df_b.iterrows():
        code = str(r[col_code]).strip()
        if not code or code in ("nan", "None"):
            continue
        # on garde aussi les libelles vides
        lib = str(r[col_lib]).strip() if col_lib in df_b.columns else ""
        a, v, e, s = _flux_for(code)
        rows.append({"COMPTE_CODE": code, "LIBELLE": lib, "ACHAT": a, "VENTE": v, "ENTRÉE BQ": e, "SORTIE BQ": s})
    audit = pd.DataFrame(rows)
    st.caption(f"📊 {len(audit)} compte(s) extrait(s) de la balance")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ACHAT", int(audit["ACHAT"].sum()))
    c2.metric("VENTE", int(audit["VENTE"].sum()))
    c3.metric("ENTREE BQ", int(audit["ENTRÉE BQ"].sum()))
    c4.metric("SORTIE BQ", int(audit["SORTIE BQ"].sum()))

    tva = st.number_input("Taux TVA achats par defaut (%)", value=float(default_tva_rate), step=0.5, key="compta_off_tva")
    if st.button("📥 Generer les 5 gabarits comptables (zip)", type="primary", use_container_width=True, key="btn_off_compta_zip"):
        zip_bytes = build_compta_zip(audit, default_tva_rate=tva)
        st.download_button("📦 Telecharger le zip des 5 gabarits",
                           data=zip_bytes, file_name="Gabarits_Evoliz_Comptabilite.zip",
                           mime="application/zip", key="dl_off_compta_zip", use_container_width=True)
