#!/usr/bin/env python3
import argparse, io, os, re, sys, zipfile, tempfile, unicodedata, math, shutil, warnings
from datetime import datetime, date
from typing import List, Dict, Tuple

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

warnings.simplefilter("ignore", category=UserWarning)

# ============== Normalizare headers (tolerant la diacritice/NBSP/punctuație) ==============

def norm(s: str) -> str:
    """Normalizează pentru potrivire tolerantă: diacritice out, lowercase,
       NBSP -> space, scoate tot ce nu e [a-z0-9]"""
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")  # NBSP
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

# Aliasuri uzuale pentru capete de tabel (am pus variantele cu / fără spații, diacritice, etc.)
ALIASES = {
    # TID
    "nrterminal": ["nrterminal", "tid", "terminalid", "terminal"],
    # denumire site / societate / locație
    "denumiresite": [
        "denumiresite", "denumiresocietate", "denumiresocietateagent",
        "denumirelocatie", "site", "locatie", "denumiresocietate agent"
    ],
    # denumire produs / comerciant (în ASTOB apare ca „Nume Comerciant”)
    "denumireprodus": ["denumireprodus", "produs", "numecomerciant", "comerciant"],
    # valoare cu TVA
    "valoarecutva": ["valoarecutva", "valoare", "valoaretva", "suma", "sumatranzactie"],
    # data tranzacției
    "datatranzactiei": ["datatranzactiei", "datatranzactie", "data"],
    # ora tranzacției (când e pe coloană separată)
    "oratranzactiei": ["oratranzactiei", "ora", "time"],
    # cheie (nume client) în tabelul cheie
    "nume": ["nume", "client", "denumireclient", "denumire"],
    # CUI
    "cui": ["cui", "codfiscal"],
    # Nr. înregistrare R.C.
    "nrinreg": ["nrinregistrarerc", "nrinregistrarerc", "nr inregistrare rc", "nrregistrarerc", "nrregcom", "j"],
    # Adresa
    "adresa": ["adresa", "sediu", "sediulcentral", "sediul central"],
}

def expand_candidates(cands: List[str]) -> List[str]:
    """Extinde cu aliasuri (normalizate)."""
    out = []
    for c in cands:
        n = norm(c)
        out.append(n)
        for key, group in ALIASES.items():
            if n == key or n in group:
                out.extend(group)
    # unici, păstrând ordinea
    seen = set(); res = []
    for x in out:
        if x not in seen:
            seen.add(x); res.append(x)
    return res

def find_col(df: pd.DataFrame, candidates: List[str]) -> str:
    """Găsește coloana în df potrivind tolerant. Întoarce numele original."""
    cmap = {norm(col): col for col in df.columns}
    variants = expand_candidates(candidates)

    # 1. potrivire exactă (după normalizare)
    for v in variants:
        if v in cmap:
            return cmap[v]

    # 2. fallback: dacă există un singur hit ca substring
    for v in variants:
        hits = [orig for k, orig in cmap.items() if v in k]
        if len(hits) == 1:
            return hits[0]

    raise KeyError(f"Missing columns: tried {candidates} (norm={variants}) in {list(df.columns)}")

# ============== Citire fișiere tolerant XLSX/CSV ==============

def read_table(path: str) -> pd.DataFrame:
    """Încărcă xlsx sau csv, cu fallback-uri la encodări și separatori."""
    name = os.path.basename(path)
    ext = os.path.splitext(name)[1].lower()

    # Încerc xlsx cu openpyxl
    if ext in (".xlsx", ".xlsm", ".xltx", ".xltm", ".xls"):
        try:
            return pd.read_excel(path, engine="openpyxl")
        except Exception:
            # poate fi CSV cu extensie greșită
            pass

    # CSV: încerc cu sniff + encodări
    for enc in ("utf-8", "latin-1", "cp1250", "cp1252"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception:
            continue

    # last resort
    return pd.read_csv(path, engine="python")

# ============== Utilitare ==============

RO_MONTHS = {
    1: "IANUARIE", 2: "FEBRUARIE", 3: "MARTIE", 4: "APRILIE",
    5: "MAI", 6: "IUNIE", 7: "IULIE", 8: "AUGUST",
    9: "SEPTEMBRIE", 10: "OCTOMBRIE", 11: "NOIEMBRIE", 12: "DECEMBRIE",
}

def ro_header_date(dt: date) -> str:
    return f"{dt.day} {RO_MONTHS[dt.month]} {dt.year}"

def as_datetime(dt_series: pd.Series) -> pd.Series:
    """Parsează tolerant dată/oră."""
    return pd.to_datetime(dt_series, errors="coerce", dayfirst=True, infer_datetime_format=True)

def to_float(s) -> float:
    if pd.isna(s):
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).replace("\u00A0", " ").strip()
    # în fișierele tale apare virgulă ca separator zecimal
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

# ============== Pregătire date ==============

def prepare_astob(ast: pd.DataFrame) -> pd.DataFrame:
    """Standardizează ASTOB: coloane: site, tid, produs, valoare, data"""
    col_site  = find_col(ast, ["Denumire Site"])
    col_tid   = find_col(ast, ["Nr. terminal", "TID"])
    # în ASTOB „Nume Comerciant” reprezintă ce vrei în „Denumire Produs”
    try:
        col_prod = find_col(ast, ["Denumire Produs", "Nume Comerciant"])
    except KeyError:
        col_prod = find_col(ast, ["Nume Comerciant", "Comerciant"])
    col_val   = find_col(ast, ["Valoare cu TVA", "Sumă tranzacție"])
    # data poate fi în două coloane (dată + oră) sau una singură
    try:
        col_data = find_col(ast, ["Data tranzacției"])
        data_series = as_datetime(ast[col_data])
    except KeyError:
        col_data = None
        col_date = find_col(ast, ["Data tranzacției", "Data"])
        col_time = find_col(ast, ["Ora tranzacției", "Ora"])
        data_series = as_datetime(ast[col_date].astype(str) + " " + ast[col_time].astype(str))

    df = pd.DataFrame({
        "site":   ast[col_site],
        "tid":    ast[col_tid].astype(str).str.strip(),
        "produs": ast[col_prod],
        "valoare": ast[col_val].map(to_float),
        "data":   data_series,
    })

    # curăță rândurile invalide
    df = df.dropna(subset=["tid", "data"])
    df = df[df["valoare"].map(lambda x: isinstance(x, (int, float, float)) or not pd.isna(x))]
    df["valoare"] = df["valoare"].fillna(0.0).astype(float)

    # sortează după data+ora crescător
    df = df.sort_values("data", kind="mergesort").reset_index(drop=True)
    return df

def prepare_key(key: pd.DataFrame) -> pd.DataFrame:
    """Standardizează KEY: coloane: tid, nume, cui, nrinreg, adresa"""
    col_tid    = find_col(key, ["TID", "Nr. terminal"])
    col_nume   = find_col(key, ["NUME", "Client"])
    col_cui    = find_col(key, ["CUI"])
    col_reg    = find_col(key, ["Nr. înregistrare R.C.", "NR. INREGISTRARE R.C.", "J"])
    col_adresa = find_col(key, ["ADRESA", "Sediul central"])

    df = pd.DataFrame({
        "tid":    key[col_tid].astype(str).str.strip(),
        "NUME":   key[col_nume].astype(str).str.strip(),
        "CUI":    key[col_cui].astype(str).str.strip(),
        "NR. INREGISTRARE R.C.": key[col_reg].astype(str).str.strip(),
        "ADRESA": key[col_adresa].astype(str).str.strip(),
    })
    # elimină rânduri goale
    df = df.dropna(subset=["tid", "NUME"])
    df = df[df["tid"] != ""]
    return df

# ============== Scriere în șablon ==============

def write_client_order(template_path: str,
                       out_path: str,
                       header_date: date,
                       colectari_text: str,
                       client_row: Dict,
                       rows: pd.DataFrame) -> None:
    """Scrie un Excel din șablon pentru un client."""
    wb = load_workbook(template_path)
    ws = wb.active

    # 1) antet stânga (Furnizor fix în șablon)
    # 2) antet dreapta – client
    replace_map = {
        "{NUME}": client_row["NUME"],
        "{CUI}": client_row["CUI"],
        "{NR. INREGISTRARE R.C.}": client_row["NR. INREGISTRARE R.C."],
        "{ADRESA}": client_row["ADRESA"],
        "{HEADER_DATE}": ro_header_date(header_date),
        "{COLECTARI}": colectari_text,
    }

    for row in ws.iter_rows(min_row=1, max_row=40, min_col=1, max_col=8):
        for cell in row:
            if isinstance(cell.value, str):
                v = cell.value
                for k, val in replace_map.items():
                    if k in v:
                        v = v.replace(k, str(val))
                cell.value = v

    # 3) tranzacții – în șablon capetele sunt pe rândul 16, iar rândul 17 are „placeholder”
    start_row = 17  # prima linie de date
    r = start_row
    for _, rec in rows.iterrows():
        ws[f"A{r}"].value = str(rec["site"])
        ws[f"B{r}"].value = str(rec["tid"])
        ws[f"C{r}"].value = str(rec["produs"])
        ws[f"D{r}"].value = float(rec["valoare"])
        ws[f"E{r}"].value = rec["data"].strftime("%Y-%m-%d %H:%M:%S")
        r += 1

    # 4) total – rândul cu „Total” e imediat sub listă (în șablon e preimprimat „Total” în col A)
    total_cell = f"D{r}"
    ws[total_cell].value = float(rows["valoare"].sum())

    # opțional: curăț zona rămasă liberă (nu e obligatoriu)
    # salvez
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    wb.save(out_path)
    wb.close()

# ============== Run generator ==============

def run_generator(astob_path: str, key_path: str,
                  template_path: str, out_dir: str, out_zip: str) -> bool:
    print("[debug] run_generator:", astob_path, key_path, template_path, out_dir, out_zip, file=sys.stderr)

    ast_raw = read_table(astob_path)
    key_raw = read_table(key_path)

    ast = prepare_astob(ast_raw)
    key = prepare_key(key_raw)

    if ast.empty:
        raise RuntimeError("Fișierul ASTOB este gol după procesare.")
    if key.empty:
        raise RuntimeError("Fișierul TABEL CHEIE este gol după procesare.")

    # determin perioadă colectări (min/max dată)
    dmin = ast["data"].min()
    dmax = ast["data"].max()
    colectari_text = f"Colectari - {dmin.date().strftime('%d.%m.%Y')} - {dmax.date().strftime('%d.%m.%Y')}"

    # data din antet = data RULĂRII (azi, timezone server). Dacă vrei Europe/Bucharest, setează ENV TZ.
    header_date = date.today()

    # îmbogățesc ASTOB cu datele clientului (după TID)
    merged = ast.merge(key, on="tid", how="left")

    # total pe client (după NUME); sar peste total <= 0
    groups = []
    for nume, grp in merged.groupby("NUME", dropna=True):
        total = grp["valoare"].sum()
        if total <= 0 or pd.isna(total):
            continue
        # păstrez ordinea corectă (deja sortat în prepare_astob)
        groups.append((nume, grp.copy()))

    if not groups:
        raise RuntimeError("Nu există clienți cu total > 0.")

    # șterg/out dir
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    # scriu câte un Excel per client
    for nume, grp in groups:
        # iau datele clientului din primul rând al grupului
        crow = {
            "NUME": nume,
            "CUI": grp["CUI"].iloc[0] if "CUI" in grp.columns else "",
            "NR. INREGISTRARE R.C.": grp["NR. INREGISTRARE R.C."].iloc[0] if "NR. INREGISTRARE R.C." in grp.columns else "",
            "ADRESA": grp["ADRESA"].iloc[0] if "ADRESA" in grp.columns else "",
        }
        out_xlsx = os.path.join(out_dir, f"Ordin - {nume}.xlsx")
        write_client_order(template_path, out_xlsx, header_date, colectari_text, crow, grp[["site","tid","produs","valoare","data"]])

    # arhivez în ZIP
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in sorted(os.listdir(out_dir)):
            zf.write(os.path.join(out_dir, fname), arcname=fname)

    return True

# ============== CLI ==============

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--astob", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--template", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--out-zip", required=True)
    args = p.parse_args()

    ok = run_generator(args.astob, args.key, args.template, args.out_dir, args.out_zip)
    if not ok:
        sys.exit(2)

if __name__ == "__main__":
    main()
