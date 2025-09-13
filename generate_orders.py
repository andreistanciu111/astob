#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import io
import os
import re
import sys
import zipfile
from datetime import datetime, timezone

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font

# ----------------------------- util -----------------------------

MONTH_RO = [
    "IANUARIE", "FEBRUARIE", "MARTIE", "APRILIE", "MAI", "IUNIE",
    "IULIE", "AUGUST", "SEPTEMBRIE", "OCTOMBRIE", "NOIEMBRIE", "DECEMBRIE"
]

def format_header_date(dt):
    return f"{dt.day} {MONTH_RO[dt.month-1]} {dt.year}"

def strip_diacritics(s: str) -> str:
    rep = {
        "ă": "a", "â": "a", "î": "i", "ş": "s", "ș": "s", "ţ": "t", "ț": "t",
        "Ă": "A", "Â": "A", "Î": "I", "Ş": "S", "Ș": "S", "Ţ": "T", "Ț": "T",
        "ó": "o", "Ó": "O", "é": "e", "É": "E", "á": "a", "Á": "A",
        "í": "i", "Í": "I", "ú": "u", "Ú": "U", "è": "e", "È": "E"
    }
    return "".join(rep.get(ch, ch) for ch in s)

def norm(s: str) -> str:
    s = strip_diacritics(str(s))
    s = re.sub(r"[^0-9A-Za-z]+", "", s).lower()
    return s

def find_col(df: pd.DataFrame, candidates: list[str]) -> str:
    cols = {norm(c): c for c in df.columns}
    for c in candidates:
        if norm(c) in cols:
            return cols[norm(c)]
    # fallback: încearcă „conține”
    for c in candidates:
        n = norm(c)
        for k, orig in cols.items():
            if n in k:
                return orig
    raise KeyError(f"Missing columns: tried {candidates} in {list(df.columns)}")

def read_table(path: str) -> pd.DataFrame:
    # sniff by extension, apoi fallback csv (latin-1) dacă e nevoie
    low = path.lower()
    try:
        if low.endswith((".xlsx", ".xlsm", ".xls")):
            return pd.read_excel(path, engine="openpyxl")
        # csv
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.read_csv(path, sep=None, engine="python")
    except Exception:
        # uneori exportul e CSV cu diacritice -> latin-1
        return pd.read_csv(path, sep=None, engine="python", encoding="latin-1")

def to_float(x):
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    s = s.replace(".", "").replace(",", ".")  # 1.234,56 -> 1234.56
    try:
        return float(s)
    except Exception:
        return 0.0

def parse_date_time(date_val, time_val=None):
    """Returnează datetime; dacă time_val e None, încearcă doar date_val."""
    if pd.isna(date_val) and pd.isna(time_val):
        return None
    try:
        if time_val is None or str(time_val).strip() == "":
            return pd.to_datetime(date_val)
        # concatenează
        return pd.to_datetime(
            f"{pd.to_datetime(date_val).date()} {str(time_val).strip()}"
        )
    except Exception:
        # fallback strict
        try:
            return pd.to_datetime(date_val)
        except Exception:
            return None

# ------------------- completare șablon Excel -------------------

def fill_top_info(ws, *, client_name, reg_nr, cui, address, colectari_str, header_date):
    """Înlocuiește token-urile exact ca în șablonul din poză."""
    mapping = {
        "{NUME}": client_name or "",
        "{NR. INREGISTRARE R.C.}": reg_nr or "",
        "{CUI}": cui or "",
        "{ADRESA}": address or "",
        "{COLECTARI}": colectari_str or "",
        "{HEADER_DATE}": header_date or "",
        # alias opțional (în caz că mai există un șablon vechi)
        "{NR_INREG}": reg_nr or "",
    }
    for row in ws.iter_rows():
        for cell in row:
            if isinstance(cell.value, str):
                s = cell.value
                changed = False
                for k, v in mapping.items():
                    if k in s:
                        s = s.replace(k, v)
                        changed = True
                if changed:
                    cell.value = s

def write_transactions_table(ws, rows):
    """
    rows: listă de dicturi cu chei:
      site, tid, produs, valoare (float), data (datetime)
    Scrie de la rândul 17 inclusiv; rândul 16 rămâne antetul vizual.
    """
    rows = [r for r in rows if r.get("data") is not None]
    rows.sort(key=lambda r: r["data"])

    START_ROW = 17
    r = START_ROW
    total = 0.0

    for it in rows:
        ws.cell(r, 1, it.get("site", ""))
        ws.cell(r, 2, it.get("tid", ""))
        ws.cell(r, 3, it.get("produs", ""))

        c_val = ws.cell(r, 4, float(it.get("valoare", 0.0)))
        c_val.number_format = '#,##0.00'

        c_dt = ws.cell(r, 5, it["data"])
        c_dt.number_format = 'yyyy-mm-dd hh:mm:ss'

        total += float(it.get("valoare", 0.0))
        r += 1

    # rând goooal între listă și total (opțional)
    # r += 1

    total_row = r + 1
    ws.cell(total_row, 1, "Total").font = Font(bold=True)
    c_tot = ws.cell(total_row, 4, total)
    c_tot.number_format = '#,##0.00'
    c_tot.font = Font(bold=True)

# --------------------------- generator --------------------------

def build_orders(astob_path: str, key_path: str, template_path: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)

    astob = read_table(astob_path)
    key = read_table(key_path)

    # ---- identificare coloane în KEY (tabel cheie) ----
    col_client = find_col(key, ["DENUMIRE SOCIETATEAGENT", "Denumire Societate Agent", "Client"])
    col_tid_key = find_col(key, ["TID"])
    # detalii client
    col_reg = find_col(key, ["NR. INREGISTRARE R.C.", "Nr. inregistrare R.C.", "NR INREG", "Nr inregistrare RC"])
    col_cui = find_col(key, ["CUI"])
    col_addr = find_col(key, ["ADRESA", "Adresa"])

    key_trim = key[[col_client, col_tid_key, col_reg, col_cui, col_addr]].copy()
    key_trim.columns = ["client", "TID", "NR_INREG", "CUI", "ADRESA"]
    # normalizări de tip
    key_trim["TID"] = key_trim["TID"].astype(str).str.strip()

    # ---- identificare coloane în ASTOB ----
    col_tid_astob = find_col(astob, ["Nr. terminal", "TID", "Terminal ID", "Nr terminal", "Terminal"])
    col_value = find_col(astob, ["Sumă tranzacție", "Suma tranzactie", "Valoare cu TVA", "Valoare"])
    # „Nume Comerciant” este exact DENUMIREA PRODUSULUI în exemplul tău
    col_prod = find_col(astob, ["Nume Comerciant", "Denumire Produs", "Denumire produs", "Produs"])
    # site-ul pentru prima coloană
    col_site = None
    for cand in ["Denumire Site", "DENUMIRE SITE", "Nume Operator", "Site"]:
        try:
            col_site = find_col(astob, [cand])
            break
        except Exception:
            pass
    if not col_site:
        # fallback: folosește numele clientului din key (după join), dacă lipsește coloana
        col_site = None

    # data + oră (pot fi una sau două coloane)
    col_date = None
    for cand in ["Data tranzacției", "Data tranzactiei", "Data"]:
        try:
            col_date = find_col(astob, [cand])
            break
        except Exception:
            pass
    col_time = None
    if col_date:
        for cand in ["Ora tranzacției", "Ora tranzactiei", "Ora", "Timp"]:
            try:
                col_time = find_col(astob, [cand])
                break
            except Exception:
                pass

    # subset astob
    need_cols = [c for c in [col_tid_astob, col_value, col_prod, col_site, col_date, col_time] if c]
    ast = astob[need_cols].copy()
    ast.columns = [("TID" if c == col_tid_astob else
                    "VALOARE" if c == col_value else
                    "PRODUS" if c == col_prod else
                    "SITE" if col_site and c == col_site else
                    "DATA" if c == col_date else
                    "ORA" if col_time and c == col_time else c) for c in ast.columns]

    # tipuri curate
    ast["TID"] = ast["TID"].astype(str).str.strip()
    ast["VALOARE"] = ast["VALOARE"].map(to_float)

    # data+ora
    if "ORA" in ast.columns:
        ast["DT"] = [parse_date_time(d, t) for d, t in zip(ast["DATA"], ast["ORA"])]
    else:
        ast["DT"] = [parse_date_time(d) for d in ast["DATA"]]

    # join după TID
    merged = pd.merge(ast, key_trim, on="TID", how="left")

    # dacă lipsește SITE în ASTOB, folosește numele clientului
    if "SITE" not in merged.columns:
        merged["SITE"] = merged["client"].fillna("")

    # grupează pe client
    groups = []
    for client_name, gdf in merged.groupby("client", dropna=True):
        if pd.isna(client_name) or str(client_name).strip() == "":
            # sari rândurile fără client potrivit
            continue
        rows = []
        for _, row in gdf.iterrows():
            rows.append({
                "site": row.get("SITE", ""),
                "tid": row.get("TID", ""),
                "produs": row.get("PRODUS", ""),
                "valoare": float(row.get("VALOARE", 0.0)),
                "data": row.get("DT", None),
            })

        # dacă nu avem tranzacții valide, sari
        if not rows:
            continue

        # info client
        groups.append({
            "client": str(client_name),
            "NR_INREG": str(row.get("NR_INREG", "")),
            "CUI": str(row.get("CUI", "")),
            "ADRESA": str(row.get("ADRESA", "")),
            "rows": rows,
        })

    if not groups:
        raise RuntimeError("Nu am găsit niciun client cu tranzacții potrivite (verifică TID în ambele fișiere).")

    # data pentru colectări
    all_dt = [r["data"] for g in groups for r in g["rows"] if r["data"] is not None]
    if not all_dt:
        raise RuntimeError("Nu am putut determina datele tranzacțiilor (coloana de dată/ora).")
    dt_min = min(all_dt)
    dt_max = max(all_dt)

    colectari_str = f"Colectari - {dt_min:%d.%m.%Y} - {dt_max:%d.%m.%Y}"
    header_date_str = format_header_date(datetime.now(timezone.utc).astimezone())

    # creează fișierele Excel per client
    out_files = []
    for g in groups:
        wb = load_workbook(template_path)
        ws = wb.active

        fill_top_info(
            ws,
            client_name=g["client"],
            reg_nr=g["NR_INREG"],
            cui=g["CUI"],
            address=g["ADRESA"],
            colectari_str=colectari_str,
            header_date=header_date_str,
        )
        write_transactions_table(ws, g["rows"])

        safe_name = re.sub(r"[^\w\s\-\(\)\._]", "_", g["client"]).strip()
        if not safe_name:
            safe_name = "CLIENT"
        fname = f"Ordin - {safe_name}.xlsx"
        fpath = os.path.join(out_dir, fname)
        wb.save(fpath)
        out_files.append(fpath)

    return out_files

def zip_files(files: list[str], out_zip: str):
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in files:
            zf.write(p, arcname=os.path.basename(p))

# ----------------------------- main -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Generează ordine de plată ASTOB pe baza șablonului.")
    ap.add_argument("--astob", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--template", required=True, help="Ex: static/bp model cu {} - date.xlsx")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--out-zip", required=True)
    args = ap.parse_args()

    files = build_orders(args.astob, args.key, args.template, args.out_dir)
    zip_files(files, args.out_zip)

if __name__ == "__main__":
    main()
