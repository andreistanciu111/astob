#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, re, zipfile, unicodedata
from pathlib import Path
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font

# ---------- Helpers ----------
def norm(s):
    if s is None: return ""
    if not isinstance(s, str): s = str(s)
    s = s.strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower()

def find_col(df, candidates):
    cols = {norm(c): c for c in df.columns}
    for cand in candidates:
        key = norm(cand)
        for c in cols:
            if key == c: return cols[c]
    # fuzzy: contains
    for cand in candidates:
        key = norm(cand)
        for c in cols:
            if key in c: return cols[c]
    raise KeyError(f"Missing columns: tried {candidates} in {list(df.columns)}")

def find_model_row(ws):
    tokens = ["denumire site", "tid", "denumire produs", "valoare cu tva", "data tranzactiei"]
    def clean_cell(v):
        s = norm(v)
        s = s.replace("{","").replace("}","")
        return s
    for r in range(1, ws.max_row+1):
        found = set()
        for c in range(1, ws.max_column+1):
            v = ws.cell(r,c).value
            s = clean_cell(v)
            for t in tokens:
                if t in s: found.add(t)
        if len(found)==len(tokens):
            # build token -> column map
            colmap = {}
            for c in range(1, ws.max_column+1):
                s = clean_cell(ws.cell(r,c).value)
                for t in tokens:
                    if t in s and t not in colmap:
                        colmap[t]=c
            return r, colmap
    raise RuntimeError("Template model row with table tokens not found")

def unmerge_all(ws):
    if ws.merged_cells.ranges:
        for r in list(ws.merged_cells.ranges):
            ws.unmerge_cells(str(r))

def copy_style(src, dst):
    if src.has_style:
        dst.font = src.font.copy()
        dst.fill = src.fill.copy()
        dst.border = src.border.copy()
        dst.alignment = src.alignment.copy()
        dst.number_format = src.number_format

def replace_total(ws, total_value):
    # replace any cell that contains {TOTAL} (with/without spaces/braces), numeric if exact token
    pattern = re.compile(r"\{?\s*total\s*\}?", re.I)
    for r in range(1, ws.max_row+1):
        for c in range(1, ws.max_column+1):
            v = ws.cell(r,c).value
            if isinstance(v, str) and pattern.search(v):
                if pattern.fullmatch(v.strip()):
                    ws.cell(r,c).value = float(total_value)
                else:
                    ws.cell(r,c).value = pattern.sub(f"{total_value:,.2f}".replace(",", ""), v)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--astob", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--template", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--out-zip", required=True)
    ap.add_argument("--clients-zip", required=False)
    args = ap.parse_args()

    astob_path = Path(args.astob)
    key_path = Path(args.key)
    template_path = Path(args.template)
    out_dir = Path(args.out_dir)
    out_zip = Path(args.out_zip)

    ensure_dir(out_dir)

    # 1) Read Excel files
    astob = pd.read_excel(astob_path)
    key = pd.read_excel(key_path)

    # 2) Map columns (robust to diacritics / small header changes)
    col_tid_key = find_col(key, ["TID"])
    col_bmc = find_col(key, ["BMC"])
    col_tid_astob = find_col(astob, ["Nr. terminal","nr terminal","terminal","TID"])
    col_prod = find_col(astob, ["Nume Operator","Operator","Denumire Produs"])
    col_sum = find_col(astob, ["Sumă tranzacție","Suma tranzactie","Valoare cu TVA","Valoare"])
    col_date = find_col(astob, ["Data tranzacției","Data tranzactiei","Data"])
    col_time = find_col(astob, ["Ora tranzacției","Ora tranzactiei","Ora"])
    col_client = find_col(astob, ["DENUMIRE SOCIETATEAGENT","Denumire Societate Agent","Client"])

    # 3) Prepare data
    astob2 = astob.copy()
    astob2.rename(columns={col_tid_astob:"TID",
                           col_prod:"Denumire Produs",
                           col_sum:"Valoare cu TVA",
                           col_date:"Data",
                           col_time:"Ora",
                           col_client:"Client"}, inplace=True)
    # numeric
    astob2["Valoare cu TVA"] = pd.to_numeric(astob2["Valoare cu TVA"], errors="coerce").fillna(0.0)
    # filter > 0
    astob2 = astob2[astob2["Valoare cu TVA"] > 0]

    if astob2.empty:
        # create an empty zip so the API succeeds (no clients with total > 0)
        with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            pass
        return

    key2 = key[[col_tid_key, col_bmc]].copy()
    key2.columns = ["TID","BMC"]

    # join by TID
    df = astob2.merge(key2, on="TID", how="left")
    # prepare datetime text
    def fmt_dt(row):
        d = row.get("Data")
        t = row.get("Ora")
        try:
            dtxt = pd.to_datetime(d).strftime("%Y-%m-%d")
        except Exception:
            dtxt = str(d)
        try:
            ttxt = pd.to_datetime(t).strftime("%H:%M:%S")
        except Exception:
            ttxt = str(t)
        return f"{dtxt} {ttxt}".strip()
    df["Data Tranzactiei"] = df.apply(fmt_dt, axis=1)

    # 4) Group by client and build files
    created_files = []
    for client, g in df.groupby("Client"):
        total = float(g["Valoare cu TVA"].sum())
        if total <= 0: 
            continue

        # Load template and prep sheet
        wb = load_workbook(template_path)
        ws = wb.active

        # no merged cells
        unmerge_all(ws)
        # row 2 height = 25.20
        try:
            ws.row_dimensions[2].height = 25.20
        except Exception:
            pass

        # find model row + column map
        model_row, colmap = find_model_row(ws)
        # snapshot styles from model row
        model_cells = {c: ws.cell(model_row, c) for c in colmap.values()}

        # clear model row (we’ll overwrite in-place for first row and insert others below)
        # Determine insertion start row
        start_row = model_row

        # Write rows
        rows = []
        for _, r in g.iterrows():
            rows.append({
                "denumire site": r.get("BMC",""),
                "tid": r.get("TID",""),
                "denumire produs": r.get("Denumire Produs",""),
                "valoare cu tva": float(r.get("Valoare cu TVA",0.0)),
                "data tranzactiei": r.get("Data Tranzactiei",""),
            })

        # Insert additional rows (N-1)
        if len(rows) > 1:
            ws.insert_rows(start_row+1, amount=len(rows)-1)

        # Apply Calibri 14 Bold to transaction rows
        txn_font = Font(name="Calibri", size=14, bold=True)

        for i, rowdata in enumerate(rows):
            r = start_row + i
            for token, col in colmap.items():
                key = token  # already lower
                val = rowdata[key]
                cell = ws.cell(r, col)
                # value
                cell.value = val
                # style from model
                copy_style(model_cells[col], cell)
                # enforce Calibri 14 Bold for data rows
                cell.font = txn_font

        # Replace TOTAL anywhere
        replace_total(ws, total)

        # Replace header placeholders if they exist (simple best-effort)
        placeholders = {
            "{NUME}": str(client),
            "{CLIENT}": str(client),
        }
        for r in range(1, ws.max_row+1):
            for c in range(1, ws.max_column+1):
                v = ws.cell(r,c).value
                if isinstance(v, str):
                    nv = v
                    for k, vv in placeholders.items():
                        nv = nv.replace(k, vv)
                    if nv != v:
                        ws.cell(r,c).value = nv

        # Save file
        safe_client = re.sub(r'[\\/*?:"<>|]+', "_", str(client)).strip() or "Client"
        out_path = out_dir / f"Ordin - {safe_client}.xlsx"
        wb.save(out_path)
        created_files.append(out_path)

    # 5) Zip everything (even if none, create an empty ZIP so API succeeds)
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in created_files:
            zf.write(p, p.name)

if __name__ == "__main__":
    main()
# Please place the real generate_orders.py here.
