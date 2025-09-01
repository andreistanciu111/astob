#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, re, zipfile, unicodedata
from pathlib import Path
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font

# ---------- Helpers ----------
def norm(s):
    if s is None: return ""
    if not isinstance(s, str): s = str(s)
    s = s.strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return s.lower()

def read_table(path: Path) -> pd.DataFrame:
    """Citește .xlsx/.xls/.csv, cu fallback-uri utile."""
    ext = path.suffix.lower()
    try:
        if ext in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
            return pd.read_excel(path, engine="openpyxl")
        if ext == ".xls":
            return pd.read_excel(path, engine="xlrd")
        if ext == ".csv":
            # autodetect separator
            return pd.read_csv(path, sep=None, engine="python")
        # fallback 1: încearcă openpyxl
        return pd.read_excel(path, engine="openpyxl")
    except Exception:
        # fallback 2: încearcă CSV generic
        try:
            return pd.read_csv(path, sep=None, engine="python")
        except Exception as e2:
            raise

def find_col(df, candidates):
    cols = {norm(c): c for c in df.columns}
    for cand in candidates:
        key = norm(cand)
        if key in cols:
            return cols[key]
    for cand in candidates:
        key = norm(cand)
        for k, v in cols.items():
            if key in k:
                return v
    raise KeyError(f"Missing columns: tried {candidates} in {list(df.columns)}")

def find_model_row(ws):
    tokens = ["denumire site","tid","denumire produs","valoare cu tva","data tranzactiei"]
    def clean(v):
        s = norm(v)
        s = s.replace("{","").replace("}","")
        return s
    for r in range(1, ws.max_row+1):
        found = set()
        for c in range(1, ws.max_column+1):
            s = clean(ws.cell(r,c).value)
            for t in tokens:
                if t in s: found.add(t)
        if len(found)==len(tokens):
            colmap = {}
            for c in range(1, ws.max_column+1):
                s = clean(ws.cell(r,c).value)
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
    if getattr(src, "has_style", False):
        try:
            dst.font = src.font.copy()
            dst.fill = src.fill.copy()
            dst.border = src.border.copy()
            dst.alignment = src.alignment.copy()
            dst.number_format = src.number_format
        except Exception:
            pass

def replace_total(ws, total_value):
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

    # 1) Citește tabelele
    astob = read_table(astob_path)
    key   = read_table(key_path)

    # 2) Mapează coloane (robust la variații/diacritice)
    col_tid_key   = find_col(key, ["TID"])
    col_bmc       = find_col(key, ["BMC"])
    col_clientkey = find_col(key, [
        "DENUMIRE SOCIETATEAGENT", "DENUMIRE SOCIETATE AGENT",
        "Denumire Societate Agent", "Denumire Societate/Agent", "Client"
    ])
    col_tid_astob = find_col(astob, ["Nr. terminal","nr terminal","terminal","TID"])
    col_prod      = find_col(astob, ["Nume Operator","Operator","Denumire Produs"])
    col_sum       = find_col(astob, ["Sumă tranzacție","Suma tranzactie","Valoare cu TVA","Valoare"])
    col_date      = find_col(astob, ["Data tranzacției","Data tranzactiei","Data"])
    col_time      = find_col(astob, ["Ora tranzacției","Ora tranzactiei","Ora"])

    # 3) Normalizează ASTOB
    astob2 = astob.copy()
    astob2.rename(columns={
        col_tid_astob:"TID",
        col_prod:"Denumire Produs",
        col_sum:"Valoare cu TVA",
        col_date:"Data",
        col_time:"Ora",
    }, inplace=True)
    astob2["Valoare cu TVA"] = pd.to_numeric(astob2["Valoare cu TVA"], errors="coerce").fillna(0.0)
    astob2 = astob2[astob2["Valoare cu TVA"] > 0]

    # 4) Normalizează KEY
    key2 = key[[col_tid_key, col_bmc, col_clientkey]].copy()
    key2.columns = ["TID","BMC","Client"]

    # 5) Join pe TID
    df = astob2.merge(key2, on="TID", how="left")

    # 6) Datetime text
    def fmt_dt(row):
        d, t = row.get("Data"), row.get("Ora")
        try:  dtxt = pd.to_datetime(d).strftime("%Y-%m-%d")
        except Exception: dtxt = str(d)
        try:  ttxt = pd.to_datetime(t).strftime("%H:%M:%S")
        except Exception: ttxt = str(t)
        return f"{dtxt} {ttxt}".strip()
    if not df.empty:
        df["Data Tranzactiei"] = df.apply(fmt_dt, axis=1)

    # 7) Creează fișiere per CLIENT
    created_files = []
    for client, g in df.groupby("Client", dropna=True):
        client_str = "" if pd.isna(client) else str(client).strip()
        total = float(g["Valoare cu TVA"].sum())
        if total <= 0 or not client_str:
            continue

        wb = load_workbook(template_path)
        ws = wb.active
        unmerge_all(ws)
        try:
            ws.row_dimensions[2].height = 25.20
        except Exception:
            pass

        model_row, colmap = find_model_row(ws)
        model_cells = {c: ws.cell(model_row, c) for c in colmap.values()}

        rows = []
        for _, r in g.iterrows():
            rows.append({
                "denumire site": r.get("BMC",""),
                "tid": r.get("TID",""),
                "denumire produs": r.get("Denumire Produs",""),
                "valoare cu tva": float(r.get("Valoare cu TVA",0.0)),
                "data tranzactiei": r.get("Data Tranzactiei",""),
            })

        if len(rows) > 1:
            ws.insert_rows(model_row+1, amount=len(rows)-1)

        txn_font = Font(name="Calibri", size=14, bold=True)
        for i, rowdata in enumerate(rows):
            r = model_row + i
            for token, col in colmap.items():
                cell = ws.cell(r, col)
                cell.value = rowdata[token]
                copy_style(model_cells[col], cell)
                cell.font = txn_font

        replace_total(ws, total)

        placeholders = {"{NUME}": client_str, "{CLIENT}": client_str}
        for r in range(1, ws.max_row+1):
            for c in range(1, ws.max_column+1):
                v = ws.cell(r,c).value
                if isinstance(v, str):
                    nv = v
                    for k, vv in placeholders.items():
                        nv = nv.replace(k, vv)
                    if nv != v:
                        ws.cell(r,c).value = nv

        safe_client = re.sub(r'[\\/*?:"<>|]+', "_", client_str).strip() or "Client"
        out_path = out_dir / f"Ordin - {safe_client}.xlsx"
        wb.save(out_path)
        created_files.append(out_path)

    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in created_files:
            zf.write(p, p.name)

if __name__ == "__main__":
    main()
