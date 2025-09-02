#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, re, zipfile, unicodedata
from pathlib import Path
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font

# ---------- utilitare ----------
def norm(s):
    if s is None: return ""
    if not isinstance(s, str): s = str(s)
    s = s.strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return s.lower()

def sniff_type(path: Path) -> str:
    try:
        with open(path, "rb") as f:
            sig = f.read(8)
        if sig.startswith(b"PK"):  # .xlsx (zip)
            return "xlsx"
        if sig.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):  # .xls (OLE)
            return "xls"
    except Exception:
        pass
    return "other"

def read_table(path: Path) -> pd.DataFrame:
    kind = sniff_type(path)
    trials = []
    if kind == "xlsx":
        trials = [("excel-openpyxl", dict(fn=pd.read_excel, kw=dict(engine="openpyxl")))]
    elif kind == "xls":
        trials = [("excel-xlrd", dict(fn=pd.read_excel, kw=dict(engine="xlrd")))]
    else:
        trials = [
            ("excel-openpyxl", dict(fn=pd.read_excel, kw=dict(engine="openpyxl"))),
            ("excel-xlrd",     dict(fn=pd.read_excel, kw=dict(engine="xlrd"))),
            ("csv-auto",       dict(fn=pd.read_csv,   kw=dict(sep=None, engine="python"))),
            ("csv-utf8",       dict(fn=pd.read_csv,   kw=dict(encoding="utf-8", sep=None, engine="python"))),
            ("csv-latin1",     dict(fn=pd.read_csv,   kw=dict(encoding="latin-1", sep=None, engine="python"))),
            ("csv-win1250",    dict(fn=pd.read_csv,   kw=dict(encoding="windows-1250", sep=None, engine="python"))),
            ("csv-win1252",    dict(fn=pd.read_csv,   kw=dict(encoding="windows-1252", sep=None, engine="python"))),
        ]
    errors = []
    for tag, spec in trials:
        try:
            df = spec["fn"](path, **spec["kw"])
            print(f"[read_table] {path.name}: ok via {tag}")
            return df
        except Exception as e:
            errors.append(f"{tag}: {e.__class__.__name__}: {e}")
    raise ValueError(f"Cannot read table {path.name}. Tried: " + " | ".join(errors))

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

def opt_col(df, candidates):
    try:
        return find_col(df, candidates)
    except Exception:
        return None

TOKENS_TABLE = ["denumire site","tid","denumire produs","valoare cu tva","data tranzactiei"]
def find_model_row(ws):
    def clean(v):
        s = norm(v)
        s = s.replace("{","").replace("}","")
        return s
    for r in range(1, ws.max_row+1):
        found = set()
        for c in range(1, ws.max_column+1):
            s = clean(ws.cell(r,c).value)
            for t in TOKENS_TABLE:
                if t in s: found.add(t)
        if len(found)==len(TOKENS_TABLE):
            colmap = {}
            for c in range(1, ws.max_column+1):
                s = clean(ws.cell(r,c).value)
                for t in TOKENS_TABLE:
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

def replace_placeholders(ws, mapping: dict):
    # înlocuiește {CHEIE} oriunde apare (case-insensitive, permite și diacritice)
    for r in range(1, ws.max_row+1):
        for c in range(1, ws.max_column+1):
            v = ws.cell(r,c).value
            if not isinstance(v, str):
                continue
            new_v = v
            for key, val in mapping.items():
                # acceptă si variante cu/ fără punctuație/diacritice ex: {NR. INREGISTRARE R.C.}
                pat = re.compile(r"\{\s*"+re.escape(key)+r"\s*\}", re.I)
                new_v = pat.sub("" if val is None else str(val), new_v)
            if new_v != v:
                ws.cell(r,c).value = new_v

def clear_leftover_token_rows(ws, start_row, search_rows=10):
    # dacă mai există un rând cu {DENUMIRE SITE}/{TID}/... după tabel, îl curățăm
    token_re = re.compile(r"\{\s*(denumire site|tid|denumire produs|valoare cu tva|data tranzactiei)\s*\}", re.I)
    for r in range(start_row, min(ws.max_row, start_row+search_rows)+1):
        has_token = False
        for c in range(1, ws.max_column+1):
            v = ws.cell(r,c).value
            if isinstance(v, str) and token_re.search(v):
                has_token = True
                break
        if has_token:
            for c in range(1, ws.max_column+1):
                ws.cell(r,c).value = None  # golim rândul
    return

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# ---------- MAIN ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--astob", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--template", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--out-zip", required=True)
    ap.add_argument("--clients-zip", required=False)  # rezervat
    args = ap.parse_args()

    astob_path = Path(args.astob)
    key_path = Path(args.key)
    template_path = Path(args.template)
    out_dir = Path(args.out_dir)
    out_zip = Path(args.out_zip)
    ensure_dir(out_dir)

    print(f"[debug] astob={astob_path.name}, key={key_path.name}")
    print(f"[debug] sniff astob={sniff_type(astob_path)}, sniff key={sniff_type(key_path)}")

    # 1) tabele
    astob = read_table(astob_path)
    key   = read_table(key_path)

    # 2) coloane obligatorii
    col_tid_key   = find_col(key, ["TID"])
    col_bmc       = find_col(key, ["BMC"])
    col_clientkey = find_col(key, [
        "DENUMIRE SOCIETATEAGENT","DENUMIRE SOCIETATE AGENT",
        "Denumire Societate Agent","Denumire Societate/Agent","Client"
    ])
    col_tid_astob = find_col(astob, ["Nr. terminal","nr terminal","terminal","TID"])
    col_prod      = find_col(astob, ["Nume Operator","Operator","Denumire Produs"])
    col_sum       = find_col(astob, ["Sumă tranzacție","Suma tranzactie","Valoare cu TVA","Valoare"])
    col_date      = find_col(astob, ["Data tranzacției","Data tranzactiei","Data"])
    col_time      = find_col(astob, ["Ora tranzacției","Ora tranzactiei","Ora"])

    # 2b) coloane opționale pentru antet
    col_cui    = opt_col(key, ["CUI","CIF"])
    col_adresa = opt_col(key, ["Adresa","Sediu central","Sediul central"])
    col_rc     = opt_col(key, ["Nr. înregistrare R.C.","Nr. inregistrare R.C.","Nr. Reg. Com.","Nr Reg Com"])

    # 3) normalizează ASTOB
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

    # 4) normalizează KEY + extra info
    cols = [col_tid_key, col_bmc, col_clientkey]
    new_names = ["TID","BMC","Client"]
    if col_cui:    cols.append(col_cui);    new_names.append("CUI")
    if col_adresa: cols.append(col_adresa); new_names.append("ADRESA")
    if col_rc:     cols.append(col_rc);     new_names.append("NR_RC")
    key2 = key[cols].copy()
    key2.columns = new_names

    # 5) join pe TID
    df = astob2.merge(key2, on="TID", how="left")

    # 6) text dată+oră
    def fmt_dt(row):
        d, t = row.get("Data"), row.get("Ora")
        try:  dtxt = pd.to_datetime(d).strftime("%Y-%m-%d")
        except Exception: dtxt = str(d)
        try:  ttxt = pd.to_datetime(t).strftime("%H:%M:%S")
        except Exception: ttxt = str(t)
        return f"{dtxt} {ttxt}".strip()
    if not df.empty:
        df["Data Tranzactiei"] = df.apply(fmt_dt, axis=1)

    # 7) fișiere per client
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
            ws.row_dimensions[2].height = 25.20  # cerința ta pentru rândul 2
        except Exception:
            pass

        model_row, colmap = find_model_row(ws)
        model_cells = {c: ws.cell(model_row, c) for c in colmap.values()}
        # memorăm înălțimea rândului model
        model_height = ws.row_dimensions[model_row].height or 15

        # pregătim rândurile de scris
        rows = []
        for _, r in g.iterrows():
            rows.append({
                "denumire site": r.get("BMC",""),
                "tid": r.get("TID",""),
                "denumire produs": r.get("Denumire Produs",""),
                "valoare cu tva": float(r.get("Valoare cu TVA",0.0)),
                "data tranzactiei": r.get("Data Tranzactiei",""),
            })

        # inserăm rânduri dacă e cazul
        if len(rows) > 1:
            ws.insert_rows(model_row+1, amount=len(rows)-1)

        # stil + valori pe fiecare rând; forțăm înălțime egală
        txn_font = Font(name="Calibri", size=14, bold=True)
        for i, rowdata in enumerate(rows):
            ridx = model_row + i
            ws.row_dimensions[ridx].height = model_height
            for token, col in colmap.items():
                cell = ws.cell(ridx, col)
                cell.value = rowdata[token]
                copy_style(model_cells[col], cell)
                cell.font = txn_font

        # înlocuim TOTAL
        replace_total(ws, total)

        # completăm antetul (și golim dacă nu avem valoare)
        # luăm info client din key (primul non-null din grup)
        def first_notna(series, default=""):
            try:
                s = series.dropna()
                return str(s.iloc[0]) if len(s) else default
            except Exception:
                return default

        mapping = {
            "NUME": client_str,
            "CLIENT": client_str,
            "CUI": first_notna(g.get("CUI",""), ""),
            "ADRESA": first_notna(g.get("ADRESA",""), ""),
            "NR. INREGISTRARE R.C.": first_notna(g.get("NR_RC",""), ""),
            "NR INREGISTRARE R.C.": first_notna(g.get("NR_RC",""), ""),
            "NR INREGISTRARE RC": first_notna(g.get("NR_RC",""), ""),
        }
        replace_placeholders(ws, mapping)

        # curățăm eventuale rânduri rămase cu token-urile tabelului
        clear_leftover_token_rows(ws, start_row=model_row+len(rows))

        # salvează
        safe_client = re.sub(r'[\\/*?:"<>|]+', "_", client_str).strip() or "Client"
        out_path = out_dir / f"Ordin - {safe_client}.xlsx"
        wb.save(out_path)
        created_files.append(out_path)

    # zip
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in created_files:
            zf.write(p, p.name)

if __name__ == "__main__":
    main()
