#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, re, zipfile, unicodedata
from pathlib import Path
from datetime import timedelta
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font

# =============== Utilitare ===============
def norm(s):
    if s is None: return ""
    if not isinstance(s, str): s = str(s)
    s = s.strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return s.lower()

def sniff_type(path: Path) -> str:
    try:
        sig = path.read_bytes()[:8]
        if sig.startswith(b"PK"): return "xlsx"
        if sig.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"): return "xls"
    except Exception: pass
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
        if key in cols: return cols[key]
    for cand in candidates:
        key = norm(cand)
        for k, v in cols.items():
            if key in k: return v
    raise KeyError(f"Missing columns: tried {candidates} in {list(df.columns)}")

def opt_col(df, candidates):
    try: return find_col(df, candidates)
    except Exception: return None

def unmerge_all(ws):
    if ws.merged_cells.ranges:
        for r in list(ws.merged_cells.ranges):
            ws.unmerge_cells(str(r))

def copy_style(src, dst):
    if getattr(src, "has_style", False):
        try:
            dst.font = src.font.copy(); dst.fill = src.fill.copy()
            dst.border = src.border.copy(); dst.alignment = src.alignment.copy()
            dst.number_format = src.number_format
        except Exception: pass

def replace_total(ws, total_value):
    """Doar {TOTAL} -> sumă; pune 'Total' în col. A pe același rând (Calibri 14 Bold)."""
    pat = re.compile(r"\{\s*total\s*\}", re.I)
    num_txt = f"{total_value:,.2f}".replace(",", "")
    for r in range(1, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            v = ws.cell(r, c).value
            if isinstance(v, str) and pat.search(v):
                ws.cell(r, c).value = float(total_value) if pat.fullmatch(v.strip()) else pat.sub(num_txt, v)
                left = ws.cell(r, 1)
                if not isinstance(left.value, str) or not re.search(r"\btotal\b", str(left.value), re.I):
                    left.value = "Total"
                    try: left.font = Font(name="Calibri", size=14, bold=True)
                    except Exception: pass
                return

def replace_placeholders(ws, mapping: dict):
    for r in range(1, ws.max_row+1):
        for c in range(1, ws.max_column+1):
            v = ws.cell(r,c).value
            if not isinstance(v, str): continue
            new_v = v
            for key, val in mapping.items():
                new_v = new_v.replace(key, val)
            if new_v != v:
                ws.cell(r,c).value = new_v

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# ======= detectăm STRICT rândul-model (cu {token-uri}) =======
TOKENS_TABLE = ["denumire site","tid","denumire produs","valoare cu tva","data tranzactiei"]
TOKEN_PAT = {
    "denumire site":   re.compile(r"\{\s*denumire\s*site\s*\}", re.I),
    "tid":             re.compile(r"\{\s*tid\s*\}", re.I),
    "denumire produs": re.compile(r"\{\s*denumire\s*produs\s*\}", re.I),
    "valoare cu tva":  re.compile(r"\{\s*valoare\s*cu\s*tva\s*\}", re.I),
    "data tranzactiei":re.compile(r"\{\s*data\s*tranzactiei\s*\}", re.I),
}
def find_model_row(ws):
    for r in range(1, ws.max_row+1):
        hits = {}
        for c in range(1, ws.max_column+1):
            v = ws.cell(r,c).value
            if not isinstance(v, str): continue
            for tok, pat in TOKEN_PAT.items():
                if tok not in hits and pat.search(v):
                    hits[tok] = c
        if len(hits) >= 4:
            return r, hits
    raise RuntimeError("Template model row with {DENUMIRE SITE}/{TID}/… not found")

def clear_leftover_token_rows(ws, start_row, search_rows=10):
    token_re = re.compile(r"\{\s*(denumire\s*site|tid|denumire\s*produs|valoare\s*cu\s*tva|data\s*tranzactiei)\s*\}", re.I)
    last = min(ws.max_row, start_row + search_rows)
    for r in range(start_row, last+1):
        has_token = any(isinstance(ws.cell(r,c).value, str) and token_re.search(ws.cell(r,c).value)
                        for c in range(1, ws.max_column+1))
        if has_token:
            for c in range(1, ws.max_column+1):
                ws.cell(r,c).value = None

def ro_month_upper(d) -> str:
    luni = ["IANUARIE","FEBRUARIE","MARTIE","APRILIE","MAI","IUNIE",
            "IULIE","AUGUST","SEPTEMBRIE","OCTOMBRIE","NOIEMBRIE","DECEMBRIE"]
    return f"{d.day} {luni[d.month-1]} {d.year}"

# =============== MAIN ===============
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

    print(f"[debug] astob={astob_path.name}, key={key_path.name}")
    print(f"[debug] sniff astob={sniff_type(astob_path)}, sniff key={sniff_type(key_path)}")

    # 1) Citire tabele
    astob = read_table(astob_path)
    key   = read_table(key_path)

    # 2) Coloane obligatorii
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
    col_time      = opt_col(astob, ["Ora tranzacției","Ora tranzactiei","Ora"])  # opțional

    # 3) Normalizează ASTOB
    astob2 = astob.copy()
    astob2.rename(columns={
        col_tid_astob: "TID",
        col_prod:      "Denumire Produs",
        col_sum:       "Valoare cu TVA",
        col_date:      "Data",
    }, inplace=True)
    if col_time:
        astob2.rename(columns={col_time: "Ora"}, inplace=True)

    astob2["Valoare cu TVA"] = pd.to_numeric(astob2["Valoare cu TVA"], errors="coerce").fillna(0.0)
    astob2 = astob2[astob2["Valoare cu TVA"] > 0]

    # === Datetime + sort (dată + oră) robust, dayfirst ===
    d = pd.to_datetime(astob2["Data"], errors="coerce", dayfirst=True, infer_datetime_format=True)
    if "Ora" in astob2.columns:
        t = pd.to_timedelta(astob2["Ora"].astype(str).str.strip().str.replace(".", ":", regex=False), errors="coerce")
        base = d.fillna(pd.Timestamp(1970,1,1))
        dt = base + t.fillna(pd.Timedelta(0))
    else:
        dt = d
    astob2["DT_SORT"] = dt

    # Pentru afișare:
    def fmt_dt(row):
        if pd.notna(row["DT_SORT"]):
            return row["DT_SORT"].strftime("%Y-%m-%d %H:%M:%S")
        return str(row.get("Data", "")).strip()
    astob2["Data Tranzactiei"] = astob2.apply(fmt_dt, axis=1)

    # 4) Normalizează KEY + info antet
    col_cui    = opt_col(key, ["CUI","CIF"])
    col_adresa = opt_col(key, ["Adresa","Sediu central","Sediul central"])
    col_rc     = opt_col(key, [
        "Nr. Inregistrare","Nr Inregistrare","Nr. înregistrare","Numar inregistrare","Număr înregistrare",
        "Nr. înregistrare R.C.","Nr. inregistrare R.C.","Nr. Reg. Com.","Nr Reg Com",
        "Nr. înregistrare R.V.","Nr. inregistrare R.V.","Nr. înregistrare RV","Nr inregistrare RV",
        "NR INREGISTRARE R.V.","NR INREGISTRARE RV","NR INREG RV","NR RV","R.V.","RV"
    ])

    cols = [col_tid_key, col_bmc, col_clientkey]
    new_names = ["TID","BMC","Client"]
    if col_cui:    cols.append(col_cui);    new_names.append("CUI")
    if col_adresa: cols.append(col_adresa); new_names.append("ADRESA")
    if col_rc:     cols.append(col_rc);     new_names.append("NR_RC")
    key2 = key[cols].copy(); key2.columns = new_names

    # 5) Join pe TID
    df = astob2.merge(key2, on="TID", how="left")

    # 6) Fișiere per client
    created_files = []
    for client, g in df.groupby("Client", dropna=True):
        client_str = "" if pd.isna(client) else str(client).strip()
        total = float(g["Valoare cu TVA"].sum())
        if total <= 0 or not client_str: continue

        # sortare dată+oră (crescător)
        g = g.sort_values("DT_SORT", kind="mergesort")

        wb = load_workbook(template_path)
        ws = wb.active
        unmerge_all(ws)
        try: ws.row_dimensions[2].height = 25.20
        except Exception: pass

        model_row, colmap = find_model_row(ws)
        model_cells = {c: ws.cell(model_row, c) for c in colmap.values()}
        model_height = ws.row_dimensions[model_row].height or 15

        rows = []
        for _, r in g.iterrows():
            rows.append({
                "denumire site":   r.get("BMC",""),
                "tid":             r.get("TID",""),
                "denumire produs": r.get("Denumire Produs",""),
                "valoare cu tva":  float(r.get("Valoare cu TVA",0.0)),
                "data tranzactiei":r.get("Data Tranzactiei",""),
            })

        if len(rows) > 1:
            ws.insert_rows(model_row+1, amount=len(rows)-1)

        txn_font = Font(name="Calibri", size=14, bold=True)
        for i, rowdata in enumerate(rows):
            ridx = model_row + i
            ws.row_dimensions[ridx].height = model_height
            for token, col in colmap.items():
                cell = ws.cell(ridx, col)
                cell.value = rowdata[token]
                copy_style(model_cells[col], cell)
                cell.font = txn_font

        # TOTAL
        replace_total(ws, total)

        # Antet client
        def first_notna(series, default=""):
            try:
                s = series.dropna()
                return str(s.iloc[0]) if len(s) else default
            except Exception:
                return default

        mapping = {
            "NUME": client_str, "CLIENT": client_str,
            "CUI": first_notna(g.get("CUI",""), ""),
            "ADRESA": first_notna(g.get("ADRESA",""), ""),
            # Nr. Inregistrare / RC / RV
            "NR. INREGISTRARE": first_notna(g.get("NR_RC",""), ""),
            "NR INREGISTRARE":  first_notna(g.get("NR_RC",""), ""),
            "NR. INREGISTRARE R.C.": first_notna(g.get("NR_RC",""), ""),
            "NR INREGISTRARE R.C.":  first_notna(g.get("NR_RC",""), ""),
            "NR INREGISTRARE RC":    first_notna(g.get("NR_RC",""), ""),
            "NR. INREGISTRARE R.V.": first_notna(g.get("NR_RC",""), ""),
            "NR INREGISTRARE R.V.":  first_notna(g.get("NR_RC",""), ""),
            "NR. INREGISTRARE RV":   first_notna(g.get("NR_RC",""), ""),
            "NR INREGISTRARE RV":    first_notna(g.get("NR_RC",""), ""),
            "NR INREG RV":           first_notna(g.get("NR_RC",""), ""),
            "NR RV":                 first_notna(g.get("NR_RC",""), ""),
        }
        replace_placeholders(ws, mapping)

        # === COLECTARI & HEADER_DATE ===
        dmin_dt = g["DT_SORT"].min()
        dmax_dt = g["DT_SORT"].max()
        colectari_str = f"Colectari - {dmin_dt:%d.%m.%Y} - {dmax_dt:%d.%m.%Y}"
        header_date = dmax_dt.date() + timedelta(days=1)    # ziua următoare după ultima tranzacție
        header_str  = ro_month_upper(header_date)
        replace_placeholders(ws, {
            "{COLECTARI}": colectari_str,
            "{HEADER_DATE}": header_str,
        })

        clear_leftover_token_rows(ws, start_row=model_row+len(rows))

        safe_client = re.sub(r'[\\/*?:"<>|]+', "_", client_str).strip() or "Client"
        out_path = out_dir / f"Ordin - {safe_client}.xlsx"
        wb.save(out_path)
        created_files.append(out_path)

    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in created_files:
            zf.write(p, p.name)

if __name__ == "__main__":
    main()
