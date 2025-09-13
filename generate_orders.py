# generate_orders.py
from __future__ import annotations
import zipfile, re, unicodedata
from io import BytesIO
from datetime import datetime, date
from typing import Dict, List, Tuple

import pandas as pd
from unidecode import unidecode
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment

# ---------------- utils ----------------
RO_MONTHS = {1:"IANUARIE",2:"FEBRUARIE",3:"MARTIE",4:"APRILIE",5:"MAI",6:"IUNIE",7:"IULIE",8:"AUGUST",9:"SEPTEMBRIE",10:"OCTOMBRIE",11:"NOIEMBRIE",12:"DECEMBRIE"}
def today_ro(d: date | None = None) -> str:
    d = d or date.today()
    return f"{d.day} {RO_MONTHS[d.month]} {d.year}"

def norm(s: str) -> str:
    s = unidecode(str(s)).lower()
    s = s.replace("\u00A0"," ")
    s = re.sub(r"[^a-z0-9 ]+"," ", s)
    return re.sub(r"\s+"," ", s).strip()

def find_col(df: pd.DataFrame, candidates: List[str]) -> str:
    cmap = {norm(c): c for c in df.columns}
    vars = [norm(c) for c in candidates]
    # exact
    for v in vars:
        if v in cmap: return cmap[v]
    # conține (unic)
    for v in vars:
        hits = [orig for k,orig in cmap.items() if v in k]
        if len(hits)==1: return hits[0]
    raise KeyError(f"Missing columns: tried {candidates} in {list(df.columns)}")

def read_table_from_bytes(data: bytes) -> pd.DataFrame:
    # încearcă Excel
    try:
        return pd.read_excel(BytesIO(data), engine="openpyxl")
    except Exception:
        pass
    # încearcă CSV (diverse encodări/separatori)
    for enc in ("utf-8","cp1250","cp1252","latin-1"):
        try:
            return pd.read_csv(BytesIO(data), sep=None, engine="python", encoding=enc)
        except Exception:
            continue
    # last resort
    return pd.read_csv(BytesIO(data), engine="python")

def to_float(x) -> float:
    if pd.isna(x): return 0.0
    if isinstance(x,(int,float)): return float(x)
    s = str(x).strip()
    # 1.234,56 -> 1234.56
    if s.count(",")==1:
        s = s.replace(".","").replace(",",".")
    else:
        s = s.replace(",",".")
    try: return float(s)
    except: return 0.0

def combine_dt(dv, tv) -> datetime | None:
    if pd.isna(dv): return None
    try:
        d = pd.to_datetime(str(dv), dayfirst=True)
    except: return None
    if tv is not None and not pd.isna(tv) and str(tv).strip():
        try:
            t = pd.to_datetime(str(tv)).time()
            return datetime.combine(d.date(), t)
        except: return d.to_pydatetime()
    return d.to_pydatetime()

def safe_name(s: str) -> str:
    s = re.sub(r'[\\/:*?"<>|]', "_", str(s))
    s = re.sub(r"\s+"," ", s).strip()
    return s

def first_cell(ws, needle: str):
    for row in ws.iter_rows():
        for c in row:
            if isinstance(c.value,str) and needle in c.value:
                return c
    return None

def snapshot_row(ws, row_idx: int):
    styles = {}
    height = ws.row_dimensions[row_idx].height
    for col in range(1, ws.max_column+1):
        cell = ws.cell(row=row_idx, column=col)
        styles[col] = (cell.font, cell.border, cell.fill, cell.number_format, cell.protection, cell.alignment)
    return styles, height

def apply_row(ws, row_idx: int, styles, height):
    for col, st in styles.items():
        c = ws.cell(row=row_idx, column=col)
        c.font, c.border, c.fill, c.number_format, c.protection, c.alignment = st
    if height is not None:
        ws.row_dimensions[row_idx].height = height

def replace_all(ws, mapping: Dict[str,str]):
    for row in ws.iter_rows():
        for c in row:
            if isinstance(c.value,str):
                v = c.value
                for k,val in mapping.items():
                    if k in v: v = v.replace(k, val)
                if v != c.value: c.value = v

# -------------- core --------------
def generate_zip_from_bytes(astob_bytes: bytes, key_bytes: bytes, template_path: str) -> bytes:
    # citește tabele tolerant (xlsx/csv)
    ast = read_table_from_bytes(astob_bytes)
    key = read_table_from_bytes(key_bytes)

    # ASTOB: coloane
    col_tid_ast  = find_col(ast, ["Nr. terminal","TID"])
    col_sum_ast  = find_col(ast, ["Sumă tranzacție","Suma tranzactie","Valoare cu TVA"])
    col_prod_ast = find_col(ast, ["Nume Comerciant","Comerciant","Denumire Produs"])
    col_date_ast = find_col(ast, ["Data tranzacției","Data tranzactiei","Data"])
    col_time_ast = None
    try:
        col_time_ast = find_col(ast, ["Ora tranzacției","Ora tranzactiei","Ora"])
    except: pass

    # KEY: coloane (nume client, site din cheie!)
    col_tid_key  = find_col(key, ["TID","Nr. terminal"])
    col_name_key = find_col(key, ["DENUMIRE SOCIETATEAGENT","Denumire Societate Agent","NUME","Client"])
    col_rc_key   = find_col(key, ["NR. INREGISTRARE R.C.","Nr. inregistrare R.C.","NR INREG"])
    col_cui_key  = find_col(key, ["CUI"])
    col_addr_key = find_col(key, ["ADRESA","Sediul central","Adresă"])
    col_site_key = find_col(key, ["DENUMIRE SITE","Denumire Site","Site"])

    # normalize
    ast["_TID"]  = ast[col_tid_ast].astype(str).str.replace(r"\.0$","",regex=True).str.strip()
    ast["_VAL"]  = ast[col_sum_ast].map(to_float)
    ast["_PROD"] = ast[col_prod_ast].astype(str).str.strip()
    if col_time_ast:
        ast["_DT"] = [combine_dt(d, ast[col_time_ast].iloc[i]) for i,d in enumerate(ast[col_date_ast])]
    else:
        ast["_DT"] = [combine_dt(d, None) for d in ast[col_date_ast]]
    ast = ast.dropna(subset=["_TID","_DT"])

    key["_TID"]  = key[col_tid_key].astype(str).str.replace(r"\.0$","",regex=True).str.strip()
    key["_NAME"] = key[col_name_key].astype(str).str.strip()
    key["_RC"]   = key[col_rc_key].astype(str).str.strip()
    key["_CUI"]  = key[col_cui_key].astype(str).str.strip()
    key["_ADR"]  = key[col_addr_key].astype(str).str.strip()
    key["_SITE"] = key[col_site_key].astype(str).str.strip()

    # map TID -> info (site din KEY, cum ai cerut)
    tid2info = { r["_TID"]: {"client": r["_NAME"], "rc": r["_RC"], "cui": r["_CUI"], "adr": r["_ADR"], "site": r["_SITE"]} for _,r in key.iterrows() }

    # păstrăm doar tranzacții cu TID recunoscut
    ast = ast[ast["_TID"].isin(tid2info.keys())].copy()
    if ast.empty:
        raise RuntimeError("Nu s-au găsit TID-uri comune între ASTOB și TABEL CHEIE.")

    # grupăm pe client
    rows_by_client: Dict[str, List[Tuple[str,str,str,float,datetime]]] = {}
    for _, r in ast.iterrows():
        info = tid2info.get(r["_TID"])
        if not info: continue
        site = info["site"]                # din CHEIE
        tid  = r["_TID"]
        prod = r["_PROD"]
        val  = float(r["_VAL"] or 0.0)
        dt   = r["_DT"]
        rows_by_client.setdefault(info["client"], []).append((site, tid, prod, val, dt))

    # șablon
    wb0 = load_workbook(template_path)
    ws0 = wb0.active
    # poziții placeholder tabel
    c_site = first_cell(ws0, "{DENUMIRE SITE}")
    c_tid  = first_cell(ws0, "{TID}")
    c_prod = first_cell(ws0, "{DENUMIRE PRODUS}")
    c_val  = first_cell(ws0, "{VALOARE CU TVA}")
    c_dat  = first_cell(ws0, "{DATA TRANZACTIEI}")
    c_tot  = first_cell(ws0, "{TOTAL}")
    if not all([c_site, c_tid, c_prod, c_val, c_dat, c_tot]):
        raise RuntimeError("Nu găsesc placeholder-ele de tabel în șablon.")
    row_model = c_site.row
    data_styles, data_height = snapshot_row(ws0, row_model)
    total_styles, total_height = snapshot_row(ws0, c_tot.row)

    out_zip = BytesIO()
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for client, items in rows_by_client.items():
            # filtrează 0 și sortează după dată+oră
            items = [t for t in items if t[3] > 0.0]
            if not items: continue
            items.sort(key=lambda x: x[4])  # dt

            total_client = round(sum(v for *_, v, _ in items), 2)
            if total_client <= 0.0:
                continue

            dmin = min(dt.date() for *_, dt in items)
            dmax = max(dt.date() for *_, dt in items)
            colectari = f"Colectari - {dmin:%d.%m.%Y} - {dmax:%d.%m.%Y}"

            wb = load_workbook(template_path)
            ws = wb.active

            # antet + date client
            # scoatem datele client din primul TID al clientului
            any_tid = None
            for _,t,_,_,_ in items:
                any_tid = t; break
            info = tid2info.get(any_tid, {"rc":"","cui":"","adr":""})

            replace_all(ws, {
                "{HEADER_DATE}": today_ro(),
                "{COLECTARI}": colectari,
                "{NUME}": client,
                "{NR. INREGISTRARE R.C.}": info.get("rc",""),
                "{CUI}": info.get("cui",""),
                "{ADRESA}": info.get("adr",""),
            })

            # scrie rândul model și apoi inserează restul, aplicând stilul model
            r = row_model
            for idx, (site, tid, prod, val, dt) in enumerate(items):
                if idx > 0:
                    ws.insert_rows(r)
                apply_row(ws, r, data_styles, data_height)
                ws.cell(r, c_site.column, value=site)
                ws.cell(r, c_tid.column,  value=tid)
                ws.cell(r, c_prod.column, value=prod)
                c = ws.cell(r, c_val.column, value=float(val))
                c.number_format = "0,00"
                dcell = ws.cell(r, c_dat.column, value=dt)
                dcell.number_format = "yyyy-mm-dd hh:mm:ss"
                r += 1

            # total pe locul placeholder-ului {TOTAL} mutat după inserări
            tot_row = c_tot.row + (len(items)-1)
            apply_row(ws, tot_row, total_styles, total_height)
            ws.cell(tot_row, c_tot.column, value=float(total_client)).number_format = "0,00"

            # curăț placeholder-ele de tabel dacă au rămas în template (opțional; nu umblăm la antete/coloane/lățimi)
            # (nu schimbăm lățimi/merge – le păstrăm exact ca în șablon)

            bio = BytesIO()
            wb.save(bio); bio.seek(0)
            zf.writestr(f"Ordin - {safe_name(client)}.xlsx", bio.read())

    out_zip.seek(0)
    return out_zip.read()
