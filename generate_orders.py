from __future__ import annotations
import zipfile, re
from io import BytesIO
from datetime import datetime, date
from typing import Dict, List, Tuple
from copy import copy
from zoneinfo import ZoneInfo

import pandas as pd
from unidecode import unidecode
from openpyxl import load_workbook
from openpyxl.styles import Alignment

# ---------------- utils ----------------
RO_MONTHS = {1:"IANUARIE",2:"FEBRUARIE",3:"MARTIE",4:"APRILIE",5:"MAI",6:"IUNIE",7:"IULIE",8:"AUGUST",9:"SEPTEMBRIE",10:"OCTOMBRIE",11:"NOIEMBRIE",12:"DECEMBRIE"}

def today_ro(d: date | None = None) -> str:
    d = d or date.today()
    return f"{d.day} {RO_MONTHS[d.month]} {d.year}"

def today_ro_bucharest() -> str:
    # data curentă în Europe/Bucharest
    now = datetime.now(ZoneInfo("Europe/Bucharest")).date()
    return today_ro(now)

def norm(s: str) -> str:
    s = unidecode(str(s)).lower().replace("\u00A0"," ")
    s = re.sub(r"[^a-z0-9 ]+"," ", s)
    return re.sub(r"\s+"," ", s).strip()

def find_col(df: pd.DataFrame, candidates: List[str]) -> str:
    cmap = {norm(c): c for c in df.columns}
    vars_ = [norm(c) for c in candidates]
    for v in vars_:
        if v in cmap: return cmap[v]
    for v in vars_:
        hits = [orig for k,orig in cmap.items() if v in k]
        if len(hits)==1: return hits[0]
    raise KeyError(f"Missing columns: tried {candidates} in {list(df.columns)}")

def read_table_from_bytes(data: bytes) -> pd.DataFrame:
    try:
        return pd.read_excel(BytesIO(data), engine="openpyxl")
    except Exception:
        pass
    for enc in ("utf-8","cp1250","cp1252","latin-1"):
        try:
            return pd.read_csv(BytesIO(data), sep=None, engine="python", encoding=enc)
        except Exception:
            continue
    return pd.read_csv(BytesIO(data), engine="python")

def to_float(x) -> float:
    if pd.isna(x): return 0.0
    if isinstance(x,(int,float)): return float(x)
    s = str(x).strip()
    if s.count(",")==1: s = s.replace(".","").replace(",",".")
    else: s = s.replace(",",".")
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
    return re.sub(r"\s+"," ", s).strip()

def first_cell(ws, needle: str):
    for row in ws.iter_rows():
        for c in row:
            if isinstance(c.value,str) and needle in c.value:
                return c
    return None

def snapshot_row(ws, row_idx: int):
    styles = {}; height = ws.row_dimensions[row_idx].height
    for col in range(1, ws.max_column + 1):
        cell = ws.cell(row=row_idx, column=col)
        styles[col] = {
            "font":    copy(cell.font),
            "border":  copy(cell.border),
            "fill":    copy(cell.fill),
            "numfmt":  cell.number_format,
            "protect": copy(cell.protection),
            "align":   copy(cell.alignment),
        }
    return styles, height

def apply_row(ws, row_idx: int, styles, height):
    for col, st in styles.items():
        c = ws.cell(row=row_idx, column=col)
        c.font          = copy(st["font"])
        c.border        = copy(st["border"])
        c.fill          = copy(st["fill"])
        c.number_format = st["numfmt"]
        c.protection    = copy(st["protect"])
        c.alignment     = copy(st["align"])
    if height is not None:
        ws.row_dimensions[row_idx].height = height

def replace_all(ws, mapping: Dict[str,str]):
    for row in ws.iter_rows():
        for c in row:
            if isinstance(c.value,str):
                v = c.value; changed = False
                for k,val in mapping.items():
                    if k in v: v = v.replace(k, val); changed = True
                if changed: c.value = v

# -------------- core --------------
def generate_zip_from_bytes(astob_bytes: bytes, key_bytes: bytes, template_path: str) -> bytes:
    # citește tabele tolerant
    ast = read_table_from_bytes(astob_bytes)
    key = read_table_from_bytes(key_bytes)

    # ASTOB
    col_tid_ast  = find_col(ast, ["Nr. terminal","TID"])
    col_sum_ast  = find_col(ast, ["Sumă tranzacție","Suma tranzactie","Valoare cu TVA"])
    # denumire produs = prefer Nume Operator (cerința ta)
    col_prod_ast = find_col(ast, ["Nume Operator","Nume operator","Operator","Denumire Produs","Nume Comerciant","Comerciant"])
    col_date_ast = find_col(ast, ["Data tranzacției","Data tranzactiei","Data"])
    try:
        col_time_ast = find_col(ast, ["Ora tranzacției","Ora tranzactiei","Ora"])
    except:
        col_time_ast = None

    # KEY (site = BMC)
    col_tid_key  = find_col(key, ["TID","Nr. terminal"])
    col_name_key = find_col(key, ["DENUMIRE SOCIETATEAGENT","Denumire Societate Agent","NUME","Client"])
    col_rc_key   = find_col(key, ["NR INREGISTRARE","Nr. inregistrare R.C.","NR. INREGISTRARE R.C.","Nr inregistrare RC","NR INREG"])
    col_cui_key  = find_col(key, ["CUI","CUI CNP","CNP"])
    col_addr_key = find_col(key, ["ADRESA","Sediul central","Adresă"])
    col_site_key = find_col(key, ["BMC","DENUMIRE SITE","Denumire Site","Site","Punct de lucru","Locatie"])

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

    # map TID -> info (site din cheie BMC)
    tid2info = {
        r["_TID"]: {"client": r["_NAME"], "rc": r["_RC"], "cui": r["_CUI"], "adr": r["_ADR"], "site": r["_SITE"]}
        for _,r in key.iterrows()
    }

    # păstrăm doar tranzacții cu TID recunoscut
    ast = ast[ast["_TID"].isin(tid2info.keys())].copy()
    if ast.empty:
        raise RuntimeError("Nu s-au găsit TID-uri comune între ASTOB și TABEL CHEIE.")

    # grupăm pe client
    rows_by_client: Dict[str, List[Tuple[str,str,str,float,datetime]]] = {}
    for _, r in ast.iterrows():
        info = tid2info.get(r["_TID"])
        if not info: continue
        rows_by_client.setdefault(info["client"], []).append((
            info["site"],           # DENUMIRE SITE din BMC
            r["_TID"],
            r["_PROD"],             # <- acum e Nume Operator prioritar
            float(r["_VAL"] or 0.0),
            r["_DT"],
        ))

    # template
    wb0 = load_workbook(template_path); ws0 = wb0.active
    c_site = first_cell(ws0, "{DENUMIRE SITE}")
    c_tid  = first_cell(ws0, "{TID}")
    c_prod = first_cell(ws0, "{DENUMIRE PRODUS}")
    c_val  = first_cell(ws0, "{VALOARE CU TVA}")
    c_dat  = first_cell(ws0, "{DATA TRANZACTIEI}")
    c_tot  = first_cell(ws0, "{TOTAL}")
    if not all([c_site, c_tid, c_prod, c_val, c_dat, c_tot]):
        raise RuntimeError("Nu găsesc placeholder-ele de tabel în șablon.")
    row_model = c_site.row
    data_styles, data_height   = snapshot_row(ws0, row_model)
    total_styles, total_height = snapshot_row(ws0, c_tot.row)

    out_zip = BytesIO()
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for client, items in rows_by_client.items():
            items = [t for t in items if t[3] > 0.0]
            if not items: continue
            items.sort(key=lambda x: x[4])

            total_client = round(sum(v for *_, v, _ in items), 2)
            if total_client <= 0.0: continue

            dmin = min(dt.date() for *_, dt in items)
            dmax = max(dt.date() for *_, dt in items)
            colectari = f"Colectari - {dmin:%d.%m.%Y} - {dmax:%d.%m.%Y}"

            any_tid = items[0][1]
            info = tid2info.get(any_tid, {"rc":"","cui":"","adr":""})

            wb = load_workbook(template_path); ws = wb.active

            replace_all(ws, {
                "{HEADER_DATE}": today_ro_bucharest(),  # <- data de azi în RO
                "{COLECTARI}": colectari,
                "{NUME}": client,
                "{NR. INREGISTRARE R.C.}": info.get("rc",""),
                "{CUI}": info.get("cui",""),
                "{ADRESA}": info.get("adr",""),
                "{DENUMIRE SITE}": "Denumire Site",
                "{TID}": "TID",
                "{DENUMIRE PRODUS}": "Denumire Produs",
                "{VALOARE CU TVA}": "Valoare cu TVA",
                "{DATA TRANZACTIEI}": "Data Tranzactiei",
                "{TOTAL}": "Total",
            })

            # scriere rânduri cu stilul rândului-model
            r = row_model
            for idx, (site, tid, prod, val, dt) in enumerate(items):
                if idx > 0: ws.insert_rows(r)
                apply_row(ws, r, data_styles, data_height)

                ws.cell(r, c_site.column, value=site)
                ws.cell(r, c_tid.column,  value=tid)
                ws.cell(r, c_prod.column, value=prod)

                # valoare numerică, fără zero-padding
                vcell = ws.cell(r, c_val.column, value=None)
                vcell.number_format = "General"      # curăță orice format moștenit
                vcell.value = round(float(val), 2)   # numeric
                vcell.number_format = "0,00"         # format RO
                vcell.alignment = Alignment(horizontal="right", vertical="center")

                dcell = ws.cell(r, c_dat.column, value=dt)
                dcell.number_format = "yyyy-mm-dd hh:mm:ss"

                r += 1

            # total – pe rândul placeholder {TOTAL} mutat după inserări
            tot_row = c_tot.row + (len(items)-1)
            apply_row(ws, tot_row, total_styles, total_height)

            tcell = ws.cell(tot_row, c_tot.column, value=None)
            tcell.number_format = "General"
            tcell.value = round(float(total_client), 2)
            tcell.number_format = "0,00"
            tcell.alignment = Alignment(horizontal="right", vertical="center")

            bio = BytesIO(); wb.save(bio); bio.seek(0)
            zf.writestr(f"Ordin - {safe_name(client)}.xlsx", bio.read())

    out_zip.seek(0); return out_zip.read()
