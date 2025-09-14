# generate_orders.py — variantă cu debug
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import zipfile
from copy import copy
from dataclasses import dataclass
from datetime import datetime, date, time, timezone, timedelta
import unicodedata
from typing import Dict, Iterable, List, Tuple, Optional

import pandas as pd
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.styles.numbers import FORMAT_NUMBER_00

RO_TZ = timezone(timedelta(hours=3))
RO_MONTHS = {
    1: "IANUARIE", 2: "FEBRUARIE", 3: "MARTIE", 4: "APRILIE", 5: "MAI", 6: "IUNIE",
    7: "IULIE", 8: "AUGUST", 9: "SEPTEMBRIE", 10: "OCTOMBRIE", 11: "NOIEMBRIE", 12: "DECEMBRIE"
}

def today_ro() -> date:
    return datetime.utcnow().astimezone(RO_TZ).date()

def ro_header_date(d: date) -> str:
    return f"{d.day} {RO_MONTHS[d.month]} {d.year}"

def strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = (s.replace("Ş", "Ș").replace("Ţ", "Ț").replace("ş", "ș").replace("ţ", "ț"))
    nf = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in nf if not unicodedata.combining(ch))

def norm(s: str) -> str:
    if s is None:
        return ""
    s = strip_accents(str(s))
    s = s.replace(".", " ").replace(",", " ").replace("/", " ")
    s = " ".join(s.upper().split())
    return s

def find_col_name(df: pd.DataFrame, candidates: Iterable[str]) -> str:
    cols = {norm(c): c for c in df.columns}
    # match exact normalizat
    for cand in candidates:
        nc = norm(cand)
        if nc in cols:
            return cols[nc]
    # match conține (în ambele sensuri)
    for nc, orig in cols.items():
        for cand in candidates:
            cc = norm(cand)
            if cc in nc or nc in cc:
                return orig
    raise KeyError(f"Missing columns: tried {list(candidates)} in {list(df.columns)}")

def to_float(x) -> float:
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    s = s.replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def combine_datetime(dcol, tcol) -> Optional[datetime]:
    # acceptă lipsa orei (folosim 00:00:00)
    if pd.isna(dcol):
        return None
    try:
        d = pd.to_datetime(dcol, dayfirst=True, errors="coerce")
        if pd.isna(d):
            return None
        d = d.to_pydatetime()
        if not pd.isna(tcol):
            try:
                tt = pd.to_datetime(str(tcol), errors="coerce").time()
            except Exception:
                tt = time(0, 0, 0)
        else:
            tt = time(0, 0, 0)
        return datetime(d.year, d.month, d.day, tt.hour, tt.minute, tt.second)
    except Exception:
        return None

@dataclass
class KeyRow:
    tid: str
    site: str
    nume: str
    nr_rc: str
    cui: str
    adresa: str

@dataclass
class TxRow:
    tid: str
    operator: str
    valoare: float
    when: datetime

# mapări extinse
KEY_MAP = {
    "tid":   ["TID", "NR. TERMINAL", "NR TERMINAL"],
    "site":  ["DENUMIRE SITE", "DENUMIRE SOCIETATEAGENT", "DENUMIRE SOCIETATE AGENT", "SITE"],
    "nume":  ["NUME", "CLIENT", "DENUMIRE CLIENT", "DENUMIRE"],
    "nr_rc": ["NR. INREGISTRARE R.C.", "NR INREGISTRARE RC", "NR INREGISTRARE", "NR. INREGISTRARE"],
    "cui":   ["CUI", "CUI CNP"],
    "adresa":["ADRESA", "SEDIUL CENTRAL", "ADRESA SEDIUL CENTRAL"],
}

ASTOB_MAP = {
    "tid":     ["NR. TERMINAL", "NR TERMINAL", "TID"],
    "operator":["NUME OPERATOR", "NUME COMERCIANT", "COMERCIANT", "DENUMIRE PRODUS"],
    "valoare": ["SUMA TRANZACTIEI", "SUMA TRANZACTIE", "SUMA", "VALOARE CU TVA", "VALOARE TRANZACTIE"],
    "data":    ["DATA TRANZACTIEI", "DATA"],
    "ora":     ["ORA TRANZACTIEI", "ORA"],
}

def read_key(path: str) -> List[KeyRow]:
    key = pd.read_excel(path, engine="openpyxl")
    col_tid   = find_col_name(key, KEY_MAP["tid"])
    col_site  = find_col_name(key, KEY_MAP["site"])
    col_nume  = None
    try:
        col_nume = find_col_name(key, KEY_MAP["nume"])
    except KeyError:
        col_nume = col_site
    col_nrrc  = find_col_name(key, KEY_MAP["nr_rc"])
    col_cui   = find_col_name(key, KEY_MAP["cui"])
    col_adr   = find_col_name(key, KEY_MAP["adresa"])

    rows: List[KeyRow] = []
    for _, r in key.iterrows():
        tid = str(r[col_tid]).strip()
        if tid == "" or tid.lower() == "nan":
            continue
        rows.append(KeyRow(
            tid=tid,
            site=str(r[col_site]).strip(),
            nume=str(r[col_nume]).strip(),
            nr_rc=str(r[col_nrrc]).strip(),
            cui=str(r[col_cui]).strip(),
            adresa=str(r[col_adr]).strip(),
        ))
    print(f"[debug] key: rows={len(rows)}, cols: tid='{col_tid}', site='{col_site}', nume='{col_nume}', rc='{col_nrrc}', cui='{col_cui}', adresa='{col_adr}'")
    return rows

def read_astob(path: str) -> List[TxRow]:
    df = pd.read_excel(path, engine="openpyxl")
    col_tid  = find_col_name(df, ASTOB_MAP["tid"])
    col_op   = find_col_name(df, ASTOB_MAP["operator"])
    col_val  = find_col_name(df, ASTOB_MAP["valoare"])
    col_data = find_col_name(df, ASTOB_MAP["data"])
    col_ora  = None
    try:
        col_ora = find_col_name(df, ASTOB_MAP["ora"])
    except KeyError:
        col_ora = None

    rows: List[TxRow] = []
    for _, r in df.iterrows():
        tid = str(r[col_tid]).strip()
        if tid == "" or tid.lower() == "nan":
            continue
        op = str(r[col_op]).strip()
        val = to_float(r[col_val])
        when = combine_datetime(r[col_data], r[col_ora] if col_ora else None)
        if when is None:
            # dacă data e invalidă, sărim rândul
            continue
        rows.append(TxRow(tid=tid, operator=op, valoare=val, when=when))

    rows.sort(key=lambda x: x.when)
    print(f"[debug] astob: rows_in={len(df)}, rows_ok={len(rows)}, cols: tid='{col_tid}', operator='{col_op}', valoare='{col_val}', data='{col_data}', ora='{col_ora}'")
    return rows

@dataclass
class Placeholders:
    header_date: Tuple[int, int]
    colectari: Tuple[int, int]
    nume: Tuple[int, int]
    nrrc: Tuple[int, int]
    cui: Tuple[int, int]
    adresa: Tuple[int, int]
    head_site: Tuple[int, int]
    head_tid: Tuple[int, int]
    head_prod: Tuple[int, int]
    head_val: Tuple[int, int]
    head_data: Tuple[int, int]
    total_label: Tuple[int, int]

def find_cell(ws: Worksheet, text: str) -> Tuple[int, int]:
    for r in ws.iter_rows(values_only=False):
        for c in r:
            if str(c.value).strip() == text:
                return c.row, c.column
    raise KeyError(f'Placeholder "{text}" not found in sheet "{ws.title}".')

def read_styles(ws: Worksheet, row_idx: int, cols: List[int]):
    styles = {}
    for col in cols:
        c = ws.cell(row=row_idx, column=col)
        styles[col] = {
            "font": copy(c.font), "border": copy(c.border), "fill": copy(c.fill),
            "numfmt": c.number_format, "protect": copy(c.protection), "align": copy(c.alignment),
        }
    height = ws.row_dimensions[row_idx].height
    return styles, height

def apply_row(ws: Worksheet, row_idx: int, styles, height, numfmt_override: Dict[int, str] | None = None):
    numfmt_override = numfmt_override or {}
    for col, st in styles.items():
        c = ws.cell(row=row_idx, column=col)
        c.font = copy(st["font"])
        c.border = copy(st["border"])
        c.fill = copy(st["fill"])
        c.number_format = numfmt_override.get(col, st["numfmt"])
        c.protection = copy(st["protect"])
        c.alignment = copy(st["align"])
    if height is not None:
        ws.row_dimensions[row_idx].height = height

def build_workbook(template_path: str, client: KeyRow, items: List[TxRow], colectari_str: str) -> Tuple[str, bytes]:
    wb = load_workbook(template_path)
    ws = wb.active

    ph = Placeholders(
        header_date = find_cell(ws, "{HEADER_DATE}"),
        colectari   = find_cell(ws, "{COLECTARI}"),
        nume        = find_cell(ws, "{NUME}"),
        nrrc        = find_cell(ws, "{NR. INREGISTRARE R.C.}"),
        cui         = find_cell(ws, "{CUI}"),
        adresa      = find_cell(ws, "{ADRESA}"),
        head_site   = find_cell(ws, "{DENUMIRE SITE}"),
        head_tid    = find_cell(ws, "{TID}"),
        head_prod   = find_cell(ws, "{DENUMIRE PRODUS}"),
        head_val    = find_cell(ws, "{VALOARE CU TVA}"),
        head_data   = find_cell(ws, "{DATA TRANZACTIEI}"),
        total_label = find_cell(ws, "{TOTAL}"),
    )

    ws.cell(*ph.header_date).value = ro_header_date(today_ro())
    ws.cell(*ph.colectari).value   = f"Colectari - {colectari_str}"
    ws.cell(*ph.nume).value        = client.nume
    ws.cell(*ph.nrrc).value        = client.nr_rc
    ws.cell(*ph.cui).value         = client.cui
    ws.cell(*ph.adresa).value      = client.adresa

    ws.cell(*ph.head_site).value = "Denumire Site"
    ws.cell(*ph.head_tid ).value = "TID"
    ws.cell(*ph.head_prod).value = "Denumire Produs"
    ws.cell(*ph.head_val ).value = "Valoare cu TVA"
    ws.cell(*ph.head_data).value = "Data Tranzactiei"
    ws.cell(*ph.total_label).value = "Total"

    c_site = ws.cell(*ph.head_site); c_tid = ws.cell(*ph.head_tid)
    c_prod = ws.cell(*ph.head_prod); c_val = ws.cell(*ph.head_val)
    c_dat  = ws.cell(*ph.head_data); c_tot = ws.cell(*ph.total_label)

    row_model = c_site.row + 1
    data_styles, data_height = read_styles(ws, row_model, [c_site.column, c_tid.column, c_prod.column, c_val.column, c_dat.column])
    total_styles, total_height = read_styles(ws, c_tot.row, [c_tot.column])

    num_override = { c_val.column: FORMAT_NUMBER_00, c_dat.column: "yyyy-mm-dd hh:mm:ss" }

    r = row_model
    for idx, it in enumerate(items):
        if idx > 0:
            ws.insert_rows(r)
        apply_row(ws, r, data_styles, data_height, num_override)
        ws.cell(r, c_site.column, value=client.site)
        ws.cell(r, c_tid.column,  value=str(it.tid))
        ws.cell(r, c_prod.column, value=it.operator)
        vcell = ws.cell(r, c_val.column); vcell.value = round(float(it.valoare or 0.0), 2)
        dcell = ws.cell(r, c_dat.column); dcell.value = it.when
        r += 1

    tot_row = c_tot.row + (len(items) - 1 if len(items) > 1 else 0)
    if len(items) > 1:
        ws.insert_rows(tot_row)
    apply_row(ws, tot_row, total_styles, total_height, {c_tot.column: FORMAT_NUMBER_00})
    total_value = round(sum(x.valoare for x in items), 2)
    ws.cell(tot_row, c_tot.column).value = total_value

    mem = io.BytesIO(); wb.save(mem)
    fname = f"Ordin - {client.nume}.xlsx"
    return fname, mem.getvalue()

def generate_zip(astob_path: str, key_path: str, template_path: str) -> bytes:
    key_rows = read_key(key_path)
    ast_rows = read_astob(astob_path)

    key_by_tid: Dict[str, KeyRow] = {str(kr.tid): kr for kr in key_rows}
    ast_tids = {str(tx.tid) for tx in ast_rows}
    key_tids = set(key_by_tid.keys())
    matched_tids = ast_tids & key_tids
    print(f"[debug] tids: ast={len(ast_tids)}, key={len(key_tids)}, matched={len(matched_tids)}")

    grouped: Dict[str, List[TxRow]] = {}
    client_info: Dict[str, KeyRow] = {}

    for tx in ast_rows:
        kr = key_by_tid.get(str(tx.tid))
        if not kr:
            continue
        cid = kr.nume
        grouped.setdefault(cid, []).append(tx)
        if cid not in client_info:
            client_info[cid] = kr

    all_dt = [tx.when for tx in ast_rows]
    if all_dt:
        start = min(all_dt).date(); end = max(all_dt).date()
        colectari_str = f"{start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}"
    else:
        colectari_str = ""

    written = 0
    zip_mem = io.BytesIO()
    with zipfile.ZipFile(zip_mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for client, items in grouped.items():
            total_client = round(sum(x.valoare for x in items), 2)
            if total_client <= 0:
                continue
            fname, data = build_workbook(template_path, client_info[client], items, colectari_str)
            zf.writestr(fname, data)
            written += 1

    print(f"[debug] clients_total={len(grouped)}, clients_written={written}")
    if written == 0:
        # dăm explicație utilă
        ex = ""
        if len(matched_tids) == 0:
            ex = "Niciun TID din ASTOB nu se potrivește cu TABEL CHEIE (verifică coloana TID în ambele fișiere)."
        elif all(round(to_float(tx.valoare), 2) <= 0 for tx in ast_rows):
            ex = "Toate valorile au ieșit 0 (verifică denumirea coloanei de sumă și formatul numeric)."
        elif not all_dt:
            ex = "Nicio dată validă (verifică 'Data tranzacției')."
        else:
            ex = "După filtrarea totalului > 0 nu a rămas niciun client."
        raise ValueError(f"ZIP gol. {ex}")
    return zip_mem.getvalue()

def generate_zip_from_bytes(astob_bytes: bytes, key_bytes: bytes, template_path: str) -> bytes:
    import tempfile, os
    with tempfile.TemporaryDirectory() as td:
        ap = os.path.join(td, "astob.xlsx")
        kp = os.path.join(td, "key.xlsx")
        with open(ap, "wb") as f: f.write(astob_bytes)
        with open(kp, "wb") as f: f.write(key_bytes)
        return generate_zip(ap, kp, template_path)

def main():
    import argparse, os
    p = argparse.ArgumentParser()
    p.add_argument("--astob", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--template", required=True)
    p.add_argument("--out-dir", default="out_excel")
    p.add_argument("--out-zip", default="ordine.zip")
    args = p.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    data = generate_zip(args.astob, args.key, args.template)
    out = os.path.join(args.out_dir, os.path.basename(args.out_zip))
    with open(out, "wb") as f:
        f.write(data)
    print(f"[ok] scris: {out}")

if __name__ == "__main__":
    main()
