# -*- coding: utf-8 -*-
from __future__ import annotations

import io, zipfile, unicodedata
from dataclasses import dataclass
from datetime import datetime, date, time, timezone, timedelta
from typing import Dict, Iterable, List, Tuple, Optional

import pandas as pd
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.styles.numbers import FORMAT_NUMBER_00

# ----- utilități de dată/format -----

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

# ----- citire coloane flexibile -----

def find_col_name(df: pd.DataFrame, candidates: Iterable[str]) -> str:
    cols = {norm(c): c for c in df.columns}
    for cand in candidates:
        nc = norm(cand)
        if nc in cols:
            return cols[nc]
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
    s = str(x).strip().replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def combine_datetime(dcol, tcol) -> Optional[datetime]:
    if pd.isna(dcol):
        return None
    d = pd.to_datetime(dcol, dayfirst=True, errors="coerce")
    if pd.isna(d):
        return None
    d = d.to_pydatetime()
    if pd.isna(tcol):
        tt = time(0, 0, 0)
    else:
        tt = pd.to_datetime(str(tcol), errors="coerce")
        tt = tt.time() if not pd.isna(tt) else time(0, 0, 0)
    return datetime(d.year, d.month, d.day, tt.hour, tt.minute, tt.second)

# ----- modele de rânduri -----

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

# mapări extinse (sinonime)
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

# ----- citirea fișierelor -----

def read_key(path: str) -> List[KeyRow]:
    key = pd.read_excel(path, engine="openpyxl")

    col_tid   = find_col_name(key, KEY_MAP["tid"])
    col_site  = find_col_name(key, KEY_MAP["site"])
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
        if tid and tid.lower() != "nan":
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
    try:
        col_ora  = find_col_name(df, ASTOB_MAP["ora"])
    except KeyError:
        col_ora  = None

    rows: List[TxRow] = []
    for _, r in df.iterrows():
        tid = str(r[col_tid]).strip()
        if not tid or tid.lower() == "nan":
            continue
        op = str(r[col_op]).strip()
        val = to_float(r[col_val])
        when = combine_datetime(r[col_data], r[col_ora] if col_ora else None)
        if when is None:
            continue
        rows.append(TxRow(tid=tid, operator=op, valoare=val, when=when))

    rows.sort(key=lambda x: x.when)
    print(f"[debug] astob: rows_in={len(df)}, rows_ok={len(rows)}, cols: tid='{col_tid}', operator='{col_op}', valoare='{col_val}', data='{col_data}', ora='{col_ora}'")
    return rows

# ----- lucru cu placeholder-e în șablon -----

def find_cell_with_token(ws: Worksheet, token: str) -> Tuple[int, int, str]:
    """Caută o celulă care conține token-ul (ca substring)."""
    for r in ws.iter_rows(values_only=False):
        for c in r:
            val = "" if c.value is None else str(c.value)
            if token in val:
                return c.row, c.column, val
    raise KeyError(f'Placeholder "{token}" not found in sheet "{ws.title}".')

def replace_token(ws: Worksheet, row: int, col: int, original_text: str, token: str, replacement: str) -> None:
    ws.cell(row=row, column=col).value = original_text.replace(token, replacement)

def read_row_style(ws: Worksheet, row_idx: int, cols: List[int]):
    styles = {}
    for col in cols:
        c = ws.cell(row=row_idx, column=col)
        styles[col] = {
            "font": c.font.copy(), "border": c.border.copy(), "fill": c.fill.copy(),
            "numfmt": c.number_format, "protect": c.protection.copy(), "align": c.alignment.copy(),
        }
    height = ws.row_dimensions[row_idx].height
    return styles, height

def apply_row_style(ws: Worksheet, row_idx: int, styles, height, numfmt_override: Dict[int, str] | None = None):
    numfmt_override = numfmt_override or {}
    for col, st in styles.items():
        c = ws.cell(row=row_idx, column=col)
        c.font = st["font"].copy()
        c.border = st["border"].copy()
        c.fill = st["fill"].copy()
        c.number_format = numfmt_override.get(col, st["numfmt"])
        c.protection = st["protect"].copy()
        c.alignment = st["align"].copy()
    if height is not None:
        ws.row_dimensions[row_idx].height = height

# ----- scrierea workbook-ului pt. un client -----

def build_workbook(template_path: str, client: KeyRow, items: List[TxRow], colectari_str: str) -> Tuple[str, bytes]:
    wb = load_workbook(template_path)
    ws = wb.active

    # token-uri (pot fi în interiorul unui text mai lung)
    r,c,t = find_cell_with_token(ws, "{HEADER_DATE}")
    replace_token(ws, r, c, t, "{HEADER_DATE}", ro_header_date(today_ro()))

    r,c,t = find_cell_with_token(ws, "{COLECTARI}")
    replace_token(ws, r, c, t, "{COLECTARI}", colectari_str)

    r,c,t = find_cell_with_token(ws, "{NUME}")
    replace_token(ws, r, c, t, "{NUME}", client.nume)

    r,c,t = find_cell_with_token(ws, "{NR. INREGISTRARE R.C.}")
    replace_token(ws, r, c, t, "{NR. INREGISTRARE R.C.}", client.nr_rc)

    r,c,t = find_cell_with_token(ws, "{CUI}")
    replace_token(ws, r, c, t, "{CUI}", client.cui)

    r,c,t = find_cell_with_token(ws, "{ADRESA}")
    replace_token(ws, r, c, t, "{ADRESA}", client.adresa)

    # capetele de tabel + coloanele de date (le aflăm din token-uri)
    r_site, c_site, t_site = find_cell_with_token(ws, "{DENUMIRE SITE}")
    ws.cell(r_site, c_site).value = t_site.replace("{DENUMIRE SITE}", "Denumire Site")

    r_tid, c_tid, t_tid = find_cell_with_token(ws, "{TID}")
    ws.cell(r_tid, c_tid).value = t_tid.replace("{TID}", "TID")

    r_prod, c_prod, t_prod = find_cell_with_token(ws, "{DENUMIRE PRODUS}")
    ws.cell(r_prod, c_prod).value = t_prod.replace("{DENUMIRE PRODUS}", "Denumire Produs")

    r_val, c_val, t_val = find_cell_with_token(ws, "{VALOARE CU TVA}")
    ws.cell(r_val, c_val).value = t_val.replace("{VALOARE CU TVA}", "Valoare cu TVA")

    r_dat, c_dat, t_dat = find_cell_with_token(ws, "{DATA TRANZACTIEI}")
    ws.cell(r_dat, c_dat).value = t_dat.replace("{DATA TRANZACTIEI}", "Data Tranzactiei")

    r_tot, c_tot, _ = find_cell_with_token(ws, "{TOTAL}")
    # lăsăm celula cu „Total” ca etichetă; suma va fi pe aceeași coloană, pe rândul total

    # stilul „model” = primul rând de date (sub header)
    row_model = max(r_site, r_tid, r_prod, r_val, r_dat) + 1
    data_styles, data_height = read_row_style(ws, row_model, [c_site, c_tid, c_prod, c_val, c_dat])
    total_styles, total_height = read_row_style(ws, r_tot, [c_tot])

    num_override = { c_val: FORMAT_NUMBER_00, c_dat: "yyyy-mm-dd hh:mm:ss" }

    r = row_model
    for idx, it in enumerate(items):
        if idx > 0:
            ws.insert_rows(r)
        apply_row_style(ws, r, data_styles, data_height, num_override)
        ws.cell(r, c_site, value=client.site)
        ws.cell(r, c_tid,  value=str(it.tid))
        ws.cell(r, c_prod, value=it.operator)
        vcell = ws.cell(r, c_val); vcell.value = round(float(it.valoare or 0.0), 2)
        dcell = ws.cell(r, c_dat); dcell.value = it.when
        r += 1

    # rândul total
    tot_row = r_tot + (len(items) - 1 if len(items) > 1 else 0)
    if len(items) > 1:
        ws.insert_rows(tot_row)
    apply_row_style(ws, tot_row, total_styles, total_height, {c_tot: FORMAT_NUMBER_00})
    total_value = round(sum(x.valoare for x in items), 2)
    ws.cell(tot_row, c_tot).value = total_value

    mem = io.BytesIO()
    wb.save(mem)
    fname = f"Ordin - {client.nume}.xlsx"
    return fname, mem.getvalue()

# ----- grupare + ZIP -----

def generate_zip(astob_path: str, key_path: str, template_path: str) -> bytes:
    key_rows = read_key(key_path)
    ast_rows = read_astob(astob_path)

    key_by_tid: Dict[str, KeyRow] = {str(kr.tid): kr for kr in key_rows}
    ast_tids = {str(tx.tid) for tx in ast_rows}
    matched_tids = ast_tids & set(key_by_tid.keys())
    print(f"[debug] tids: ast={len(ast_tids)}, key={len(key_by_tid)}, matched={len(matched_tids)}")

    grouped: Dict[str, List[TxRow]] = {}
    client_info: Dict[str, KeyRow] = {}

    for tx in ast_rows:
        kr = key_by_tid.get(str(tx.tid))
        if not kr:
            continue
        cid = kr.nume
        grouped.setdefault(cid, []).append(tx)
        client_info.setdefault(cid, kr)

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
        ex = ""
        if len(matched_tids) == 0:
            ex = "Niciun TID din ASTOB nu se potrivește cu TABEL CHEIE."
        elif not all_dt:
            ex = "Nicio dată validă în ASTOB."
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
