# generate_orders.py
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

# -------------------------
# Utils
# -------------------------

RO_TZ = timezone(timedelta(hours=3))  # EET/EEST approx; pentru exactitate ai putea folosi zoneinfo

RO_MONTHS = {
    1: "IANUARIE", 2: "FEBRUARIE", 3: "MARTIE", 4: "APRILIE",
    5: "MAI", 6: "IUNIE", 7: "IULIE", 8: "AUGUST",
    9: "SEPTEMBRIE", 10: "OCTOMBRIE", 11: "NOIEMBRIE", 12: "DECEMBRIE",
}

def today_ro() -> date:
    # „Astăzi” în RO (nu depindem de server)
    return datetime.utcnow().astimezone(RO_TZ).date()

def ro_header_date(d: date) -> str:
    return f"{d.day} {RO_MONTHS[d.month]} {d.year}"

def strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return s
    # normalize diacritice (ș/ţ variante), apoi scoatem combining marks
    s = (s.replace("Ş", "Ș").replace("Ţ", "Ț")
           .replace("ş", "ș").replace("ţ", "ț"))
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
    for cand in candidates:
        nc = norm(cand)
        if nc in cols:
            return cols[nc]
    # încercăm „conține” (pentru variante apropiate)
    for nc, orig in cols.items():
        for cand in candidates:
            if norm(cand) in nc:
                return orig
    raise KeyError(f"Missing columns: tried {list(candidates)} in {list(df.columns)}")

def to_float(x) -> float:
    # acceptă 46,16 / "046,16" / 46.16 / etc.
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    s = s.replace(" ", "")
    # dacă are virgulă și punct, presupunem că punctul e separator de mii
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def combine_datetime(dcol, tcol) -> Optional[datetime]:
    if pd.isna(dcol) and pd.isna(tcol):
        return None
    try:
        if isinstance(dcol, (datetime, pd.Timestamp)):
            d = pd.Timestamp(dcol).to_pydatetime()
        else:
            d = pd.to_datetime(dcol, dayfirst=True, errors="coerce")
            if pd.isna(d):
                return None
            d = d.to_pydatetime()
        if isinstance(tcol, (datetime, pd.Timestamp, time)):
            tt = pd.Timestamp(tcol).to_pydatetime().time()
        elif pd.isna(tcol) or tcol is None:
            tt = time(0, 0, 0)
        else:
            tt = pd.to_datetime(str(tcol), errors="coerce").time()  # „08:53:26”
            if tt is None:
                tt = time(0, 0, 0)
        return datetime(d.year, d.month, d.day, tt.hour, tt.minute, tt.second)
    except Exception:
        return None

# -------------------------
# Date models
# -------------------------

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

# -------------------------
# Citire tabele
# -------------------------

KEY_MAP = {
    "tid": ["TID", "NR. TERMINAL", "NR TERMINAL"],
    "site": ["DENUMIRE SITE", "DENUMIRE SOCIETATEAGENT", "DENUMIRE SOCIETATE AGENT", "SITE"],
    "nume": ["NUME", "CLIENT", "DENUMIRE CLIENT", "DENUMIRE"],
    "nr_rc": ["NR. INREGISTRARE R.C.", "NR INREGISTRARE RC", "NR INREGISTRARE", "NR. INREGISTRARE"],
    "cui": ["CUI", "CUI CNP"],
    "adresa": ["ADRESA", "SEDIUL CENTRAL", "ADRESA SEDIUL CENTRAL"],
}

ASTOB_MAP = {
    "tid": ["NR. TERMINAL", "NR TERMINAL", "TID"],
    "operator": ["NUME OPERATOR", "OPERATOR"],
    "valoare": ["SUMA TRANZACTIEI", "SUMA TRANZACTIE", "SUMA", "VALOARE CU TVA"],
    "data": ["DATA TRANZACTIEI", "DATA"],
    "ora": ["ORA TRANZACTIEI", "ORA"],
}

def read_key(path: str) -> List[KeyRow]:
    key = pd.read_excel(path, engine="openpyxl")
    col_tid    = find_col_name(key, KEY_MAP["tid"])
    col_site   = find_col_name(key, KEY_MAP["site"])
    try:
        col_nume   = find_col_name(key, KEY_MAP["nume"])
    except KeyError:
        # multe tabele nu au NUME separat – îl derivăm din „DENUMIRE SITE” dacă pare gen „CLIENT 24 …”
        col_nume = col_site
    col_nrrc  = find_col_name(key, KEY_MAP["nr_rc"])
    col_cui   = find_col_name(key, KEY_MAP["cui"])
    col_adr   = find_col_name(key, KEY_MAP["adresa"])

    rows: List[KeyRow] = []
    for _, r in key.iterrows():
        tid = str(r[col_tid]).strip()
        if tid == "" or tid.lower() == "nan":
            continue
        rows.append(
            KeyRow(
                tid=tid,
                site=str(r[col_site]).strip(),
                nume=str(r[col_nume]).strip(),
                nr_rc=str(r[col_nrrc]).strip(),
                cui=str(r[col_cui]).strip(),
                adresa=str(r[col_adr]).strip(),
            )
        )
    return rows

def read_astob(path: str) -> List[TxRow]:
    df = pd.read_excel(path, engine="openpyxl")
    col_tid   = find_col_name(df, ASTOB_MAP["tid"])
    col_op    = find_col_name(df, ASTOB_MAP["operator"])
    col_val   = find_col_name(df, ASTOB_MAP["valoare"])
    col_data  = find_col_name(df, ASTOB_MAP["data"])
    col_ora   = find_col_name(df, ASTOB_MAP["ora"])

    rows: List[TxRow] = []
    for _, r in df.iterrows():
        tid = str(r[col_tid]).strip()
        if tid == "" or tid.lower() == "nan":
            continue
        op = str(r[col_op]).strip()
        val = to_float(r[col_val])
        when = combine_datetime(r[col_data], r[col_ora])
        if when is None:
            continue
        rows.append(TxRow(tid=tid, operator=op, valoare=val, when=when))
    # sortare crescătoare după data+ora
    rows.sort(key=lambda x: x.when)
    return rows

# -------------------------
# Lucru cu șablonul
# -------------------------

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
            "font": copy(c.font),
            "border": copy(c.border),
            "fill": copy(c.fill),
            "numfmt": c.number_format,
            "protect": copy(c.protection),
            "align": copy(c.alignment),
        }
    height = ws.row_dimensions[row_idx].height
    return styles, height

def apply_row(ws: Worksheet, row_idx: int, styles, height, numfmt_override: Dict[int, str] | None = None):
    numfmt_override = numfmt_override or {}
    for col, st in styles.items():
        c = ws.cell(row=row_idx, column=col)
        c.font          = copy(st["font"])
        c.border        = copy(st["border"])
        c.fill          = copy(st["fill"])
        c.number_format = numfmt_override.get(col, st["numfmt"])
        c.protection    = copy(st["protect"])
        c.alignment     = copy(st["align"])
    if height is not None:
        ws.row_dimensions[row_idx].height = height

def build_workbook(template_path: str,
                   client: KeyRow,
                   items: List[TxRow],
                   colectari_str: str) -> Tuple[str, bytes]:

    wb = load_workbook(template_path)
    ws = wb.active

    # găsim placeholder-ele
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

    # scriem antetele / infourile
    ws.cell(*ph.header_date).value = ro_header_date(today_ro())
    ws.cell(*ph.colectari).value   = f"Colectari - {colectari_str}"
    ws.cell(*ph.nume).value        = client.nume
    ws.cell(*ph.nrrc).value        = client.nr_rc
    ws.cell(*ph.cui).value         = client.cui
    ws.cell(*ph.adresa).value      = client.adresa

    # înlocuim headerele coloanelor (fără acolade)
    ws.cell(*ph.head_site).value = "Denumire Site"
    ws.cell(*ph.head_tid ).value = "TID"
    ws.cell(*ph.head_prod).value = "Denumire Produs"
    ws.cell(*ph.head_val ).value = "Valoare cu TVA"
    ws.cell(*ph.head_data).value = "Data Tranzactiei"
    ws.cell(*ph.total_label).value = "Total"

    # coloanele țintă
    c_site = ws.cell(*ph.head_site)
    c_tid  = ws.cell(*ph.head_tid)
    c_prod = ws.cell(*ph.head_prod)
    c_val  = ws.cell(*ph.head_val)
    c_dat  = ws.cell(*ph.head_data)
    c_tot  = ws.cell(*ph.total_label)

    # rândul model = rândul de sub header (și deasupra lui "Total")
    row_model = c_site.row + 1

    # stilul model pentru rândurile de tranzacții
    data_styles, data_height = read_styles(ws, row_model, [c_site.column, c_tid.column, c_prod.column, c_val.column, c_dat.column])
    # stilul model pentru rândul de total (pe coloana totalului)
    total_styles, total_height = read_styles(ws, c_tot.row, [c_tot.column])

    # mutăm rândul de „Total” în jos pe măsură ce inserăm rânduri
    num_override = {
        c_val.column: FORMAT_NUMBER_00,
        c_dat.column: "yyyy-mm-dd hh:mm:ss",
    }

    r = row_model
    for idx, it in enumerate(items):
        if idx > 0:
            ws.insert_rows(r)
        apply_row(ws, r, data_styles, data_height, num_override)

        ws.cell(r, c_site.column, value=client.site)
        ws.cell(r, c_tid.column,  value=str(it.tid))
        ws.cell(r, c_prod.column, value=it.operator)

        vcell = ws.cell(r, c_val.column)
        vcell.value = round(float(it.valoare or 0.0), 2)  # numeric, 2 zecimale

        dcell = ws.cell(r, c_dat.column)
        dcell.value = it.when

        r += 1

    # rândul total (plasat la finalul listării)
    tot_row = c_tot.row + (len(items) - 1 if len(items) > 1 else 0)
    if len(items) > 1:
        ws.insert_rows(tot_row)
    apply_row(ws, tot_row, total_styles, total_height, {c_tot.column: FORMAT_NUMBER_00})

    total_value = round(sum(x.valoare for x in items), 2)
    tcell = ws.cell(tot_row, c_tot.column)
    tcell.value = total_value

    # salvăm în memorie
    mem = io.BytesIO()
    wb.save(mem)
    fname = f"Ordin - {client.nume}.xlsx"
    return fname, mem.getvalue()

# -------------------------
# Generator principal
# -------------------------

def generate_zip(astob_path: str, key_path: str, template_path: str) -> bytes:
    key_rows = read_key(key_path)
    ast_rows = read_astob(astob_path)

    # index Key by TID
    key_by_tid: Dict[str, KeyRow] = {}
    for kr in key_rows:
        key_by_tid[str(kr.tid)] = kr

    # grupare tranzacții pe client (după TID->client)
    grouped: Dict[str, List[TxRow]] = {}
    client_info: Dict[str, KeyRow] = {}

    for tx in ast_rows:
        kr = key_by_tid.get(str(tx.tid))
        if not kr:
            # TID fără intrare în cheie – îl sărim
            continue
        cid = kr.nume
        grouped.setdefault(cid, []).append(tx)
        # pentru „Denumire Site” afișăm site-ul din cheie; dacă un client are mai multe site-uri,
        # aici folosim primul, cum a fost în exemplele tale
        if cid not in client_info:
            client_info[cid] = kr

    # interval Colectari
    all_dt = [tx.when for tx in ast_rows]
    if all_dt:
        start = min(all_dt).date()
        end   = max(all_dt).date()
        colectari_str = f"{start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}"
    else:
        colectari_str = ""

    # construim fișierele (doar cele cu total > 0)
    zip_mem = io.BytesIO()
    with zipfile.ZipFile(zip_mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for client, items in grouped.items():
            total_client = sum(x.valoare for x in items)
            if round(total_client, 2) <= 0:
                continue
            fname, data = build_workbook(template_path, client_info[client], items, colectari_str)
            zf.writestr(fname, data)

    return zip_mem.getvalue()

# -------------------------
# Interfață utilizabilă din app.py
# -------------------------

def generate_zip_from_bytes(astob_bytes: bytes, key_bytes: bytes, template_path: str) -> bytes:
    ast_f = io.BytesIO(astob_bytes)
    key_f = io.BytesIO(key_bytes)

    # read_excel cere un „path-like” sau buffer cu .seek(0)
    ast_f.seek(0); key_f.seek(0)

    # salvăm temporar în mem pentru pandas/openpyxl (funcțiile de mai sus cer path)
    with io.BytesIO(ast_f.read()) as af, io.BytesIO(key_f.read()) as kf:
        # scriem în fișiere temporare în memorie? mai simplu folosim pandas direct:
        # Dar pentru consistență, salvăm în tempfiles pe disc (Render are /tmp).
        import tempfile, os
        with tempfile.TemporaryDirectory() as td:
            ap = os.path.join(td, "astob.xlsx")
            kp = os.path.join(td, "key.xlsx")
            with open(ap, "wb") as out: out.write(af.getvalue())
            with open(kp, "wb") as out: out.write(kf.getvalue())
            return generate_zip(ap, kp, template_path)

# -------------------------
# CLI
# -------------------------

def main():
    import argparse, os
    p = argparse.ArgumentParser()
    p.add_argument("--astob", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--template", required=True, help="Calea către șablonul Excel (ex: static/bp model cu {} - date.xlsx)")
    p.add_argument("--out-dir", default="out_excel")
    p.add_argument("--out-zip", default="ordine.zip")
    args = p.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    data = generate_zip(args.astob, args.key, args.template)

    zip_path = os.path.join(args.out_dir, os.path.basename(args.out_zip))
    with open(zip_path, "wb") as f:
        f.write(data)

    print(f"[ok] scris: {zip_path}")

if __name__ == "__main__":
    main()
