# generate_orders.py
from __future__ import annotations

import zipfile
import re
from io import BytesIO
from datetime import datetime, date
from typing import Dict, List, Tuple

import pandas as pd
from unidecode import unidecode
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter


# ------------------------------
# Utils
# ------------------------------

RO_MONTHS = {
    1: "IANUARIE", 2: "FEBRUARIE", 3: "MARTIE", 4: "APRILIE",
    5: "MAI", 6: "IUNIE", 7: "IULIE", 8: "AUGUST",
    9: "SEPTEMBRIE", 10: "OCTOMBRIE", 11: "NOIEMBRIE", 12: "DECEMBRIE",
}

def today_header_ro(d: date | None = None) -> str:
    """ex: 13 SEPTEMBRIE 2025"""
    d = d or date.today()
    return f"{d.day} {RO_MONTHS[d.month]} {d.year}"


def norm(s: str) -> str:
    """Normalizează text: lower, fără diacritice, fără spații multiple/punctuație neesențială."""
    s = unidecode(str(s)).lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def find_col(df: pd.DataFrame, candidates: List[str]) -> str:
    """
    Găsește o coloană din DataFrame ținând cont de diacritice/sinonime/spații.
    Candidates = liste de etichete POSIBILE (exacte) – le normalizăm și căutăm match.
    """
    nmap = {norm(c): c for c in df.columns}
    for cand in candidates:
        nc = norm(cand)
        # direct
        for col in df.columns:
            if norm(col) == nc:
                return col
        # sinonime „conținute”
        for key_norm, original in nmap.items():
            if nc in key_norm:
                return original
    raise KeyError(f"Missing columns: tried {candidates} in {list(df.columns)}")


def read_excel_from_bytes(xlsx_bytes: bytes) -> pd.DataFrame:
    """Citește fișier Excel din bytes, engine=openpyxl, fără a distruge tipurile."""
    bio = BytesIO(xlsx_bytes)
    return pd.read_excel(bio, engine="openpyxl")


def to_float(val) -> float:
    if pd.isna(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    # în exportul ASTOB, decimalul poate veni cu virgulă
    s = s.replace(".", "").replace(",", ".") if s.count(",") == 1 and s.count(".") > 1 else s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def combine_datetime(date_val, time_val) -> datetime:
    """Combină coloanele Data tranzacției și (dacă există) Ora tranzacției."""
    if pd.isna(date_val):
        return None
    # dacă Data deja are timp în ea
    if isinstance(date_val, datetime):
        d = date_val
    else:
        try:
            d = pd.to_datetime(str(date_val))
        except Exception:
            return None

    if time_val is not None and not pd.isna(time_val) and str(time_val).strip():
        try:
            t = pd.to_datetime(str(time_val)).time()
            return datetime.combine(d.date(), t)
        except Exception:
            return d
    return d


def safe_filename(s: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|]", "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ------------------------------
# Generator
# ------------------------------

def generate_zip_from_xlsx_bytes(
    astob_bytes: bytes,
    key_bytes: bytes,
    template_path: str,
) -> bytes:
    """
    - citește ASTOB.xlsx + TABEL CHEIE.xlsx din bytes
    - construiește foi pentru fiecare client cu total > 0
    - întoarce ZIP (bytes) cu toate „Ordin - CLIENT <n>.xlsx”
    """

    # ---- citește tabele
    ast = read_excel_from_bytes(astob_bytes)
    key = read_excel_from_bytes(key_bytes)

    # ---- identifică coloanele, tolerant
    # ASTOB
    col_tid_ast    = find_col(ast, ["Nr. terminal", "TID"])
    col_sum_ast    = find_col(ast, ["Sumă tranzacție", "Suma tranzactie", "Valoare cu TVA"])
    col_merchant   = find_col(ast, ["Nume Comerciant", "Comerciant", "Denumire Produs"])
    col_date       = find_col(ast, ["Data tranzacției", "Data tranzactiei", "Data"])
    col_time       = None
    try:
        col_time = find_col(ast, ["Ora tranzacției", "Ora tranzactiei"])
    except Exception:
        col_time = None  # e ok; nu e obligatoriu

    # KEY
    col_tid_key    = find_col(key, ["TID", "Nr. terminal"])
    col_name_key   = find_col(key, ["NUME", "Client", "Denumire client"])
    col_reg_key    = find_col(key, ["Nr. inregistrare R.C.", "NR. INREGISTRARE R.C.", "Nr inregistrare RC"])
    col_cui_key    = find_col(key, ["CUI"])
    col_addr_key   = find_col(key, ["ADRESA", "Sediul central", "Adresă"])
    col_site_key   = find_col(key, ["DENUMIRE SITE", "Denumire Site", "Site", "Denumire societate agent", "Denumire societate agent "])

    # ---- normalizează coloanele necesare
    ast = ast.copy()
    key = key.copy()

    ast["_TID"]       = ast[col_tid_ast].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    ast["_SUM"]       = ast[col_sum_ast].map(to_float)
    ast["_MERCHANT"]  = ast[col_merchant].astype(str).str.strip()
    ast["_DT"]        = [
        combine_datetime(d, ast[col_time].iloc[i] if col_time else None)
        for i, d in enumerate(ast[col_date])
    ]

    key["_TID"]       = key[col_tid_key].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    key["_NAME"]      = key[col_name_key].astype(str).str.strip()
    key["_REG"]       = key[col_reg_key].astype(str).str.strip()
    key["_CUI"]       = key[col_cui_key].astype(str).str.strip()
    key["_ADDR"]      = key[col_addr_key].astype(str).str.strip()
    key["_SITE"]      = key[col_site_key].astype(str).str.strip()

    # map TID -> (client_name, reg, cui, addr, site)
    tid_rows: Dict[str, Dict[str, str]] = {}
    for _, r in key.iterrows():
        tid_rows[r["_TID"]] = {
            "client": r["_NAME"],
            "reg":    r["_REG"],
            "cui":    r["_CUI"],
            "addr":   r["_ADDR"],
            "site":   r["_SITE"],
        }

    # grupăm pe client (clientul poate avea mai multe TID-uri)
    client_to_tids: Dict[str, List[str]] = {}
    for tid, info in tid_rows.items():
        client_to_tids.setdefault(info["client"], []).append(tid)

    # pre-filtrare: păstrăm doar rândurile ASTOB care au TID recunoscut
    ast = ast[ast["_TID"].isin(tid_rows.keys())].copy()

    # --------- compune ZIP
    out_zip = BytesIO()
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:

        for client, tids in client_to_tids.items():
            # rândurile pentru acest client (TID-urile lui)
            rows = ast[ast["_TID"].isin(tids)].copy()

            # dacă nu are tranzacții -> sari
            if rows.empty:
                continue

            # sortare data+ora (asc)
            rows = rows.sort_values(by="_DT", kind="mergesort")  # stabil

            # pregătește item-urile finale pentru scris în foaie
            items: List[Tuple[str, str, str, float, datetime]] = []
            for _, r in rows.iterrows():
                dt = r["_DT"]
                if pd.isna(dt):
                    continue
                tid = r["_TID"]
                site = tid_rows[tid]["site"]
                merchant = r["_MERCHANT"]
                suma = float(r["_SUM"])
                items.append((site, tid, merchant, suma, dt))

            # total client
            total_client = sum(v for (_, _, _, v, _) in items)
            if total_client <= 0.0001:
                # skip clienți cu TOTAL 0
                continue

            # perioadă colectări din datele clientului: min/max date
            dmin = min(dt.date() for *_, dt in items)
            dmax = max(dt.date() for *_, dt in items)
            colectari_text = f"Colectari - {dmin.strftime('%d.%m.%Y')} - {dmax.strftime('%d.%m.%Y')}"

            # încarcă șablonul și scrie foaia
            wb = load_workbook(template_path)
            ws = wb.active  # „Foaie1” în template

            # Plasează antet client (placeholder-ele exact ca în pozele tale)
            replace_in_sheet(
                ws,
                {
                    "{HEADER_DATE}": today_header_ro(),
                    "{NUME}": client,
                    "{NR. INREGISTRARE R.C.}": key.loc[key["_NAME"] == client, "_REG"].iloc[0] if (key["_NAME"] == client).any() else "",
                    "{CUI}": key.loc[key["_NAME"] == client, "_CUI"].iloc[0] if (key["_NAME"] == client).any() else "",
                    "{ADRESA}": key.loc[key["_NAME"] == client, "_ADDR"].iloc[0] if (key["_NAME"] == client).any() else "",
                    "{COLECTARI}": colectari_text,
                    # antetele coloanelor – scoatem acolade dacă încă sunt în șablon:
                    "{DENUMIRE SITE}": "Denumire Site",
                    "{TID}": "TID",
                    "{DENUMIRE PRODUS}": "Denumire Produs",
                    "{VALOARE CU TVA}": "Valoare cu TVA",
                    "{DATA TRANZACTIEI}": "Data Tranzactiei",
                    "{TOTAL}": "Total",
                }
            )

            # în template-ul tău, capul de tabel e pe rândul 16, iar datele încep la 17
            start_row = 17
            font_body = Font(name="Calibri", size=11)
            align_left  = Alignment(horizontal="left",  vertical="center", wrap_text=False)
            align_right = Alignment(horizontal="right", vertical="center", wrap_text=False)

            # scrie rândurile (A=site, B=tid, C=merchant, D=sumă, E=data+ora)
            row_idx = start_row
            for site, tid, merchant, suma, dt in items:
                ws.cell(row=row_idx, column=1, value=site).font = font_body
                ws.cell(row=row_idx, column=1).alignment = align_left

                ws.cell(row=row_idx, column=2, value=tid).font = font_body
                ws.cell(row=row_idx, column=2).alignment = align_left

                ws.cell(row=row_idx, column=3, value=merchant).font = font_body
                ws.cell(row=row_idx, column=3).alignment = align_left

                c_sum = ws.cell(row=row_idx, column=4, value=round(suma, 2))
                c_sum.font = font_body
                c_sum.alignment = align_right
                c_sum.number_format = "0,00"

                c_dt = ws.cell(row=row_idx, column=5, value=dt)
                c_dt.font = font_body
                c_dt.alignment = align_left
                c_dt.number_format = "yyyy-mm-dd hh:mm:ss"

                row_idx += 1

            # rândul TOTAL imediat sub tranzacții
            total_row = row_idx + 1  # un rând gol între
            ws.cell(row=total_row, column=1, value="Total").font = Font(name="Calibri", size=12, bold=True)

            c_total = ws.cell(row=total_row, column=5, value=round(total_client, 2))
            c_total.font = Font(name="Calibri", size=12, bold=True)
            c_total.alignment = align_right
            c_total.number_format = "0,00"

            # salvează fișa client
            out_name = f"Ordin - {safe_filename(client)}.xlsx"
            bio = BytesIO()
            wb.save(bio)
            bio.seek(0)

            zf.writestr(out_name, bio.read())

    out_zip.seek(0)
    return out_zip.read()


def replace_in_sheet(ws, mapping: Dict[str, str]):
    """
    Înlocuiește placeholder-ele (cheile din mapping) oriunde apar în foaie.
    Lasă restul formatării neschimbate.
    """
    for row in ws.iter_rows():
        for cell in row:
            if cell.value and isinstance(cell.value, str):
                v = cell.value
                changed = False
                for k, rep in mapping.items():
                    if k in v:
                        v = v.replace(k, rep)
                        changed = True
                if changed:
                    cell.value = v
