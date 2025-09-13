#!/usr/bin/env python3
import argparse
import io
import os
import re
import zipfile
from datetime import datetime, date
import unicodedata

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet


# ---------- utilitare ----------

def norm(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.strip()

def looks_like(col_name: str, candidates) -> bool:
    cn = norm(col_name).lower().replace(" ", "")
    return any(cn == norm(c).lower().replace(" ", "").replace(".", "") for c in candidates)

def find_col(df: pd.DataFrame, candidates):
    for c in df.columns:
        if looks_like(c, candidates):
            return c
    raise KeyError(f"Missing columns: tried {candidates} in {list(df.columns)}")

def read_table(path: str) -> pd.DataFrame:
    if path.lower().endswith((".xlsx", ".xlsm", ".xltx", ".xltm")):
        return pd.read_excel(path, engine="openpyxl")
    # fallback (uneori Gmail/Excel schimbă conten-type -> csv)
    try:
        return pd.read_csv(path, sep=None, engine="python")
    except Exception:
        # ultima încercare – latin1
        return pd.read_csv(path, sep=None, engine="python", encoding="latin1")

RO_MONTHS = {
    1:"IANUARIE",2:"FEBRUARIE",3:"MARTIE",4:"APRILIE",5:"MAI",6:"IUNIE",
    7:"IULIE",8:"AUGUST",9:"SEPTEMBRIE",10:"OCTOMBRIE",11:"NOIEMBRIE",12:"DECEMBRIE"
}
def header_date_today() -> str:
    today = date.today()
    return f"{today.day} {RO_MONTHS[today.month]} {today.year}"

def to_datetime(d, t=None):
    # acceptă deja d tip datetime, sau string „YYYY-MM-DD hh:mm:ss”
    if pd.isna(d):
        return None
    if isinstance(d, datetime):
        return d
    try:
        if t is not None and not pd.isna(t):
            # combină Data + Ora
            return datetime.strptime(f"{str(d)} {str(t)}", "%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    # încearcă parse liber
    return pd.to_datetime(str(d), errors="coerce")

def num(v):
    if pd.isna(v):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    # 46,16 -> 46.16
    return float(str(v).replace(".", "").replace(",", "."))
# ---------- stiluri openpyxl ----------

def copy_cell_style(dst, src):
    dst.font = src.font
    dst.border = src.border
    dst.fill = src.fill
    dst.number_format = src.number_format
    dst.protection = src.protection
    dst.alignment = src.alignment

def snapshot_row_styles(ws: Worksheet, row_idx: int):
    styles = {}
    for col in range(1, ws.max_column + 1):
        cell = ws.cell(row=row_idx, column=col)
        styles[col] = {
            "font": cell.font,
            "border": cell.border,
            "fill": cell.fill,
            "numfmt": cell.number_format,
            "protect": cell.protection,
            "align": cell.alignment,
        }
    height = ws.row_dimensions[row_idx].height
    return styles, height

def apply_row_styles(ws: Worksheet, row_idx: int, styles, height):
    for col, st in styles.items():
        c = ws.cell(row=row_idx, column=col)
        c.font = st["font"]
        c.border = st["border"]
        c.fill = st["fill"]
        c.number_format = st["numfmt"]
        c.protection = st["protect"]
        c.alignment = st["align"]
    if height is not None:
        ws.row_dimensions[row_idx].height = height

# ---------- generator ----------

def run_generator(astob_path: str, key_path: str, template_path: str, out_dir: str, out_zip_path: str):
    os.makedirs(out_dir, exist_ok=True)

    ast = read_table(astob_path)
    key = read_table(key_path)

    # coloane ASTOB
    col_date = find_col(ast, ["Data tranzacției", "Data tranzactiei", "data tranzactiei"])
    col_time = next((c for c in ast.columns if looks_like(c, ["Ora tranzacției", "Ora tranzactiei"])), None)
    col_value = find_col(ast, ["Sumă tranzacție", "Suma tranzactie", "sumă tranzacție"])
    col_tid_ast = find_col(ast, ["Nr. terminal", "TID"])
    col_site_ast = next((c for c in ast.columns if looks_like(c, ["Nr. site"])), None)
    col_product_name = next((c for c in ast.columns if looks_like(c, ["Nume Comerciant"])), None)
    if col_product_name is None:
        # fallback – mapăm cod tip produs -> denumire produs din cheie, dacă există
        col_code = find_col(ast, ["Cod tip produs"])
        col_code_key = find_col(key, ["Cod tip produs"])
        col_dn_key = find_col(key, ["DENUMIRE PRODUS"])
        prod_map = dict(zip(key[col_code_key], key[col_dn_key]))
        ast["__prod__"] = ast[col_code].map(prod_map).fillna("")
        col_product_name = "__prod__"

    # coloane CHEIE
    col_tid_key = find_col(key, ["TID", "Nr. terminal"])
    col_site_name = find_col(key, ["DENUMIRE SITE"])
    col_client = find_col(key, ["DENUMIRE SOCIETATEAGENT", "Denumire Societate Agent", "Client"])
    col_reg = find_col(key, ["Nr. inregistrare R.C.", "Nr. inregistrare R.C", "Nr. inregistrare RC", "NR. INREGISTRARE R.C."])
    col_cui = find_col(key, ["CUI"])
    col_addr = find_col(key, ["ADRESA", "Adresă", "Adresa"])

    # pregătim frame unificat
    df = ast.copy()

    df["__timestamp__"] = [to_datetime(d, ast.iloc[i][col_time] if col_time else None) for i, d in enumerate(ast[col_date])]
    df["__amount__"] = ast[col_value].apply(num)
    df["__tid__"] = ast[col_tid_ast].astype(str)
    df["__site_id__"] = ast[col_site_ast].astype(str) if col_site_ast else ""
    df["__prodname__"] = ast[col_product_name].astype(str)

    # join cu CHEIE după TID
    k = key[[col_tid_key, col_site_name, col_client, col_reg, col_cui, col_addr]].copy()
    k[col_tid_key] = k[col_tid_key].astype(str)
    k.columns = ["__tid__", "__site__", "__client__", "__reg__", "__cui__", "__addr__"]

    df = df.merge(k, on="__tid__", how="left")

    # dacă lipsesc info de client (ex.: mai multe TID-uri pt același client), completăm cu ultima valoare non-nan la grupare
    df["__client__"] = df["__client__"].fillna(method="ffill").fillna(method="bfill")
    df["__site__"] = df["__site__"].fillna("")

    # grupăm pe client
    clients = []
    for client, g in df.groupby("__client__"):
        total = g["__amount__"].sum()
        if total <= 0:
            continue  # nu generăm pentru total 0
        clients.append((client, g.copy(), total))

    if not clients:
        raise ValueError("Nu există clienți cu total > 0 în acest lot.")

    # încarcă șablonul o singură dată ca să citim stilurile etalon
    base_wb = load_workbook(template_path)
    base_ws = base_wb.active

    # memorăm locațiile cu tokenuri (le căutăm dinamic ca să nu depindem de coordonate fixe)
    def find_token(ws, token):
        token = "{" + token + "}"
        for r in ws.iter_rows(values_only=False):
            for c in r:
                if isinstance(c.value, str) and token in c.value:
                    return c.row, c.column
        return None, None

    row_hdr, col_hdr = find_token(base_ws, "HEADER_DATE")
    row_col, col_col = find_token(base_ws, "COLECTARI")
    row_total_tpl, _ = find_token(base_ws, "TOTAL")
    row_first_tpl, _ = find_token(base_ws, "DENUMIRE SITE")
    if not row_first_tpl:
        row_first_tpl = 17  # fallback

    # snapshot stiluri pentru rândul de date și rândul de total
    data_styles, data_height = snapshot_row_styles(base_ws, row_first_tpl)
    total_styles, total_height = snapshot_row_styles(base_ws, row_total_tpl)

    # pentru fiecare client: completăm și salvăm
    for client, g, total in clients:
        wb = load_workbook(template_path)
        ws = wb.active

        # antet: data de azi + colectari (min/max din g)
        if row_hdr:
            ws.cell(row=row_hdr, column=col_hdr).value = header_date_today()
        if row_col:
            start = g["__timestamp__"].min().date()
            end = g["__timestamp__"].max().date()
            ws.cell(row=row_col, column=col_col).value = f"Colectari - {start:%d.%m.%Y} - {end:%d.%m.%Y}"

        # date client (din primul rând cu info completă)
        r0 = g.iloc[0]
        # înlocuim toate tokenurile dacă apar oriunde în foaie
        replace_map = {
            "{NUME}": str(r0["__client__"] or ""),
            "{NR. INREGISTRARE R.C.}": str(r0["__reg__"] or ""),
            "{CUI}": str(r0["__cui__"] or ""),
            "{ADRESA}": str(r0["__addr__"] or ""),
        }
        for r in ws.iter_rows(values_only=False):
            for c in r:
                if isinstance(c.value, str):
                    v = c.value
                    for ktoken, rep in replace_map.items():
                        if ktoken in v:
                            v = v.replace(ktoken, rep)
                    c.value = v

        # sortare pe dată+oră
        g = g.sort_values("__timestamp__", kind="mergesort")

        # scriem rândurile
        start_row = row_first_tpl
        # primul rând: suprascrie rândul model
        if not g.empty:
            row = start_row
            ws.cell(row=row, column=1, value=str(g.iloc[0]["__site__"] or ""))
            ws.cell(row=row, column=2, value=str(g.iloc[0]["__tid__"]))
            ws.cell(row=row, column=3, value=str(g.iloc[0]["__prodname__"]))
            c_amt = ws.cell(row=row, column=4, value=float(g.iloc[0]["__amount__"]))
            c_amt.number_format = "#,##0.00"
            c_dt = ws.cell(row=row, column=5, value=g.iloc[0]["__timestamp__"])
            c_dt.number_format = "yyyy-mm-dd hh:mm:ss"
            # păstrăm formatările model
            apply_row_styles(ws, row, data_styles, data_height)

        # restul rândurilor – inserăm sub model și copiem stilurile
        for i in range(1, len(g)):
            row = start_row + i
            ws.insert_rows(row)
            apply_row_styles(ws, row, data_styles, data_height)
            ws.cell(row=row, column=1, value=str(g.iloc[i]["__site__"] or ""))
            ws.cell(row=row, column=2, value=str(g.iloc[i]["__tid__"]))
            ws.cell(row=row, column=3, value=str(g.iloc[i]["__prodname__"]))
            c_amt = ws.cell(row=row, column=4, value=float(g.iloc[i]["__amount__"]))
            c_amt.number_format = "#,##0.00"
            c_dt = ws.cell(row=row, column=5, value=g.iloc[i]["__timestamp__"])
            c_dt.number_format = "yyyy-mm-dd hh:mm:ss"

        # rândul TOTAL – poziția lui inițială se mută odată cu inserările
        total_row = row_total_tpl + max(0, len(g) - 1)
        # ne asigurăm că scriem eticheta și suma, cu stilul din șablon
        apply_row_styles(ws, total_row, total_styles, total_height)
        ws.cell(row=total_row, column=1, value="Total")
        c_tot = ws.cell(row=total_row, column=4, value=float(total))
        c_tot.number_format = "#,##0.00"

        # denumire fișier – CLIENT în nume
        safe_client = re.sub(r'[\\/*?:"<>|]', "_", str(r0["__client__"] or "CLIENT"))
        out_xlsx = os.path.join(out_dir, f"Ordin - {safe_client}.xlsx")
        wb.save(out_xlsx)

    # facem ZIP
    with zipfile.ZipFile(out_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fn in sorted(os.listdir(out_dir)):
            if fn.lower().endswith(".xlsx"):
                zf.write(os.path.join(out_dir, fn), arcname=fn)

    return True


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--astob", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--template", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--out-zip", required=True)
    args = ap.parse_args()

    ok = run_generator(args.astob, args.key, args.template, args.out_dir, args.out_zip)
    print("ok" if ok else "fail")


if __name__ == "__main__":
    main()
