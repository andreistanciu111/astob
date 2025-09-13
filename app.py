# app.py
from fastapi import FastAPI
from pydantic import BaseModel
from base64 import b64decode, b64encode
from io import BytesIO
import tempfile
import os

from generate_orders import generate_zip_from_xlsx_bytes  # vezi fișierul de mai jos

app = FastAPI()

class B64Payload(BaseModel):
    astob_b64: str
    key_b64: str
    astob_name: str | None = None
    key_name: str | None = None
    # Dacă la un moment dat vrei alt șablon, poți trimite explicit acest câmp din n8n.
    template_path: str | None = "static/bp model cu {} - date.xlsx"


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/generate_b64")
def generate_b64(p: B64Payload):
    # decode fișierele
    astob_bytes = b64decode(p.astob_b64)
    key_bytes   = b64decode(p.key_b64)

    # creează ZIP în memorie folosind șablonul (din repo)
    zip_bytes = generate_zip_from_xlsx_bytes(
        astob_bytes=astob_bytes,
        key_bytes=key_bytes,
        template_path=p.template_path or "static/bp model cu {} - date.xlsx",
    )

    # răspunsul: JSON cu zip în base64 (n8n -> Convert to File)
    return {
        "ok": True,
        "filename": "ordine.zip",
        "content_type": "application/zip",
        "zip_b64": b64encode(zip_bytes).decode("utf-8"),
    }
