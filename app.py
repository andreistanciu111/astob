# app.py
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from base64 import b64decode, b64encode
from io import BytesIO
from typing import Optional

from generate_orders import generate_zip_from_xlsx_bytes  # îl ai deja

app = FastAPI(title="ASTOB Orders")

class B64Payload(BaseModel):
    astob_b64: str
    key_b64: str
    astob_name: Optional[str] = None
    key_name: Optional[str] = None
    template_path: Optional[str] = "static/bp model cu {} - date.xlsx"

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/generate_b64")
def generate_b64(p: B64Payload):
    try:
        astob_bytes = b64decode(p.astob_b64)
        key_bytes   = b64decode(p.key_b64)
        tpl_path    = p.template_path or "static/bp model cu {} - date.xlsx"

        zip_bytes = generate_zip_from_xlsx_bytes(
            astob_bytes=astob_bytes,
            key_bytes=key_bytes,
            template_path=tpl_path,
        )
        return JSONResponse(
            content={
                "ok": True,
                "filename": "ordine.zip",
                "content_type": "application/zip",
                "zip_b64": b64encode(zip_bytes).decode("utf-8"),
            },
            media_type="application/json",
        )
    except Exception as e:
        # răspuns JSON chiar și pe eroare
        return JSONResponse(
            content={"ok": False, "error": "Generator failed"},
            status_code=500,
            media_type="application/json",
        )
