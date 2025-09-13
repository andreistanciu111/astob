# app.py
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from base64 import b64decode, b64encode
from typing import Optional
from generate_orders import generate_zip_from_bytes

app = FastAPI(title="ASTOB Orders")

class B64Payload(BaseModel):
    astob_b64: str
    key_b64: str
    template_path: Optional[str] = "static/bp model cu {} - date.xlsx"

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/generate_b64")
def generate_b64(p: B64Payload):
    try:
        astob_bytes = b64decode(p.astob_b64)
        key_bytes   = b64decode(p.key_b64)
        zip_bytes   = generate_zip_from_bytes(astob_bytes, key_bytes, p.template_path or "static/bp model cu {} - date.xlsx")
        return JSONResponse({
            "ok": True,
            "filename": "ordine.zip",
            "content_type": "application/zip",
            "zip_b64": b64encode(zip_bytes).decode("utf-8"),
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": "Generator failed", "details": str(e)}, status_code=500)
