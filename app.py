# app.py
from __future__ import annotations

import base64, binascii, re
from io import BytesIO
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

from generate_orders import generate_zip_from_bytes

DEFAULT_TEMPLATE = "static/bp model cu {} - date.xlsx"

app = FastAPI(title="astob-orders")

class B64Payload(BaseModel):
    astob_b64: str
    key_b64: str
    template: Optional[str] = None

def _b64_to_bytes(s: str) -> bytes:
    """Acceptă Base64 cu prefix data:, spații, newline, urme de '={{ }}', urlsafe, padding lipsă/în exces."""
    if not isinstance(s, str):
        raise HTTPException(status_code=400, detail="Base64 invalid: not a string")

    # decupează ghilimele, spații, newline
    s = s.strip().strip('"').strip("'")
    # uneori vine cu '=' la început (ex. din anumite node-uri / copy-paste)
    if s.startswith("="):
        s = s[1:].lstrip()
    # data URL: data:...;base64,xxxx
    if "," in s and s.lower().startswith("data:"):
        s = s.split(",", 1)[1]
    # scoate spații/newlines
    s = re.sub(r"\s+", "", s)
    # urlsafe -> standard
    s = s.replace("-", "+").replace("_", "/")
    # completează padding la multiplu de 4
    if len(s) % 4:
        s += "=" * (4 - (len(s) % 4))

    try:
        return base64.b64decode(s, validate=False)
    except binascii.Error as e:
        raise HTTPException(status_code=400, detail=f"Base64 invalid: {e}")

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/generate_b64")
def generate_b64(payload: B64Payload):
    tpl = payload.template or DEFAULT_TEMPLATE
    astob_bytes = _b64_to_bytes(payload.astob_b64)
    key_bytes   = _b64_to_bytes(payload.key_b64)
    try:
        zip_bytes = generate_zip_from_bytes(astob_bytes, key_bytes, tpl)
    except Exception as e:
        return JSONResponse({"ok": False, "error": "Generator failed", "details": str(e)}, status_code=200)
    return {"ok": True, "zip_b64": base64.b64encode(zip_bytes).decode("ascii")}

@app.post("/generate")
async def generate(astob: UploadFile = File(...), key: UploadFile = File(...), template: Optional[str] = None):
    tpl = template or DEFAULT_TEMPLATE
    try:
        zip_bytes = generate_zip_from_bytes(await astob.read(), await key.read(), tpl)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Generator failed: {e}")
    return StreamingResponse(BytesIO(zip_bytes),
                             media_type="application/zip",
                             headers={"Content-Disposition": 'attachment; filename="ordine.zip"'})
