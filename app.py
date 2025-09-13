import base64, os, tempfile, traceback
from pathlib import Path
from typing import Optional
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from generate_orders import run_generator  # folosește scriptul tău actual

app = FastAPI(title="ASTOB Orders Generator")

@app.get("/ping")
def ping():
    return {"ok": True}

class GenReq(BaseModel):
    astob_b64: str
    key_b64: str
    astob_name: Optional[str] = "ASTOB.xlsx"
    key_name: Optional[str] = "TABEL CHEIE.xlsx"
    # dacă nu trimiți template în body, îl ia din ./static
    template: Optional[str] = "static/bp model cu {} - date.xlsx"

def _write_b64(b64: str, path: Path):
    path.write_bytes(base64.b64decode(b64))

@app.post("/generate_b64")
def generate_b64(req: GenReq):
    try:
        # verifică existența șablonului local dacă nu vine în body
        tpl_path = Path(req.template)
        if not tpl_path.exists():
            return JSONResponse(
                {"ok": False, "error": f"Template missing at '{tpl_path}'"},
                status_code=400
            )

        with tempfile.TemporaryDirectory(prefix="astobgen_") as tmpdir:
            tmp = Path(tmpdir)
            astob_path = tmp / "astob.xlsx"
            key_path   = tmp / "key.xlsx"
            out_dir    = tmp / "out_excel"
            out_zip    = tmp / "ordine.zip"

            _write_b64(req.astob_b64, astob_path)
            _write_b64(req.key_b64,   key_path)

            ok = run_generator(str(astob_path), str(key_path), str(tpl_path), str(out_dir), str(out_zip))
            if not ok or not out_zip.exists():
                return JSONResponse({"ok": False, "error": "Generator failed"}, status_code=500)

            # returnăm direct fișierul ZIP (n8n -> Response Format: File)
            return FileResponse(
                path=str(out_zip),
                media_type="application/zip",
                filename="ordine.zip"
            )
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "Generator failed", "stderr": traceback.format_exc()},
            status_code=500
        )
