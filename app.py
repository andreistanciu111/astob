import tempfile, shutil, subprocess, sys
from pathlib import Path
from typing import Optional
from base64 import b64decode
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI(title="ASTOB Orders Generator")

@app.get("/")
def root():
    return {"ok": True, "endpoints": ["/health", "POST /generate", "POST /generate_b64"]}

@app.get("/health")
def health():
    return {"ok": True}

def run_generator(astob_path: Path, key_path: Path, template_path: Path|None, clients_zip_path: Path|None):
    tmpdir = astob_path.parent
    out_dir = tmpdir / "out_excel"
    out_zip = tmpdir / "ordine.zip"

    if template_path is None:
        template_path = Path("static") / "bp model cu {}.xlsx"
        if not template_path.exists():
            return JSONResponse(
                {"ok": False, "error": "Template missing. Upload 'template' or add to ./static/bp model cu {}.xlsx"},
                status_code=400
            )

    cmd = [
        sys.executable, "generate_orders.py",
        "--astob", str(astob_path),
        "--key", str(key_path),
        "--template", str(template_path),
        "--out-dir", str(out_dir),
        "--out-zip", str(out_zip),
    ]
    if clients_zip_path:
        cmd += ["--clients-zip", str(clients_zip_path)]

    proc = subprocess.run(cmd, capture_output=True, text=True)

    if proc.returncode != 0:
        return JSONResponse({
            "ok": False,
            "error": "Generator failed",
            "stderr": proc.stderr,
            "stdout": proc.stdout,
            "cmd": cmd
        }, status_code=500)

    if not out_zip.exists():
        listing = [str(p.relative_to(tmpdir)) for p in tmpdir.rglob("*")]
        return JSONResponse({
            "ok": False,
            "error": "ZIP not produced",
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "cmd": cmd,
            "tmp_listing": listing
        }, status_code=500)

    def iterfile(path: Path):
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                yield chunk

    headers = {"Content-Disposition": 'attachment; filename="ordine.zip"'}
    return StreamingResponse(iterfile(out_zip), media_type="application/zip", headers=headers)

@app.post("/generate")
async def generate(
    astob: UploadFile = File(..., description="ASTOB file (.xlsx/.xls/.csv)"),
    key: UploadFile = File(..., description="TABEL CHEIE (.xlsx/.xls/.csv)"),
    template: Optional[UploadFile] = File(None, description="bp model cu {}.xlsx (optional)"),
    clients_zip: Optional[UploadFile] = File(None, description="CLIENTI_...zip (optional)"),
):
    tmpdir = Path(tempfile.mkdtemp(prefix="astobgen_"))
    try:
        # salvează cu extensiile originale
        astob_path = tmpdir / f"astob{Path(astob.filename or 'astob.xlsx').suffix or '.xlsx'}"
        key_path   = tmpdir / f"key{Path(key.filename or 'key.xlsx').suffix or '.xlsx'}"
        with open(astob_path, "wb") as f: f.write(await astob.read())
        with open(key_path, "wb") as f:   f.write(await key.read())

        template_path = None
        if template is not None:
            template_path = tmpdir / "template.xlsx"
            with open(template_path, "wb") as f: f.write(await template.read())

        clients_zip_path = None
        if clients_zip is not None:
            clients_zip_path = tmpdir / "clients.zip"
            with open(clients_zip_path, "wb") as f: f.write(await clients_zip.read())
        else:
            packaged = Path("static") / "CLIENTI_TOTAL_PESTE_0_ONE_ROW_HEADERS_SINGLE_DETAILS_TOTAL_TOP.zip"
            if packaged.exists():
                clients_zip_path = packaged

        resp = run_generator(astob_path, key_path, template_path, clients_zip_path)
        return resp
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

@app.post("/generate_b64")
async def generate_b64(payload: dict):
    """
    Body JSON:
    {
      "astob_b64": "...", "astob_name": "ASTOB SRL (7).xlsx",
      "key_b64": "...",   "key_name":   "TABEL CHEIE.xlsx",
      // optional:
      "template_b64": "...", "template_name": "bp model cu {}.xlsx",
      "clients_zip_b64": "...", "clients_zip_name": "CLIENTI_....zip"
    }
    """
    required = ["astob_b64", "astob_name", "key_b64", "key_name"]
    if not all(k in payload for k in required):
        return JSONResponse({"ok": False, "error": f"Missing keys. Required: {required}"}, status_code=400)

    tmpdir = Path(tempfile.mkdtemp(prefix="astobgen_"))
    try:
        # decode and write
        def write_b64(name_key, b64_key, fallback_name):
            name = payload.get(name_key) or fallback_name
            path = tmpdir / name
            data = b64decode(payload[b64_key])
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "wb") as f: f.write(data)
            return path

        astob_path = write_b64("astob_name", "astob_b64", "astob.xlsx")
        key_path   = write_b64("key_name", "key_b64", "key.xlsx")

        template_path = None
        if payload.get("template_b64"):
            template_path = write_b64("template_name", "template_b64", "template.xlsx")

        clients_zip_path = None
        if payload.get("clients_zip_b64"):
            clients_zip_path = write_b64("clients_zip_name", "clients_zip_b64", "clients.zip")
        else:
            packaged = Path("static") / "CLIENTI_TOTAL_PESTE_0_ONE_ROW_HEADERS_SINGLE_DETAILS_TOTAL_TOP.zip"
            if packaged.exists():
                clients_zip_path = packaged

        resp = run_generator(astob_path, key_path, template_path, clients_zip_path)
        return resp
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
