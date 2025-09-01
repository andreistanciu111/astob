import tempfile
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
import subprocess, sys, shutil

app = FastAPI(title="ASTOB Orders Generator")

@app.get("/")
def root():
    return {"ok": True, "endpoints": ["/health", "POST /generate"]}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/generate")
async def generate(
    astob: UploadFile = File(..., description="ASTOB SRL (7).xlsx"),
    key: UploadFile = File(..., description="TABEL CHEIE.xlsx"),
    template: Optional[UploadFile] = File(None, description="bp model cu {}.xlsx (optional)"),
    clients_zip: Optional[UploadFile] = File(None, description="CLIENTI_...zip (optional)"),
):
    tmpdir = Path(tempfile.mkdtemp(prefix="astobgen_"))
    try:
        # 1) Salvează fișierele cu extensia originală
        astob_name = Path(astob.filename or "astob.xlsx")
        key_name   = Path(key.filename or "key.xlsx")
        astob_path = tmpdir / f"astob{(astob_name.suffix or '.xlsx')}"
        key_path   = tmpdir / f"key{(key_name.suffix or '.xlsx')}"
        with open(astob_path, "wb") as f:
            f.write(await astob.read())
        with open(key_path, "wb") as f:
            f.write(await key.read())

        # 2) Template: upload sau din ./static/bp model cu {}.xlsx
        if template is not None:
            template_path = tmpdir / "template.xlsx"
            with open(template_path, "wb") as f:
                f.write(await template.read())
        else:
            template_path = Path("static") / "bp model cu {}.xlsx"
            if not template_path.exists():
                return JSONResponse(
                    {"ok": False, "error": "Template missing. Upload 'template' or add to ./static/bp model cu {}.xlsx"},
                    status_code=400
                )

        # 3) clients_zip opțional
        clients_zip_path = None
        if clients_zip is not None:
            clients_zip_path = tmpdir / "clients.zip"
            with open(clients_zip_path, "wb") as f:
                f.write(await clients_zip.read())
        else:
            packaged = Path("static") / "CLIENTI_TOTAL_PESTE_0_ONE_ROW_HEADERS_SINGLE_DETAILS_TOTAL_TOP.zip"
            if packaged.exists():
                clients_zip_path = packaged

        # 4) Căi output
        out_dir = tmpdir / "out_excel"
        out_zip = tmpdir / "ordine.zip"

        # 5) Rulează generatorul
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

        # 6) Stream ZIP
        def iterfile(path: Path):
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk

        headers = {"Content-Disposition": 'attachment; filename=\"ordine.zip\"'}
        return StreamingResponse(iterfile(out_zip), media_type="application/zip", headers=headers)

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
