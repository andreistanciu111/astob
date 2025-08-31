
import tempfile, os
from pathlib import Path
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
import subprocess, sys

app = FastAPI(title="ASTOB Orders Generator")

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/generate")
async def generate(
    astob: UploadFile = File(..., description="ASTOB SRL (7).xlsx"),
    key: UploadFile = File(..., description="TABEL CHEIE.xlsx"),
    template: UploadFile | None = File(None, description="bp model cu {}.xlsx (optional)"),
    clients_zip: UploadFile | None = File(None, description="CLIENTI_...zip (optional)"),
):
    tmpdir = Path(tempfile.mkdtemp(prefix="astobgen_"))
    try:
        astob_path = tmpdir / "astob.xlsx"
        key_path = tmpdir / "key.xlsx"
        with open(astob_path, "wb") as f:
            f.write(await astob.read())
        with open(key_path, "wb") as f:
            f.write(await key.read())

        # Resolve template
        if template is not None:
            template_path = tmpdir / "template.xlsx"
            with open(template_path, "wb") as f:
                f.write(await template.read())
        else:
            template_path = Path("static") / "bp model cu {}.xlsx"
            if not template_path.exists():
                return JSONResponse(
                    {"ok": False, "error": "Template missing. Upload 'template' or add it to ./static/bp model cu {}.xlsx"},
                    status_code=400
                )

        # Optional clients zip
        clients_zip_path = None
        if clients_zip is not None:
            clients_zip_path = tmpdir / "clients.zip"
            with open(clients_zip_path, "wb") as f:
                f.write(await clients_zip.read())
        else:
            packaged = Path("static") / "Ordine_Plata_ Astob.zip"
            if packaged.exists():
                clients_zip_path = packaged

        out_dir = tmpdir / "out_excel"
        out_zip = tmpdir / "ordine.zip"

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
    from pathlib import Path
    listing = [str(p.relative_to(tmpdir)) for p in Path(tmpdir).rglob('*')]
    return JSONResponse({
        "ok": False,
        "error": "ZIP not produced",
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "cmd": cmd,
        "tmp_listing": listing
    }, status_code=500)

        def iterfile():
            with open(out_zip, "rb") as f:
                yield from f

        headers = {"Content-Disposition": 'attachment; filename="ordine.zip"'}
        return StreamingResponse(iterfile(), media_type="application/zip", headers=headers)
    finally:
        try:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass
