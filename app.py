import tempfile, base64, json, subprocess, sys
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Body
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI(title="ASTOB Orders Generator")

def pick_template_path(tmpdir: Path, uploaded_bytes: bytes | None) -> Path | None:
    if uploaded_bytes:
        p = tmpdir / "template.xlsx"
        p.write_bytes(uploaded_bytes)
        return p
    static_dir = Path("static")
    # acceptă orice nume care începe cu "bp model cu {}"
    cands = sorted(static_dir.glob("bp model cu {}*.xlsx"),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None

def run_generator(astob_path: Path, key_path: Path, template_path: Path,
                  out_dir: Path, out_zip: Path) -> tuple[int, str, str]:
    cmd = [
        sys.executable, "generate_orders.py",
        "--astob", str(astob_path),
        "--key", str(key_path),
        "--template", str(template_path),
        "--out-dir", str(out_dir),
        "--out-zip", str(out_zip),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/generate")
async def generate(
    astob: UploadFile = File(..., description="ASTOB SRL .xlsx"),
    key: UploadFile   = File(..., description="TABEL CHEIE .xlsx"),
    template: UploadFile | None = File(None, description="(optional) bp model cu {}*.xlsx"),
):
    tmpdir = Path(tempfile.mkdtemp(prefix="astobgen_"))
    try:
        astob_path = tmpdir / (astob.filename or "astob.xlsx")
        key_path   = tmpdir / (key.filename   or "key.xlsx")
        astob_path.write_bytes(await astob.read())
        key_path.write_bytes(await key.read())

        template_bytes = await template.read() if template is not None else None
        template_path = pick_template_path(tmpdir, template_bytes)
        if not template_path or not template_path.exists():
            return JSONResponse({"ok": False, "error": "Template missing. Put file like 'static/bp model cu {}*.xlsx' or upload 'template'."}, status_code=400)

        out_dir = tmpdir / "out_excel"
        out_zip = tmpdir / "ordine.zip"
        rc, out, err = run_generator(astob_path, key_path, template_path, out_dir, out_zip)
        if rc != 0 or not out_zip.exists():
            return JSONResponse({"ok": False, "error": "Generator failed", "stdout": out, "stderr": err}, status_code=500)

        def iterfile():
            with open(out_zip, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    yield chunk

        headers = {"Content-Disposition": 'attachment; filename="ordine.zip"'}
        return StreamingResponse(iterfile(), media_type="application/zip", headers=headers)
    finally:
        import shutil; shutil.rmtree(tmpdir, ignore_errors=True)

@app.post("/generate_b64")
def generate_b64(payload: dict = Body(...)):
    """
    Așteaptă JSON cu:
      astob_b64, key_b64  (obligatorii)
      astob_name, key_name (opționale)
      template_b64 (opțional)
    """
    tmpdir = Path(tempfile.mkdtemp(prefix="astobgen_"))
    try:
        # decode inputs
        try:
            astob_bytes = base64.b64decode(payload["astob_b64"])
            key_bytes   = base64.b64decode(payload["key_b64"])
        except Exception:
            return JSONResponse({"ok": False, "error": "Invalid or missing base64 fields 'astob_b64'/'key_b64'."}, status_code=400)

        astob_name = payload.get("astob_name") or "ASTOB.xlsx"
        key_name   = payload.get("key_name")   or "TABEL CHEIE.xlsx"
        template_b64 = payload.get("template_b64")

        astob_path = tmpdir / astob_name
        key_path   = tmpdir / key_name
        astob_path.write_bytes(astob_bytes)
        key_path.write_bytes(key_bytes)

        template_bytes = base64.b64decode(template_b64) if template_b64 else None
        template_path = pick_template_path(tmpdir, template_bytes)
        if not template_path or not template_path.exists():
            return JSONResponse({"ok": False, "error": "Template missing. Put file like 'static/bp model cu {}*.xlsx' or send 'template_b64'."}, status_code=400)

        out_dir = tmpdir / "out_excel"
        out_zip = tmpdir / "ordine.zip"
        rc, out, err = run_generator(astob_path, key_path, template_path, out_dir, out_zip)
        if rc != 0 or not out_zip.exists():
            return JSONResponse({"ok": False, "error": "Generator failed", "stdout": out, "stderr": err}, status_code=500)

        data = out_zip.read_bytes()
        headers = {"Content-Disposition": 'attachment; filename="ordine.zip"'}
        return StreamingResponse(iter(lambda: (data[i:i+1024*1024] for i in range(0, len(data), 1024*1024))(), None),
                                 media_type="application/zip", headers=headers)
    finally:
        import shutil; shutil.rmtree(tmpdir, ignore_errors=True)
