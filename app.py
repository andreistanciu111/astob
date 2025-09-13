import base64, tempfile, sys, subprocess
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI(title="ASTOB Orders Generator")

@app.get("/health")
def health():
    return {"ok": True}

class GenReq(BaseModel):
    astob_b64: str
    key_b64: str
    astob_name: str | None = None
    key_name: str | None = None
    # opțional: trimiți șablonul în b64; altfel îl luăm din ./static
    template_b64: str | None = None
    # opțional: pachetul de clienți
    clients_zip_b64: str | None = None
    # opțional: numele fișierului rezultat
    out_filename: str | None = "ordine.zip"

def write_b64(b64: str, path: Path):
    path.write_bytes(base64.b64decode(b64))

def pick_template_path(tmpdir: Path, template_b64: str | None) -> Path | None:
    if template_b64:
        p = tmpdir / "template.xlsx"
        write_b64(template_b64, p)
        return p
    static_dir = Path("static")
    cands = sorted(static_dir.glob("bp model cu {}*.xlsx"),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None

@app.post("/generate_b64")
def generate_b64(req: GenReq):
    tmp = Path(tempfile.mkdtemp(prefix="astobgen_"))
    try:
        astob_path = tmp / (req.astob_name or "ASTOB.xlsx")
        key_path   = tmp / (req.key_name   or "TABEL CHEIE.xlsx")
        write_b64(req.astob_b64, astob_path)
        write_b64(req.key_b64,   key_path)

        template_path = pick_template_path(tmp, req.template_b64)
        if not template_path or not template_path.exists():
            return JSONResponse(
                {"ok": False, "error": "Template missing. Add a file in ./static named like 'bp model cu {}*.xlsx' or send template_b64."},
                status_code=400
            )

        clients_zip_path = None
        if req.clients_zip_b64:
            clients_zip_path = tmp / "clients.zip"
            write_b64(req.clients_zip_b64, clients_zip_path)
        else:
            packaged = Path("static") / "CLIENTI_TOTAL_PESTE_0_ONE_ROW_HEADERS_SINGLE_DETAILS_TOTAL_TOP.zip"
            if packaged.exists():
                clients_zip_path = packaged

        out_dir = tmp / "out_excel"
        out_zip = tmp / "ordine.zip"

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
        if proc.returncode != 0 or not out_zip.exists():
            return JSONResponse(
                {"ok": False, "error": "Generator failed", "stderr": proc.stderr, "stdout": proc.stdout, "cmd": cmd},
                status_code=500
            )

        zip_b64 = base64.b64encode(out_zip.read_bytes()).decode("ascii")
        return JSONResponse({"ok": True, "filename": (req.out_filename or "ordine.zip"), "zip_b64": zip_b64})
    finally:
        import shutil; shutil.rmtree(tmp, ignore_errors=True)
