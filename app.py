import base64, os, tempfile, traceback
from flask import Flask, request, send_file, jsonify, Response
from generate_orders import run_generator

app = Flask(__name__)

@app.get("/ping")
def ping():
    return jsonify({"ok": True})

def _b64_to_file(b64_str: str, dst_path: str):
    with open(dst_path, "wb") as f:
        f.write(base64.b64decode(b64_str))

@app.post("/generate_b64")
def generate_b64():
    try:
        payload = request.get_json(force=True, silent=False) or {}
        astob_b64 = payload.get("astob_b64")
        key_b64   = payload.get("key_b64")
        astob_name = payload.get("astob_name") or "ASTOB.xlsx"
        key_name   = payload.get("key_name") or "TABEL CHEIE.xlsx"
        template   = payload.get("template") or "static/bp model cu {} - date.xlsx"

        if not astob_b64 or not key_b64:
            return jsonify({"ok": False, "error": "Missing astob_b64 or key_b64"}), 400

        with tempfile.TemporaryDirectory(prefix="astobgen_") as tmp:
            astob_path = os.path.join(tmp, "astob.xlsx")
            key_path   = os.path.join(tmp, "key.xlsx")
            out_dir    = os.path.join(tmp, "out_excel")
            out_zip    = os.path.join(tmp, "ordine.zip")

            _b64_to_file(astob_b64, astob_path)
            _b64_to_file(key_b64,   key_path)

            ok = run_generator(astob_path, key_path, template, out_dir, out_zip)
            if not ok or not os.path.exists(out_zip):
                return jsonify({"ok": False, "error": "Generator failed"}), 500

            # returnez binar ca attachment
            return send_file(
                out_zip,
                mimetype="application/zip",
                as_attachment=True,
                download_name="ordine.zip",
                max_age=0,
                conditional=False,
            )
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": "Generator failed",
            "stderr": traceback.format_exc(),
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))
