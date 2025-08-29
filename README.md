
# ASTOB Orders HTTP Service (v2)

FastAPI wrapper around `generate_orders.py`.
- `GET /health` → {"ok": true}
- `POST /generate` (multipart/form-data)
  fields:
    - astob (file)  : ASTOB SRL (7).xlsx  [required]
    - key   (file)  : TABEL CHEIE.xlsx    [required]
    - template (file)  : bp model cu {}.xlsx [optional if placed in ./static]
    - clients_zip (file): CLIENTI_...zip     [optional]
  response: application/zip (all orders)

## Deploy
Render/Railway/Cloud Run
- Build:  pip install -r requirements.txt
- Start:  uvicorn app:app --host 0.0.0.0 --port $PORT

## Notes
Place your template at: ./static/bp model cu {}.xlsx (exact name).
