from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import gzip, json, random, datetime, threading
from pathlib import Path
from typing import Optional

app = FastAPI(title="Finonex Payments API (Cached + Gzip)")

FILE_PATH = Path("payments.jsonl.gz")   # Compressed file
STATUS_TRANSITIONS = {"pending": ["approved", "declined"]}

DATA_CACHE = []  # In-memory cache


@app.on_event("startup")
def load_data_once():
    """Load the compressed JSONL file once on startup."""
    global DATA_CACHE
    DATA_CACHE = []

    if not FILE_PATH.exists():
        raise FileNotFoundError(f"File not found: {FILE_PATH}")

    open_func = gzip.open if FILE_PATH.suffix == ".gz" else open

    with open_func(FILE_PATH, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                DATA_CACHE.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Skipping invalid line: {e}")
                continue

    print(f"✅ Loaded {len(DATA_CACHE)} payment records from {FILE_PATH.name}.")


def save_data_async():
    """Persist cached data back to compressed file asynchronously."""
    def _write():
        open_func = gzip.open if FILE_PATH.suffix == ".gz" else open
        with open_func(FILE_PATH, "wt", encoding="utf-8") as f:
            for record in DATA_CACHE:
                f.write(json.dumps(record) + "\n")

    threading.Thread(target=_write, daemon=True).start()


def maybe_update_status(record):
    """One-way status update simulation."""
    if record["status"] == "pending" and random.random() < 0.3:
        record["status"] = random.choice(STATUS_TRANSITIONS["pending"])
        record["status_updated_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    return record


@app.get("/payments")
def get_payments(
    date: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    psp_name: Optional[str] = None,
    status: Optional[str] = None,
):
    """Cached, gzipped payments API with filters and pagination."""
    global DATA_CACHE

    data = DATA_CACHE
    if date:
        data = [p for p in data if p["processing_date"].startswith(date)]
    if psp_name:
        data = [p for p in data if p["psp_name"].lower() == psp_name.lower()]
    if status:
        data = [p for p in data if p["status"].lower() == status.lower()]

    updated_any = False
    for record in data:
        old_status = record["status"]
        maybe_update_status(record)
        if record["status"] != old_status:
            updated_any = True

    if updated_any:
        save_data_async()

    start, end = (page - 1) * limit, (page * limit)
    page_data = data[start:end]

    if not page_data:
        raise HTTPException(status_code=404, detail="No more records")

    return JSONResponse({
        "date": date,
        "page": page,
        "limit": limit,
        "count": len(page_data),
        "data": page_data
    })


@app.get("/payments/{txn_id}")
def get_payment(txn_id: str):
    global DATA_CACHE
    for record in DATA_CACHE:
        if record["id"] == txn_id:
            old = record["status"]
            maybe_update_status(record)
            if record["status"] != old:
                save_data_async()
            return record
    raise HTTPException(status_code=404, detail="Transaction not found")
