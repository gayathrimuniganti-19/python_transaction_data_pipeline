#!/usr/bin/env python3
"""Bounded-memory transaction ingestion with SQLite-backed audit/outbox records."""
from __future__ import annotations
import argparse, csv, json, math, os, sqlite3, sys, tempfile, time, uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

FIELDS = ["transaction_id", "customer_id", "amount", "currency", "transaction_timestamp", "status"]
STATUSES = {"PENDING", "COMPLETED", "FAILED", "REFUNDED"}
ACCEPTED_FIELDS = ["run_id"] + FIELDS
REJECT_FIELDS = ["run_id", "source_file", "source_row", "reason"] + FIELDS


def stream_records(path: Path) -> Iterator[tuple[int, dict[str, Any] | None, str | None]]:
    """Yield one source record at a time; JSON arrays require optional ijson."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("CSV has no header row")
            missing = [x for x in FIELDS if x not in reader.fieldnames]
            if missing:
                raise ValueError("CSV missing required columns: " + ", ".join(missing))
            for n, row in enumerate(reader, 2):
                if None in row:  # Extra values beyond the declared header.
                    yield n, None, "malformed_csv: extra fields"
                else:
                    yield n, row, None
    elif suffix in {".jsonl", ".ndjson"}:
        with path.open("r", encoding="utf-8") as f:
            for n, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                    if not isinstance(obj, dict):
                        yield n, None, "JSON line must be an object"
                    else:
                        yield n, obj, None
                except json.JSONDecodeError as e:
                    yield n, None, f"malformed_json: {e.msg}"
    elif suffix == ".json":
        try:
            import ijson
        except ImportError as e:
            raise RuntimeError("JSON array input requires ijson; install requirements.txt") from e
        with path.open("rb") as f:
            for n, obj in enumerate(ijson.items(f, "item"), 1):
                yield (n, obj, None) if isinstance(obj, dict) else (n, None, "JSON array element must be an object")
    else:
        raise ValueError("Unsupported input extension; use CSV, JSONL, NDJSON, or JSON")


def validate(record: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    missing = [k for k in FIELDS if record.get(k) is None or str(record.get(k)).strip() == ""]
    if missing:
        return None, "missing_required:" + ",".join(missing)
    try:
        amount = float(record["amount"])
    except (TypeError, ValueError):
        return None, "amount_must_be_numeric"
    if not math.isfinite(amount):
        return None, "amount_must_be_finite"
    if amount < 0:
        return None, "amount_must_be_non_negative"
    currency = str(record["currency"]).strip().upper()
    if len(currency) != 3 or not currency.isalpha():
        return None, "currency_must_be_three_letters"
    status = str(record["status"]).strip().upper()
    if status not in STATUSES:
        return None, "invalid_status"
    timestamp = str(record["transaction_timestamp"]).strip()
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return None, "invalid_iso8601_timestamp"
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None, "timestamp_must_include_timezone"
    return {"transaction_id": str(record["transaction_id"]).strip(),
            "customer_id": str(record["customer_id"]).strip(), "amount": amount,
            "currency": currency, "transaction_timestamp": timestamp, "status": status}, None


def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS customers(customer_id TEXT PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS transactions(
      transaction_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL,
      amount REAL NOT NULL CHECK(amount >= 0), currency TEXT NOT NULL,
      transaction_timestamp TEXT NOT NULL, status TEXT NOT NULL,
      first_loaded_at TEXT NOT NULL,
      FOREIGN KEY(customer_id) REFERENCES customers(customer_id));
    CREATE TABLE IF NOT EXISTS run_keys(run_id TEXT NOT NULL, transaction_id TEXT NOT NULL, PRIMARY KEY(run_id, transaction_id));
    CREATE TABLE IF NOT EXISTS run_records(
      run_id TEXT NOT NULL, source_row INTEGER NOT NULL, outcome TEXT NOT NULL,
      reason TEXT, record_json TEXT NOT NULL,
      PRIMARY KEY(run_id, source_row));
    CREATE TABLE IF NOT EXISTS runs(
      run_id TEXT PRIMARY KEY, source_file TEXT NOT NULL, status TEXT NOT NULL,
      started_at TEXT NOT NULL, finished_at TEXT, report_json TEXT);
    """)
    conn.commit()
    return conn


def load_customers(conn: sqlite3.Connection, path: Path | None) -> bool:
    """Load customer master if provided; return whether FK validation is enabled."""
    if path is None:
        return False
    if not path.is_file():
        raise FileNotFoundError(f"Customer master not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "customer_id" not in reader.fieldnames:
            raise ValueError("Customer master CSV must contain customer_id header")
        with conn:
            for row in reader:
                cid = (row.get("customer_id") or "").strip()
                if cid:
                    conn.execute("INSERT OR IGNORE INTO customers(customer_id) VALUES (?)", (cid,))
    return True


def _atomic_csv(path: Path, fields: list[str], rows: Iterator[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try: os.unlink(tmp_name)
        except OSError: pass
        raise


def run_pipeline(input_path: Path, output_dir: Path, db_path: Path, batch_size: int = 1000,
                 customers_file: Path | None = None) -> dict[str, Any]:
    if batch_size < 1:
        raise ValueError("batch-size must be at least 1")
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    accepted_path, rejected_path, report_path = (output_dir / x for x in ("accepted.csv", "rejected.csv", "report.json"))
    run_id, started = str(uuid.uuid4()), time.perf_counter()
    started_at = datetime.now().astimezone().isoformat()
    counts = {k: 0 for k in ("input_records", "accepted_records", "rejected_records", "invalid_records",
              "duplicate_in_input", "already_loaded", "malformed_records", "unknown_customer")}
    errors: list[str] = []
    conn = init_db(db_path)
    complete = False
    try:
        enforce_customers = load_customers(conn, customers_file)
        conn.execute("INSERT INTO runs(run_id,source_file,status,started_at) VALUES (?,?,?,?)",
                     (run_id, str(input_path), "RUNNING", started_at)); conn.commit()
        batch: list[tuple[int, dict[str, Any] | None, str | None]] = []

        def process(items):
            now = datetime.now().astimezone().isoformat()
            with conn:  # Each batch's target changes and audit/outbox rows commit together.
                for row_num, record, parse_error in items:
                    counts["input_records"] += 1
                    outcome, reason, clean = "REJECTED", parse_error, None
                    if parse_error:
                        counts["malformed_records"] += 1
                    else:
                        clean, reason = validate(record or {})
                        if reason:
                            counts["invalid_records"] += 1
                        elif enforce_customers and not conn.execute("SELECT 1 FROM customers WHERE customer_id=?", (clean["customer_id"],)).fetchone():
                            reason = "unknown_customer_reference"; counts["unknown_customer"] += 1
                        else:
                            try:
                                conn.execute("INSERT INTO run_keys(run_id,transaction_id) VALUES (?,?)", (run_id, clean["transaction_id"]))
                            except sqlite3.IntegrityError:
                                reason = "duplicate_in_input"; counts["duplicate_in_input"] += 1
                        if clean is not None and reason is None:
                            try:
                                if not enforce_customers:
                                    conn.execute("INSERT OR IGNORE INTO customers(customer_id) VALUES (?)", (clean["customer_id"],))
                                conn.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?)",
                                  (clean["transaction_id"], clean["customer_id"], clean["amount"], clean["currency"], clean["transaction_timestamp"], clean["status"], now))
                                outcome = "ACCEPTED"; counts["accepted_records"] += 1
                            except sqlite3.IntegrityError:
                                reason = "already_loaded"; counts["already_loaded"] += 1
                    if outcome == "REJECTED": counts["rejected_records"] += 1
                    payload = clean if clean is not None else (record or {})
                    conn.execute("INSERT INTO run_records VALUES (?,?,?,?,?)",
                      (run_id, row_num, outcome, reason or "", json.dumps(payload, ensure_ascii=False, default=str)))

        for item in stream_records(input_path):
            batch.append(item)
            if len(batch) >= batch_size:
                process(batch); batch = []
        if batch: process(batch)
        complete = True
    except Exception as exc:
        errors.append(f"{type(exc).__name__}: {exc}")
    duration = time.perf_counter() - started
    reconciliation = complete and counts["input_records"] == counts["accepted_records"] + counts["rejected_records"]
    report = {"run_id": run_id, "source_file": str(input_path), "started_at": started_at,
      "finished_at": datetime.now().astimezone().isoformat(), "duration_seconds": round(duration, 4),
      "records_per_second": round(counts["input_records"] / duration, 2) if duration else 0,
      "batch_size": batch_size, "counts": counts, "completed": complete,
      "reconciliation_passed": reconciliation, "error_count": len(errors), "errors": errors,
      "customer_reference_validation": "enabled" if customers_file else "not_configured",
      "outputs": {"accepted_csv": str(accepted_path), "rejected_csv": str(rejected_path), "database": str(db_path)}}
    try:
        # Materialize outputs from durable SQLite audit rows, not transient buffers.
        rows = conn.execute("SELECT source_row,outcome,reason,record_json FROM run_records WHERE run_id=? ORDER BY source_row", (run_id,)).fetchall()
        accepted = ({"run_id": run_id, **json.loads(r[3])} for r in rows if r[1] == "ACCEPTED")
        rejected = ({"run_id": run_id, "source_file": input_path.name, "source_row": r[0], "reason": r[2], **json.loads(r[3])} for r in rows if r[1] == "REJECTED")
        _atomic_csv(accepted_path, ACCEPTED_FIELDS, accepted)
        _atomic_csv(rejected_path, REJECT_FIELDS, rejected)
        with conn:
            conn.execute("UPDATE runs SET status=?,finished_at=?,report_json=? WHERE run_id=?",
                         ("COMPLETED" if complete else "FAILED", report["finished_at"], json.dumps(report), run_id))
    except Exception as exc:
        report["completed"] = False; report["reconciliation_passed"] = False
        report["errors"].append(f"output_publish: {type(exc).__name__}: {exc}")
        report["error_count"] = len(report["errors"])
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    conn.close()
    return report


def main() -> int:
    p = argparse.ArgumentParser(description="Stream, validate, and publish transaction data")
    p.add_argument("--input", required=True); p.add_argument("--output-dir", default="output")
    p.add_argument("--db-path", default="output/transactions.db"); p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--customers", help="Optional customer master CSV with customer_id column")
    a = p.parse_args()
    try:
        report = run_pipeline(Path(a.input), Path(a.output_dir), Path(a.db_path), a.batch_size,
                              Path(a.customers) if a.customers else None)
    except Exception as e:
        print(f"FATAL: {type(e).__name__}: {e}", file=sys.stderr); return 2
    print(json.dumps(report, indent=2))
    return 0 if report.get("completed") and report.get("reconciliation_passed") else 1

if __name__ == "__main__": raise SystemExit(main())
