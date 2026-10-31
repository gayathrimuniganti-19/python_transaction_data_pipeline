# Scalable Transaction Ingestion & Data Quality Pipeline

A reproducible local Data Engineering implementation for the YottaFlex.ai 72-hour challenge. The pipeline ingests CSV, JSON Lines (`.jsonl` / `.ndjson`) and top-level JSON arrays with bounded-memory readers, validates transaction records, enforces optional customer referential integrity, writes accepted/rejected outputs, and stores durable audit state in SQLite.

## What it demonstrates

- **Bounded-memory ingestion:** streaming CSV/JSONL readers and `ijson` for JSON arrays; only one configured batch is held in memory.
- **Data contract:** required fields, numeric/finite/non-negative amounts, ISO-8601 timezone-aware timestamps, currency format and allowed statuses.
- **Data quality:** malformed and invalid records are quarantined with source file, row number and reason.
- **Keys and relationships:** transaction primary key; optional customer master with a foreign-key check (`--customers`). Without a supplied master, customer IDs are registered as observed source entities and referential validation is reported as not configured.
- **Idempotency:** a persistent transaction primary key prevents duplicate target inserts across runs; a per-run key table identifies duplicates inside one input.
- **Audit/outbox pattern:** each batch commits target changes and per-record run outcomes together in SQLite. CSV files are atomically materialized from the durable run ledger after processing.
- **Operational metadata:** run ID, start/end timestamps, duration, throughput, batch size, counts, errors and reconciliation status.
- **Reproducibility:** fixed-seed synthetic data generator, including an optional customer master generator.
- **Local-first:** no credentials or external services required.

## Data contract and grain

| Field | Type | Validation |
|---|---|---|
| `transaction_id` | string | Required; primary key; unique per transaction |
| `customer_id` | string | Required; must exist in supplied customer master when enabled |
| `amount` | numeric | Finite and greater than or equal to zero |
| `currency` | string | Three alphabetic characters; normalized uppercase |
| `transaction_timestamp` | ISO-8601 string | Required and timezone-aware |
| `status` | string | `PENDING`, `COMPLETED`, `FAILED`, or `REFUNDED` |

Target grain: one row per `transaction_id`. Amounts are stored as SQLite `REAL` for this demonstration; production financial systems should use integer minor units or a decimal-safe database type.

## Architecture

```text
CSV / JSONL / JSON array + optional customer master
                       |
                       v
             Streaming source reader
              (bounded batch size)
                       |
                       v
           Schema + business validation
                       |
            +----------+-----------+
            |                      |
            v                      v
     Reject/quarantine       Customer FK check
                                   |
                                   v
                       Per-run duplicate check
                                   |
                                   v
                      SQLite transaction target
                                   |
                     +-------------+-------------+
                     |                           |
                     v                           v
             Durable run ledger          Run-level report
                     |
                     v
           Atomic accepted/rejected CSV publication
```

## Setup

Python 3.10+ is recommended.

### Windows PowerShell

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick start

Generate a reproducible 1,000-row dataset and a matching customer master:

```bash
python generate_data.py --rows 1000 --seed 42 --output data/sample.csv --customers-output data/customers.csv
```

Run with customer reference validation enabled:

```bash
python pipeline.py --input data/sample.csv --customers data/customers.csv --output-dir output --db-path output/transactions.db --batch-size 500
```

Run without a customer master (customer reference validation is then explicitly marked `not_configured`):

```bash
python pipeline.py --input data/sample.csv --output-dir output --db-path output/transactions.db --batch-size 500
```

Run tests:

```bash
python -m pytest -q
```

Generate and process a larger dataset:

```bash
python generate_data.py --rows 100000 --seed 42 --output data/large.csv --customers-output data/customers_large.csv
python pipeline.py --input data/large.csv --customers data/customers_large.csv --output-dir output_large --db-path output_large/transactions.db --batch-size 2000
```

JSON Lines input:

```bash
python pipeline.py --input data/sample.jsonl --output-dir output_jsonl --db-path output_jsonl/transactions.db
```

For a top-level JSON array, install `ijson` from `requirements.txt`:

```bash
python pipeline.py --input data/transactions.json --output-dir output_json --db-path output_json/transactions.db
```

## CLI options

| Option | Required | Default | Meaning |
|---|---:|---|---|
| `--input` | Yes | — | Input CSV, JSONL, NDJSON or JSON file |
| `--output-dir` | No | `output` | Accepted/rejected CSV and report directory |
| `--db-path` | No | `output/transactions.db` | Persistent SQLite database |
| `--batch-size` | No | `1000` | Maximum input records per transaction batch |
| `--customers` | No | — | Customer master CSV containing `customer_id` |

## Output and reconciliation

- `accepted.csv`: rows inserted into the target in this run, including `run_id`.
- `rejected.csv`: invalid, malformed, duplicate-in-input, unknown-customer and already-loaded records, with source location and reason.
- `report.json`: run metadata, counts, error details and completion/reconciliation flags.
- SQLite database: `customers`, `transactions`, `run_keys`, `run_records`, and `runs` tables.

For a fully completed run, `input_records = accepted_records + rejected_records`. A failed or partially read run is marked `completed: false` and cannot pass reconciliation. A repeated transaction ID is never inserted twice into `transactions`.

## Recovery and consistency notes

Each processing batch commits transaction changes and the corresponding run-ledger outcomes in the same SQLite transaction. The accepted/rejected CSV files are generated from this durable ledger and published with atomic file replacement. This avoids relying on in-memory output buffers and makes the database the source of truth for run outcomes. A process interruption can leave a run marked `RUNNING`; inspect the `runs` table and its `run_records` before operational restart. The implementation does not automatically resume a partially consumed source file from an offset; rerun the source after investigating the incomplete run. Existing successfully committed transaction keys remain protected by the primary key.

## Testing and evidence

The automated suite covers valid/invalid reconciliation, same-file duplicates, cross-run idempotency, malformed JSONL, empty input, customer reference failures, non-finite/negative/non-numeric amounts, timezone requirements, missing headers, malformed CSV rows and invalid batch size. Run `python -m pytest -q` from the repository root.

Performance is environment-dependent. Benchmark on the target machine using the 100,000-row command above and preserve the resulting `report.json`; do not present a previous machine's throughput as a guaranteed performance claim.

## Known limitations and next steps

- SQLite is a local demonstrator; a production distributed pipeline would use a warehouse/object store and partitioned execution.
- JSON array streaming requires `ijson`; CSV and JSONL use only the standard library.
- SQLite `REAL` is not appropriate for high-precision financial accounting.
- Late-arriving events and historical corrections are not merged; use an append-only event history or explicit versioning if history matters.
- A failed run is auditable but not automatically resumed; operational restart requires rerunning/reconciling the source.
- Customer master data is optional; enable it to enforce referential integrity against an authoritative customer list.

## GitHub submission checklist

1. Create a clean virtual environment and install `requirements.txt`.
2. Run `python -m pytest -q` and retain the actual terminal output.
3. Run the sample pipeline and inspect all three output artifacts.
4. Run the 100,000-row benchmark and retain its report.
5. Confirm generated outputs, databases, caches, virtual environments and secrets are excluded by `.gitignore`.
6. Push source, tests, sample data, README and dependency file to a **public** GitHub repository.
7. Open the public repository in a private/incognito browser window and confirm it is accessible before submitting the URL.
