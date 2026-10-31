#!/usr/bin/env python3
"""Generate deterministic synthetic transaction data for repeatable testing."""
import argparse
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

FIELDS = ["transaction_id", "customer_id", "amount", "currency", "transaction_timestamp", "status"]
STATUSES = ["PENDING", "COMPLETED", "FAILED", "REFUNDED"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--output", default="data/sample.csv")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--format", choices=["csv", "jsonl"], default=None)
    parser.add_argument("--inject-errors", action="store_true", help="Add a few intentional bad rows")
    parser.add_argument("--customers-output", default=None, help="Optional customer master CSV path")
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows must be at least 1")
    random.seed(args.seed)
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = args.format or ("jsonl" if path.suffix.lower() in {".jsonl", ".ndjson"} else "csv")
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    customer_count = max(10, args.rows // 3)
    if args.customers_output:
        customer_path = Path(args.customers_output)
        customer_path.parent.mkdir(parents=True, exist_ok=True)
        with customer_path.open("w", encoding="utf-8", newline="") as cf:
            cw = csv.writer(cf); cw.writerow(["customer_id"])
            cw.writerows([[f"CUST{i:06d}"] for i in range(1, customer_count + 1)])

    def records():
        for i in range(args.rows):
            amount = round(random.uniform(1, 5000), 2)
            record = {
                "transaction_id": f"TX{i+1:09d}",
                "customer_id": f"CUST{random.randint(1, customer_count):06d}",
                "amount": amount,
                "currency": random.choice(["INR", "USD", "EUR"]),
                "transaction_timestamp": (start + timedelta(seconds=i * 61)).isoformat(),
                "status": random.choice(STATUSES),
            }
            if args.inject_errors and i == 2:
                record["amount"] = "-5"
            if args.inject_errors and i == 5:
                record["customer_id"] = ""
            yield record
        if args.inject_errors and args.rows >= 10:
            # Duplicate the first key intentionally.
            yield {
                "transaction_id": "TX000000001", "customer_id": "CUST000001", "amount": 10,
                "currency": "INR", "transaction_timestamp": start.isoformat(), "status": "COMPLETED"
            }

    with path.open("w", encoding="utf-8", newline="") as f:
        if fmt == "csv":
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(records())
        else:
            for record in records():
                f.write(json.dumps(record) + "\n")
    print(f"Wrote synthetic dataset to {path} ({args.rows} requested rows; seed={args.seed})")


if __name__ == "__main__":
    main()
