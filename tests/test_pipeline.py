import csv, json, sqlite3
from pathlib import Path
import pytest
from pipeline import FIELDS, run_pipeline


def row(**changes):
    data = dict(transaction_id="TX1", customer_id="C1", amount="10.5", currency="INR",
                transaction_timestamp="2025-01-01T00:00:00+00:00", status="COMPLETED")
    data.update(changes); return data


def write_csv(path, rows, fields=FIELDS):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as f: return list(csv.DictReader(f))


def test_valid_invalid_and_reconciliation(tmp_path):
    src=tmp_path/"in.csv"; write_csv(src,[row(),row(transaction_id="TX2",amount="-1"),row(transaction_id="TX3",customer_id="")])
    r=run_pipeline(src,tmp_path/"out",tmp_path/"db.sqlite",2)
    assert r["completed"] and r["reconciliation_passed"]
    assert (r["counts"]["accepted_records"],r["counts"]["rejected_records"])==(1,2)
    assert len(read_csv(tmp_path/"out"/"accepted.csv"))==1
    assert read_csv(tmp_path/"out"/"accepted.csv")[0]["run_id"]==r["run_id"]


def test_duplicate_within_file(tmp_path):
    src=tmp_path/"in.csv"; write_csv(src,[row(),row()])
    r=run_pipeline(src,tmp_path/"out",tmp_path/"db.sqlite")
    assert r["counts"]["accepted_records"]==1 and r["counts"]["duplicate_in_input"]==1


def test_rerun_idempotency(tmp_path):
    src=tmp_path/"in.csv"; write_csv(src,[row()]); db=tmp_path/"db.sqlite"
    run_pipeline(src,tmp_path/"o1",db); r=run_pipeline(src,tmp_path/"o2",db)
    assert r["counts"]["accepted_records"]==0 and r["counts"]["already_loaded"]==1
    with sqlite3.connect(db) as c: assert c.execute("select count(*) from transactions").fetchone()[0]==1


def test_jsonl_malformed_line(tmp_path):
    src=tmp_path/"in.jsonl"; src.write_text(json.dumps(row())+"\n{bad\n"+json.dumps(row(transaction_id="TX2"))+"\n")
    r=run_pipeline(src,tmp_path/"out",tmp_path/"db.sqlite")
    assert r["counts"]["input_records"]==3 and r["counts"]["malformed_records"]==1


def test_empty_csv(tmp_path):
    src=tmp_path/"empty.csv"; write_csv(src,[])
    r=run_pipeline(src,tmp_path/"out",tmp_path/"db.sqlite")
    assert r["counts"]["input_records"]==0 and r["reconciliation_passed"]


def test_customer_foreign_key_rejects_unknown(tmp_path):
    src=tmp_path/"in.csv"; write_csv(src,[row()]); master=tmp_path/"customers.csv"
    write_csv(master,[{"customer_id":"C2"}], ["customer_id"])
    r=run_pipeline(src,tmp_path/"out",tmp_path/"db.sqlite",customers_file=master)
    assert r["counts"]["unknown_customer"]==1 and r["counts"]["accepted_records"]==0
    assert read_csv(tmp_path/"out"/"rejected.csv")[0]["reason"]=="unknown_customer_reference"


@pytest.mark.parametrize("amount,reason",[("NaN","amount_must_be_finite"),("inf","amount_must_be_finite"),("-2","amount_must_be_non_negative"),("abc","amount_must_be_numeric")])
def test_amount_edge_cases(tmp_path,amount,reason):
    src=tmp_path/"in.csv"; write_csv(src,[row(amount=amount)])
    r=run_pipeline(src,tmp_path/"out",tmp_path/"db.sqlite")
    assert read_csv(tmp_path/"out"/"rejected.csv")[0]["reason"]==reason


def test_timezone_required(tmp_path):
    src=tmp_path/"in.csv"; write_csv(src,[row(transaction_timestamp="2025-01-01T00:00:00")])
    r=run_pipeline(src,tmp_path/"out",tmp_path/"db.sqlite")
    assert r["counts"]["invalid_records"]==1


def test_missing_header_fails_closed(tmp_path):
    src=tmp_path/"in.csv"; src.write_text("transaction_id,amount\nTX1,10\n")
    r=run_pipeline(src,tmp_path/"out",tmp_path/"db.sqlite")
    assert not r["completed"] and not r["reconciliation_passed"] and r["error_count"]==1


def test_bad_csv_extra_fields_are_quarantined(tmp_path):
    src=tmp_path/"in.csv"; src.write_text(",".join(FIELDS)+"\n"+",".join(["TX1","C1","10","INR","2025-01-01T00:00:00+00:00","COMPLETED","EXTRA"])+"\n")
    r=run_pipeline(src,tmp_path/"out",tmp_path/"db.sqlite")
    assert r["counts"]["malformed_records"]==1


def test_bad_batch_size(tmp_path):
    src=tmp_path/"in.csv"; write_csv(src,[])
    with pytest.raises(ValueError): run_pipeline(src,tmp_path/"out",tmp_path/"db.sqlite",0)
