import csv
import json
from decimal import Decimal

import duckdb
import pytest

from finance_pipeline.dedup import dedup_transactions
from finance_pipeline.generate_data import COLUMNS, GeneratorConfig, generate_rows, write_csv
from finance_pipeline.ingest import read_raw_transactions
from finance_pipeline.publish import publish, run_pipeline
from finance_pipeline.transform import cast_to_curated, flatten_quarantine
from finance_pipeline.validate import REASONS_COL, validate_transactions

BASE = {
    "transaction_id": "TXN-000001",
    "account_id": "ACC-0001",
    "transaction_ts": "2025-01-01T00:00:00",
    "transaction_type": "CREDIT",
    "amount": "10.00",
    "currency": "INR",
    "status": "POSTED",
    "source_updated_ts": "2025-01-01T10:00:00",
}


def _run(spark, tmp_path, rows, name="run"):
    src = tmp_path / f"{name}.csv"
    write_csv(rows, src)
    out, q = tmp_path / name / "output", tmp_path / name / "quarantine"
    return run_pipeline(spark, src, out, q), out, q


def _query(path, sql):
    con = duckdb.connect()
    try:
        return con.execute(sql.format(p=path.as_posix())).fetchall()
    finally:
        con.close()


SEED42 = {"input_rows": 1000, "curated_rows": 870, "superseded_rows": 80,
          "quarantined_rows": 50, "reconciled": True}


def test_seed_42_manifest_and_manifest_file(spark, tmp_path):
    manifest, out, _ = _run(spark, tmp_path, generate_rows(GeneratorConfig(num_rows=1000, seed=42)))
    assert manifest == SEED42
    assert json.loads((out / "manifest.json").read_text(encoding="utf-8")) == SEED42


def test_published_parquet_has_real_types(spark, tmp_path):
    _, out, _ = _run(spark, tmp_path, generate_rows(GeneratorConfig(num_rows=1000, seed=42)))
    path = out / "curated" / "transactions.parquet"
    types = {r[0]: r[1] for r in _query(path, "DESCRIBE SELECT * FROM read_parquet('{p}')")}
    assert list(types) == list(COLUMNS)
    assert types["amount"] == "DECIMAL(18,2)"
    assert types["transaction_ts"] == "TIMESTAMP"
    assert types["source_updated_ts"] == "TIMESTAMP"


def test_published_row_counts_and_unique_sorted_ids(spark, tmp_path):
    _, out, _ = _run(spark, tmp_path, generate_rows(GeneratorConfig(num_rows=1000, seed=42)))
    cur = out / "curated" / "transactions.parquet"
    sup = out / "superseded" / "superseded.parquet"
    ids = [r[0] for r in _query(cur, "SELECT transaction_id FROM read_parquet('{p}')")]
    assert len(ids) == 870 and len(set(ids)) == 870 and ids == sorted(ids)
    assert _query(sup, "SELECT count(*) FROM read_parquet('{p}')")[0][0] == 80


def test_quarantine_csv_has_header_reasons_and_50_rows(spark, tmp_path):
    _, _, q = _run(spark, tmp_path, generate_rows(GeneratorConfig(num_rows=1000, seed=42)))
    with (q / "quarantine.csv").open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert reader.fieldnames == list(COLUMNS) + [REASONS_COL]
    assert len(rows) == 50
    assert all(r[REASONS_COL] for r in rows)


def test_published_decimal_sum_is_exact(spark, tmp_path):
    rows = [dict(BASE, transaction_id="TXN-000001", amount="0.10"),
            dict(BASE, transaction_id="TXN-000002", amount="0.20")]
    _, out, _ = _run(spark, tmp_path, rows)
    got = _query(out / "curated" / "transactions.parquet", "SELECT SUM(amount) FROM read_parquet('{p}')")[0][0]
    assert got == Decimal("0.30")


def test_publishing_twice_is_idempotent(spark, tmp_path):
    rows = generate_rows(GeneratorConfig(num_rows=300, seed=5))
    src = tmp_path / "in.csv"
    write_csv(rows, src)
    out, q = tmp_path / "output", tmp_path / "quarantine"
    first = run_pipeline(spark, src, out, q)
    cur = out / "curated" / "transactions.parquet"
    rows1 = _query(cur, "SELECT * FROM read_parquet('{p}')")
    second = run_pipeline(spark, src, out, q)
    assert first == second
    assert _query(cur, "SELECT * FROM read_parquet('{p}')") == rows1


def test_header_only_input_publishes_empty_outputs(spark, tmp_path):
    manifest, out, q = _run(spark, tmp_path, [])
    assert manifest == {"input_rows": 0, "curated_rows": 0, "superseded_rows": 0,
                        "quarantined_rows": 0, "reconciled": True}
    assert _query(out / "curated" / "transactions.parquet", "SELECT count(*) FROM read_parquet('{p}')")[0][0] == 0
    assert (q / "quarantine.csv").is_file()


def test_publish_refuses_unreconciled_counts_and_writes_nothing(spark, tmp_path):
    src = tmp_path / "in.csv"
    write_csv([BASE], src)
    valid, quarantine = validate_transactions(read_raw_transactions(spark, src))
    latest, superseded = dedup_transactions(valid)
    out, q = tmp_path / "output", tmp_path / "quarantine"
    with pytest.raises(ValueError, match="reconcile"):
        publish(cast_to_curated(latest), flatten_quarantine(quarantine),
                cast_to_curated(superseded), 5, out, q)
    assert not out.exists() and not q.exists()
