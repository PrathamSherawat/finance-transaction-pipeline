import random
import uuid

import pytest

from finance_pipeline.dedup import count_tie_conflicts, dedup_transactions
from finance_pipeline.generate_data import COLUMNS, GeneratorConfig, generate_rows, write_csv
from finance_pipeline.ingest import read_raw_transactions
from finance_pipeline.validate import validate_transactions

BASE = {
    "transaction_id": "TXN-000001",
    "account_id": "ACC-0001",
    "transaction_ts": "2025-01-01T00:00:00",
    "transaction_type": "CREDIT",
    "amount": "10.00",
    "currency": "INR",
    "status": "PENDING",
    "source_updated_ts": "2025-01-01T10:00:00",
}


def _row(**kw):
    return dict(BASE, **kw)


def _read(spark, tmp_path, rows):
    p = tmp_path / f"{uuid.uuid4().hex}.csv"
    write_csv(rows, p)
    return read_raw_transactions(spark, p)


def _tuples(df):
    return sorted(tuple(r[c] for c in COLUMNS) for r in df.collect())


def test_newer_version_wins_and_older_is_superseded(spark, tmp_path):
    df = _read(spark, tmp_path, [
        _row(status="PENDING", source_updated_ts="2025-01-01T10:00:00"),
        _row(status="POSTED", source_updated_ts="2025-01-01T11:00:00"),
    ])
    latest, superseded = dedup_transactions(df)
    assert [r["status"] for r in latest.collect()] == ["POSTED"]
    assert [r["status"] for r in superseded.collect()] == ["PENDING"]


def test_chain_of_three_in_shuffled_order_keeps_the_newest(spark, tmp_path):
    df = _read(spark, tmp_path, [
        _row(status="PENDING", source_updated_ts="2025-01-01T11:00:00"),
        _row(status="FAILED", source_updated_ts="2025-01-01T12:00:00"),
        _row(status="POSTED", source_updated_ts="2025-01-01T10:00:00"),
    ])
    latest, superseded = dedup_transactions(df)
    assert [r["status"] for r in latest.collect()] == ["FAILED"]
    assert superseded.count() == 2


def test_identical_redelivery_is_one_transaction_not_a_conflict(spark, tmp_path):
    df = _read(spark, tmp_path, [_row(), _row()])
    latest, superseded = dedup_transactions(df)
    assert latest.count() == 1 and superseded.count() == 1
    assert count_tie_conflicts(df) == 0


@pytest.mark.parametrize("order", [("POSTED", "FAILED"), ("FAILED", "POSTED")])
def test_conflicting_tie_is_deterministic_regardless_of_file_order(spark, tmp_path, order):
    df = _read(spark, tmp_path, [_row(status=s) for s in order])
    latest, _ = dedup_transactions(df)
    # documented, arbitrary rule: the greatest status string wins ("POSTED" > "FAILED")
    assert [r["status"] for r in latest.collect()] == ["POSTED"]
    assert count_tie_conflicts(df) == 1


def test_conflict_on_an_older_timestamp_is_not_counted(spark, tmp_path):
    df = _read(spark, tmp_path, [
        _row(status="POSTED", source_updated_ts="2025-01-01T10:00:00"),
        _row(status="FAILED", source_updated_ts="2025-01-01T10:00:00"),
        _row(status="PENDING", source_updated_ts="2025-01-01T11:00:00"),
    ])
    assert count_tie_conflicts(df) == 0


def test_different_ids_are_independent(spark, tmp_path):
    df = _read(spark, tmp_path, [
        _row(transaction_id="TXN-000001"), _row(transaction_id="TXN-000002"),
    ])
    latest, superseded = dedup_transactions(df)
    assert latest.count() == 2 and superseded.count() == 0


def test_output_columns_and_types(spark, tmp_path):
    latest, superseded = dedup_transactions(_read(spark, tmp_path, [_row(), _row(status="POSTED")]))
    for out in (latest, superseded):
        assert out.columns == list(COLUMNS)
        assert all(dtype == "string" for _, dtype in out.dtypes)


def test_empty_input_gives_two_empty_outputs(spark, tmp_path):
    latest, superseded = dedup_transactions(_read(spark, tmp_path, []))
    assert latest.count() == 0 and superseded.count() == 0


def test_missing_column_raises(spark, tmp_path):
    df = _read(spark, tmp_path, [_row()]).drop("status")
    with pytest.raises(ValueError, match="missing"):
        dedup_transactions(df)


def _validated(spark, tmp_path, rows):
    valid, _ = validate_transactions(_read(spark, tmp_path, rows))
    return valid


def test_reconciliation_on_generated_seed_42(spark, tmp_path):
    valid = _validated(spark, tmp_path, generate_rows(GeneratorConfig(num_rows=1000, seed=42)))
    latest, superseded = dedup_transactions(valid)
    assert valid.count() == 950
    assert latest.count() == 870 and superseded.count() == 80
    assert sorted(_tuples(latest) + _tuples(superseded)) == sorted(_tuples(valid))
    latest_ids = [t[0] for t in _tuples(latest)]
    assert len(latest_ids) == len(set(latest_ids))
    assert sorted(_tuples(latest) + _tuples(superseded)) == _tuples(valid)
    ids = [r["transaction_id"] for r in latest.collect()]
    assert len(ids) == len(set(ids))


def test_latest_has_the_maximum_timestamp_per_id(spark, tmp_path):
    rows = generate_rows(GeneratorConfig(num_rows=1000, seed=42))
    valid = _validated(spark, tmp_path, rows)
    expected = {}
    for r in valid.collect():
        k = r["transaction_id"]
        expected[k] = max(expected.get(k, ""), r["source_updated_ts"])
    latest, _ = dedup_transactions(valid)
    got = {r["transaction_id"]: r["source_updated_ts"] for r in latest.collect()}
    assert got == expected


def test_generated_tie_conflicts_match_generator(spark, tmp_path):
    valid = _validated(spark, tmp_path, generate_rows(GeneratorConfig(num_rows=1000, seed=42)))
    assert count_tie_conflicts(valid) == 4  # 8 ties: 4 identical, 4 conflicting


def test_result_does_not_depend_on_input_order(spark, tmp_path):
    base_rows = generate_rows(GeneratorConfig(num_rows=1000, seed=42))
    results = []
    for seed in range(4):
        rows = list(base_rows)
        random.Random(seed).shuffle(rows)
        latest, superseded = dedup_transactions(_validated(spark, tmp_path, rows))
        results.append((_tuples(latest), _tuples(superseded)))
    assert all(r == results[0] for r in results)
