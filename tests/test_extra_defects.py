import csv
import random
from datetime import datetime
from pathlib import Path

import pytest

from finance_pipeline.generate_data import (
    ALL_DEFECTS,
    COLUMNS,
    DEFECTS,
    EXTRA_DEFECTS,
    GeneratorConfig,
    apply_defect,
    generate_rows,
    main,
    write_csv,
)
from finance_pipeline.publish import run_pipeline
from finance_pipeline.quality import run_quality_checks

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
BASE = {
    "transaction_id": "TXN-000001", "account_id": "ACC-0001",
    "transaction_ts": "2025-01-01T09:00:00", "transaction_type": "CREDIT",
    "amount": "10.00", "currency": "INR", "status": "POSTED",
    "source_updated_ts": "2025-01-01T10:00:00",
}
EXPECTED_CODES = {
    "MISSING_TRANSACTION_ID", "MISSING_ACCOUNT_ID", "MALFORMED_TRANSACTION_TS",
    "INVALID_TRANSACTION_TYPE", "NON_POSITIVE_AMOUNT", "UNSUPPORTED_CURRENCY", "INVALID_STATUS",
    "MISSING_AMOUNT", "MALFORMED_AMOUNT", "MISSING_SOURCE_UPDATED_TS", "MALFORMED_SOURCE_UPDATED_TS",
}


def _parses(value):
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _kind(r):
    """Independent classifier over all 11 defects; None means the row is valid."""
    if r["transaction_id"] == "":
        return "missing_transaction_id"
    if r["account_id"] == "":
        return "missing_account_id"
    if not _parses(r["transaction_ts"]):
        return "malformed_transaction_ts"
    if r["transaction_type"] not in {"CREDIT", "DEBIT"}:
        return "invalid_transaction_type"
    if r["amount"] == "":
        return "missing_amount"
    if not __import__("re").fullmatch(r"-?\d+\.\d{2}", r["amount"]):
        return "malformed_amount"
    if float(r["amount"]) <= 0:
        return "non_positive_amount"
    if r["currency"] not in {"INR", "USD"}:
        return "unsupported_currency"
    if r["status"] not in {"POSTED", "PENDING", "FAILED"}:
        return "invalid_status"
    if r["source_updated_ts"] == "":
        return "missing_source_updated_ts"
    if not _parses(r["source_updated_ts"]):
        return "malformed_source_updated_ts"
    return None


def _kinds(rows):
    return {_kind(r) for r in rows} - {None}


def test_defect_lists_are_consistent():
    assert len(DEFECTS) == 7 and len(EXTRA_DEFECTS) == 4
    assert ALL_DEFECTS == DEFECTS + EXTRA_DEFECTS


def test_default_config_never_emits_extra_defects():
    kinds = _kinds(generate_rows(GeneratorConfig()))
    assert kinds == set(DEFECTS) and kinds.isdisjoint(EXTRA_DEFECTS)


def test_extra_defects_on_emits_all_eleven_kinds():
    assert _kinds(generate_rows(GeneratorConfig(extra_defects=True))) == set(ALL_DEFECTS)


def test_extra_defects_keep_row_and_invalid_counts():
    for flag in (False, True):
        rows = generate_rows(GeneratorConfig(extra_defects=flag))
        assert len(rows) == 1000
        assert sum(_kind(r) is not None for r in rows) == 50


def test_extra_defects_output_is_reproducible():
    cfg = GeneratorConfig(extra_defects=True, seed=9)
    assert generate_rows(cfg) == generate_rows(cfg)


@pytest.mark.parametrize("defect", EXTRA_DEFECTS)
def test_extra_defect_changes_exactly_one_field_and_is_detected(defect):
    snapshot = dict(BASE)
    for seed in range(50):
        bad = apply_defect(random.Random(seed), BASE, defect)
        assert len([c for c in COLUMNS if bad[c] != BASE[c]]) == 1
        assert _kind(bad) == defect
    assert BASE == snapshot


@pytest.mark.parametrize("bad", ["yes", 1, None])
def test_extra_defects_must_be_a_bool(bad):
    with pytest.raises(ValueError, match="extra_defects"):
        GeneratorConfig(extra_defects=bad)


def test_cli_flag_writes_all_eleven_kinds(tmp_path):
    p = tmp_path / "t.csv"
    assert main(["--rows", "1000", "--extra-defects", "--output", str(p)]) == 0
    with p.open(newline="", encoding="utf-8") as f:
        assert _kinds(csv.DictReader(f)) == set(ALL_DEFECTS)


def test_pipeline_quarantines_every_extra_defect_with_its_own_code(spark, tmp_path):
    src = tmp_path / "in.csv"
    write_csv(generate_rows(GeneratorConfig(num_rows=1000, seed=42, extra_defects=True)), src)
    out, q = tmp_path / "out", tmp_path / "q"
    manifest = run_pipeline(spark, src, out, q)
    assert manifest == {"input_rows": 1000, "curated_rows": 870, "superseded_rows": 80,
                        "quarantined_rows": 50, "reconciled": True}
    with (q / "quarantine.csv").open(newline="", encoding="utf-8") as f:
        reasons = [r["quarantine_reasons"] for r in csv.DictReader(f)]
    assert all(";" not in x for x in reasons)  # exactly one code per bad row
    assert set(reasons) == EXPECTED_CODES
    results = run_quality_checks(out, q, SQL_DIR)
    assert all(r.passed for r in results)
