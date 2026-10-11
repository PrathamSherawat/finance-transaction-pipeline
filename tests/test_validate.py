import uuid

import pytest

from finance_pipeline.generate_data import COLUMNS, GeneratorConfig, generate_rows, write_csv
from finance_pipeline.ingest import read_raw_transactions
from finance_pipeline.validate import REASONS_COL, rule_failures, validate_transactions

GOOD = {
    "transaction_id": "TXN-000001",
    "account_id": "ACC-0001",
    "transaction_ts": "2025-01-01T00:00:00",
    "transaction_type": "CREDIT",
    "amount": "10.00",
    "currency": "INR",
    "status": "POSTED",
    "source_updated_ts": "2025-01-01T00:00:01",
}


def _reasons(spark, tmp_path, **overrides):
    """Write one row (GOOD + overrides) as CSV, read it via Spark, return its reasons."""
    p = tmp_path / f"{uuid.uuid4().hex}.csv"
    write_csv([dict(GOOD, **overrides)], p)
    df = rule_failures(read_raw_transactions(spark, p))
    return df.collect()[0][REASONS_COL]


def _ts_cases(field, suffix):
    bad = ["not-a-timestamp", "2025-13-45T99:99:99", "2025-02-30T00:00:00", "2025-01-01T00:00:00Z"]
    return [(field, "", f"MISSING_{suffix}")] + [(field, v, f"MALFORMED_{suffix}") for v in bad]


CASES = (
    [
        ("transaction_id", "", "MISSING_TRANSACTION_ID"),
        ("transaction_id", "   ", "MISSING_TRANSACTION_ID"),
        ("account_id", "", "MISSING_ACCOUNT_ID"),
        ("account_id", "   ", "MISSING_ACCOUNT_ID"),
        ("transaction_type", "TRANSFER", "INVALID_TRANSACTION_TYPE"),
        ("transaction_type", "credit", "INVALID_TRANSACTION_TYPE"),
        ("transaction_type", " CREDIT", "INVALID_TRANSACTION_TYPE"),
        ("transaction_type", "", "INVALID_TRANSACTION_TYPE"),
        ("currency", "EUR", "UNSUPPORTED_CURRENCY"),
        ("currency", "", "UNSUPPORTED_CURRENCY"),
        ("currency", "inr", "UNSUPPORTED_CURRENCY"),
        ("currency", " INR", "UNSUPPORTED_CURRENCY"),
        ("status", "REVERSED", "INVALID_STATUS"),
        ("status", "posted", "INVALID_STATUS"),
        ("status", "", "INVALID_STATUS"),
    ]
    + _ts_cases("transaction_ts", "TRANSACTION_TS")
    + _ts_cases("source_updated_ts", "SOURCE_UPDATED_TS")
)


def test_valid_row_has_empty_reasons(spark, tmp_path):
    assert _reasons(spark, tmp_path) == []


@pytest.mark.parametrize("field, value, code", CASES)
def test_single_defect_gives_exactly_one_code(spark, tmp_path, field, value, code):
    assert _reasons(spark, tmp_path, **{field: value}) == [code]


def test_multiple_defects_list_all_codes_in_rule_order(spark, tmp_path):
    got = _reasons(spark, tmp_path, currency="EUR", transaction_id="", status="bogus")
    assert got == ["MISSING_TRANSACTION_ID", "UNSUPPORTED_CURRENCY", "INVALID_STATUS"]


def test_rule_failures_keeps_every_row_and_adds_one_column(spark, tmp_path):
    p = tmp_path / "t.csv"
    write_csv(generate_rows(GeneratorConfig(num_rows=300, seed=2)), p)
    raw = read_raw_transactions(spark, p)
    out = rule_failures(raw)
    assert out.columns == list(COLUMNS) + [REASONS_COL]
    assert out.count() == raw.count() == 300


AMOUNT_CASES = (
    [("amount", v, "MISSING_AMOUNT") for v in ["", "   "]]
    + [("amount", v, "MALFORMED_AMOUNT") for v in
       ["abc", "10", "10.5", "10.000", "1,000.00", "$10.00", "1e3", " 10.00", "+10.00",
        "99999999999999999999.99", "-99999999999999999999.00", "12345678901234567.00"]]
    + [("amount", v, "NON_POSITIVE_AMOUNT") for v in
       ["0.00", "-0.00", "-5.00", "-123.45", "00.00", "-9999999999999999.00"]]
)


@pytest.mark.parametrize("field, value, code", AMOUNT_CASES)
def test_amount_defect_gives_exactly_one_code(spark, tmp_path, field, value, code):
    assert _reasons(spark, tmp_path, **{field: value}) == [code]


@pytest.mark.parametrize("value", ["0.01", "5000.00", "9999999999999999.99", "007.50"])
def test_valid_amounts_pass(spark, tmp_path, value):
    assert _reasons(spark, tmp_path, amount=value) == []


def test_amount_defect_combines_with_others_in_rule_order(spark, tmp_path):
    got = _reasons(spark, tmp_path, amount="-1.00", currency="EUR", transaction_type="X")
    assert got == ["INVALID_TRANSACTION_TYPE", "NON_POSITIVE_AMOUNT", "UNSUPPORTED_CURRENCY"]


def test_generated_sample_quarantines_exactly_the_invalid_rows(spark, tmp_path):
    cfg = GeneratorConfig(num_rows=1000, seed=42)
    p = tmp_path / "t.csv"
    write_csv(generate_rows(cfg), p)
    rows = rule_failures(read_raw_transactions(spark, p)).collect()
    failing = [r[REASONS_COL] for r in rows if r[REASONS_COL]]
    assert len(rows) == 1000
    assert len(failing) == 50                      # generator's invalid count
    assert all(len(reasons) == 1 for reasons in failing)  # one defect per bad row


def _tuple(row, cols):
    return tuple("" if row[c] is None else row[c] for c in cols)


def _generate_and_validate(spark, tmp_path, n, seed):
    rows = generate_rows(GeneratorConfig(num_rows=n, seed=seed))
    p = tmp_path / f"{uuid.uuid4().hex}.csv"
    write_csv(rows, p)
    valid, quarantine = validate_transactions(read_raw_transactions(spark, p))
    return rows, valid, quarantine


@pytest.mark.parametrize("n, seed", [(1000, 42), (1000, 7), (300, 1)])
def test_valid_plus_quarantine_equals_input_with_no_overlap(spark, tmp_path, n, seed):
    rows, valid, quarantine = _generate_and_validate(spark, tmp_path, n, seed)
    got = sorted(
        [_tuple(r, COLUMNS) for r in valid.collect()]
        + [_tuple(r, COLUMNS) for r in quarantine.collect()]
    )
    expected = sorted(tuple(r[c] for c in COLUMNS) for r in rows)
    assert got == expected


def test_seed_42_split_is_950_valid_50_quarantined(spark, tmp_path):
    _, valid, quarantine = _generate_and_validate(spark, tmp_path, 1000, 42)
    assert valid.count() == 950
    assert quarantine.count() == 50


def test_quarantine_reasons_cover_every_generated_defect_type(spark, tmp_path):
    _, _, quarantine = _generate_and_validate(spark, tmp_path, 1000, 42)
    reasons = [r[REASONS_COL] for r in quarantine.collect()]
    assert all(len(x) == 1 for x in reasons)
    assert {x[0] for x in reasons} == {
        "MISSING_TRANSACTION_ID",
        "MISSING_ACCOUNT_ID",
        "MALFORMED_TRANSACTION_TS",
        "INVALID_TRANSACTION_TYPE",
        "NON_POSITIVE_AMOUNT",
        "UNSUPPORTED_CURRENCY",
        "INVALID_STATUS",
    }


def test_output_columns(spark, tmp_path):
    _, valid, quarantine = _generate_and_validate(spark, tmp_path, 200, 3)
    assert valid.columns == list(COLUMNS)
    assert quarantine.columns == list(COLUMNS) + [REASONS_COL]
    assert all(dtype == "string" for _, dtype in valid.dtypes)


def test_header_only_file_gives_two_empty_outputs(spark, tmp_path):
    p = tmp_path / "empty.csv"
    write_csv([], p)
    valid, quarantine = validate_transactions(read_raw_transactions(spark, p))
    assert valid.count() == 0 and quarantine.count() == 0
