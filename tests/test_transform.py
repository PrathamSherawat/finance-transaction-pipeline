import uuid
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from finance_pipeline.generate_data import COLUMNS, write_csv
from finance_pipeline.ingest import read_raw_transactions
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


def _row(**kw):
    return dict(BASE, **kw)


def _read(spark, tmp_path, rows):
    p = tmp_path / f"{uuid.uuid4().hex}.csv"
    write_csv(rows, p)
    return read_raw_transactions(spark, p)


def test_session_timezone_is_utc(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_cast_gives_expected_types(spark, tmp_path):
    out = cast_to_curated(_read(spark, tmp_path, [_row()]))
    types = dict(out.dtypes)
    assert out.columns == list(COLUMNS)
    assert types["amount"] == "decimal(18,2)"
    assert types["transaction_ts"] == "timestamp"
    assert types["source_updated_ts"] == "timestamp"
    assert all(types[c] == "string" for c in
               ("transaction_id", "account_id", "transaction_type", "currency", "status"))


def test_decimal_sum_is_exact(spark, tmp_path):
    rows = [_row(transaction_id="TXN-000001", amount="0.10"),
            _row(transaction_id="TXN-000002", amount="0.20")]
    out = cast_to_curated(_read(spark, tmp_path, rows))
    assert sum(r["amount"] for r in out.collect()) == Decimal("0.30")


def test_timestamp_round_trips_to_the_same_string(spark, tmp_path):
    out = cast_to_curated(_read(spark, tmp_path, [_row()]))
    got = out.select(F.date_format("transaction_ts", "yyyy-MM-dd'T'HH:mm:ss").alias("t")).collect()[0]["t"]
    assert got == "2025-01-01T00:00:00"


def test_cast_preserves_row_count(spark, tmp_path):
    rows = [_row(transaction_id=f"TXN-{i:06d}") for i in range(1, 4)]
    assert cast_to_curated(_read(spark, tmp_path, rows)).count() == 3


@pytest.mark.parametrize("field, value", [
    ("amount", "abc"),
    ("amount", "99999999999999999999.99"),   # overflows decimal(18,2)
    ("transaction_ts", "not-a-timestamp"),
])
def test_safeguard_raises_instead_of_publishing_nulls(spark, tmp_path, field, value):
    df = _read(spark, tmp_path, [_row(**{field: value})])
    with pytest.raises(ValueError, match="cast"):
        cast_to_curated(df)


def test_cast_missing_column_raises(spark, tmp_path):
    with pytest.raises(ValueError, match="missing"):
        cast_to_curated(_read(spark, tmp_path, [_row()]).drop("status"))


def test_flatten_quarantine_joins_reasons(spark, tmp_path):
    df = _read(spark, tmp_path, [_row(transaction_id="", currency="EUR")])
    _, quarantine = validate_transactions(df)
    out = flatten_quarantine(quarantine)
    assert dict(out.dtypes)[REASONS_COL] == "string"
    assert out.collect()[0][REASONS_COL] == "MISSING_TRANSACTION_ID;UNSUPPORTED_CURRENCY"


def test_flatten_quarantine_requires_reasons_column(spark, tmp_path):
    with pytest.raises(ValueError, match="missing"):
        flatten_quarantine(_read(spark, tmp_path, [_row()]))
