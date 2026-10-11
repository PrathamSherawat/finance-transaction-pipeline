"""Row-level validation: attach a list of failed-rule codes to every row."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from finance_pipeline.schema import TS_FORMAT, VALID_CURRENCIES, VALID_STATUSES, VALID_TYPES

# TS_FORMAT lives in schema.py. Parsing relies on spark.sql.legacy.timeParserPolicy=CORRECTED
# (set in get_spark), so bad strings give null instead of raising.
REASONS_COL = "quarantine_reasons"



def _is_missing(name: str) -> Column:
    """True when the column is null, empty or whitespace-only. Never null."""
    c = F.col(name)
    return F.coalesce(c.isNull() | (F.trim(c) == ""), F.lit(True))


def _not_in(name: str, allowed: tuple[str, ...]) -> Column:
    """True when the value is not exactly one of `allowed` (null counts as not in).

    NULL IN (...) is NULL, so coalesce it to False before negating; otherwise
    null rows would match neither this rule nor its opposite.
    """
    return ~F.coalesce(F.col(name).isin(*allowed), F.lit(False))


def _malformed_ts(name: str) -> Column:
    """True when the value is present but is not a real YYYY-MM-DDTHH:MM:SS timestamp."""
    parsed_is_null = F.coalesce(F.to_timestamp(F.col(name), TS_FORMAT).isNull(), F.lit(True))
    return ~_is_missing(name) & parsed_is_null


AMOUNT_PATTERN = r"^-?\d{1,16}\.\d{2}$"  # max 16 integer digits: fits decimal(18,2)
ZERO_PATTERN = r"^0+\.00$"


def _amount_is_wellformed(name: str = "amount") -> Column:
    """True when the value is exactly -?digits.dd. Never null."""
    return F.coalesce(F.col(name).rlike(AMOUNT_PATTERN), F.lit(False))


def _malformed_amount() -> Column:
    return ~_is_missing("amount") & ~_amount_is_wellformed()


def _non_positive_amount() -> Column:
    """Well-formed and zero or negative. String logic only, so huge values cannot overflow."""
    c = F.col("amount")
    zero_or_negative = F.coalesce(c.startswith("-") | c.rlike(ZERO_PATTERN), F.lit(False))
    return _amount_is_wellformed() & zero_or_negative


def _rules() -> list[tuple[str, Column]]:
    """(reason code, failure condition) in fixed order, so output arrays are deterministic."""
    return [
        ("MISSING_TRANSACTION_ID", _is_missing("transaction_id")),
        ("MISSING_ACCOUNT_ID", _is_missing("account_id")),
        ("MISSING_TRANSACTION_TS", _is_missing("transaction_ts")),
        ("MALFORMED_TRANSACTION_TS", _malformed_ts("transaction_ts")),
        ("INVALID_TRANSACTION_TYPE", _not_in("transaction_type", VALID_TYPES)),
        ("MISSING_AMOUNT", _is_missing("amount")),
        ("MALFORMED_AMOUNT", _malformed_amount()),
        ("NON_POSITIVE_AMOUNT", _non_positive_amount()),
        ("UNSUPPORTED_CURRENCY", _not_in("currency", VALID_CURRENCIES)),
        ("INVALID_STATUS", _not_in("status", VALID_STATUSES)),
        ("MISSING_SOURCE_UPDATED_TS", _is_missing("source_updated_ts")),
        ("MALFORMED_SOURCE_UPDATED_TS", _malformed_ts("source_updated_ts")),
    ]


def rule_failures(df: DataFrame) -> DataFrame:
    """Add `quarantine_reasons` (array<string>); an empty array means the row is valid."""
    codes = [F.when(cond, F.lit(code)) for code, cond in _rules()]
    return df.withColumn(REASONS_COL, F.array_compact(F.array(*codes)))


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a frame that already has REASONS_COL into (valid, quarantine).

    size() of a non-null array is never null, so every row lands in exactly one output.
    """
    no_failures = F.size(F.col(REASONS_COL)) == 0
    valid = df.filter(no_failures).drop(REASONS_COL)
    quarantine = df.filter(~no_failures)
    return valid, quarantine


def validate_transactions(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Raw all-string frame in; (valid, quarantine) out."""
    return split_valid_quarantine(rule_failures(df))
