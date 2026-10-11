"""Typing of validated, deduplicated rows. Runs only after validation and dedup."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from finance_pipeline.schema import COLUMNS
from finance_pipeline.validate import REASONS_COL, TS_FORMAT

AMOUNT_TYPE = "decimal(18,2)"
TS_COLUMNS = ("transaction_ts", "source_updated_ts")


def _require_columns(df: DataFrame, needed) -> None:
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"input is missing columns: {missing}")


def cast_to_curated(df: DataFrame) -> DataFrame:
    """Cast amount to decimal(18,2) and the two timestamps to timestamp.

    Safeguard: validation guarantees every cast succeeds. If any cast would give
    null anyway, raise instead of publishing silent nulls.
    """
    _require_columns(df, COLUMNS)
    uncastable = F.col("amount").cast(AMOUNT_TYPE).isNull()
    for c in TS_COLUMNS:
        uncastable = uncastable | F.to_timestamp(F.col(c), TS_FORMAT).isNull()
    bad = df.filter(uncastable).count()
    if bad:
        raise ValueError(f"{bad} row(s) cannot be cast to curated types; validation and casting disagree")

    exprs = []
    for c in COLUMNS:
        if c == "amount":
            exprs.append(F.col(c).cast(AMOUNT_TYPE).alias(c))
        elif c in TS_COLUMNS:
            exprs.append(F.to_timestamp(F.col(c), TS_FORMAT).alias(c))
        else:
            exprs.append(F.col(c))
    return df.select(*exprs)


def flatten_quarantine(df: DataFrame) -> DataFrame:
    """Turn the reasons array into a ';'-joined string so it fits in a CSV cell."""
    _require_columns(df, [REASONS_COL])
    return df.withColumn(REASONS_COL, F.concat_ws(";", F.col(REASONS_COL)))
