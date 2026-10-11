"""Deduplication: keep the latest version of each transaction_id."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from finance_pipeline.schema import COLUMNS
from finance_pipeline.validate import TS_FORMAT

KEY = "transaction_id"
# Tie-break order when source_updated_ts is equal. The choice is arbitrary but
# deterministic: it only has to make the ordering total.
TIE_BREAK_COLUMNS = (
    "status", "amount", "account_id", "transaction_ts", "transaction_type", "currency",
)
_RANK = "_rank"
_TS = "_parsed_updated_ts"


def _require_columns(df: DataFrame) -> None:
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"input is missing columns: {missing}")


def dedup_transactions(valid_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (latest, superseded) from a validated all-string frame.

    latest has exactly one row per transaction_id. superseded holds every other
    row, so latest + superseded always equals the input.
    """
    _require_columns(valid_df)
    order = [F.to_timestamp(F.col("source_updated_ts"), TS_FORMAT).desc()]
    order += [F.col(c).desc() for c in TIE_BREAK_COLUMNS]
    ranked = valid_df.withColumn(_RANK, F.row_number().over(Window.partitionBy(KEY).orderBy(*order)))
    latest = ranked.filter(F.col(_RANK) == 1).drop(_RANK)
    superseded = ranked.filter(F.col(_RANK) > 1).drop(_RANK)
    return latest, superseded


def count_tie_conflicts(valid_df: DataFrame) -> int:
    """Count transaction_ids whose newest timestamp is shared by rows with different content."""
    _require_columns(valid_df)
    w = Window.partitionBy(KEY)
    with_ts = valid_df.withColumn(_TS, F.to_timestamp(F.col("source_updated_ts"), TS_FORMAT))
    newest = with_ts.withColumn("_max", F.max(_TS).over(w)).filter(F.col(_TS) == F.col("_max"))
    per_id = newest.groupBy(KEY).agg(F.countDistinct(*COLUMNS).alias("n_versions"))
    return per_id.filter(F.col("n_versions") > 1).count()
