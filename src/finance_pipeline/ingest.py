"""Spark ingestion of raw transaction CSVs."""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from finance_pipeline.schema import COLUMNS

# Every column is read as a string so malformed values reach validation intact.
RAW_SCHEMA = StructType(
    [StructField(name, StringType(), nullable=True) for name in COLUMNS]
)


def get_spark(app_name: str = "finance-pipeline") -> SparkSession:
    """Return a small local SparkSession tuned for tests and sample-sized data."""
    # Make Spark's Python workers use this interpreter (the active venv).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # EXCEPTION (the default) raises on strings like "2025-1-1T00:00:00"
        # instead of returning null; the validator needs plain nulls.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def check_header(path: str | Path) -> None:
    """Raise if the CSV header is not exactly COLUMNS, in order.

    Spark assigns columns by position when given an explicit schema, so a
    wrong header would silently mislabel data.
    """
    p = Path(path)
    with p.open(newline="", encoding="utf-8-sig") as f:
        header = next(csv.reader(f), None)
    expected = list(COLUMNS)
    if header is None:
        raise ValueError(f"{p} is empty; expected header {expected}")
    missing = [c for c in expected if c not in header]
    unexpected = [c for c in header if c not in expected]
    if missing:
        raise ValueError(f"header is missing columns: {missing}")
    if unexpected:
        raise ValueError(f"header has unexpected columns: {unexpected}")
    if header != expected:
        raise ValueError(
            f"header columns are in the wrong order: expected {expected}, got {header}"
        )


def read_raw_transactions(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read the CSV with the explicit all-string schema, failing fast on bad structure."""
    check_header(path)
    return spark.read.csv(
        str(path), header=True, schema=RAW_SCHEMA, mode="FAILFAST"
    )
