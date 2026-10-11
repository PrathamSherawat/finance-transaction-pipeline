"""Publish curated, superseded and quarantined data (DuckDB writers) plus a manifest."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from finance_pipeline.dedup import dedup_transactions
from finance_pipeline.schema import COLUMNS
from finance_pipeline.ingest import get_spark, read_raw_transactions
from finance_pipeline.transform import TS_COLUMNS, cast_to_curated, flatten_quarantine
from finance_pipeline.validate import REASONS_COL, TS_FORMAT, validate_transactions

QUARANTINE_COLUMNS = list(COLUMNS) + [REASONS_COL]

# Rows arrive in DuckDB as strings and are re-typed here, so no Python-side
# decimal or timezone conversion can alter a value.
TYPED_SELECT = (
    "SELECT transaction_id, account_id, "
    "CAST(transaction_ts AS TIMESTAMP) AS transaction_ts, transaction_type, "
    "CAST(amount AS DECIMAL(18,2)) AS amount, currency, status, "
    "CAST(source_updated_ts AS TIMESTAMP) AS source_updated_ts FROM stage"
)
PLAIN_SELECT = "SELECT * FROM stage"


def _typed_rows(df: DataFrame) -> list[tuple]:
    exprs = []
    for c in COLUMNS:
        if c in TS_COLUMNS:
            exprs.append(F.date_format(F.col(c), TS_FORMAT).alias(c))
        elif c == "amount":
            exprs.append(F.col(c).cast("string").alias(c))
        else:
            exprs.append(F.col(c))
    return [tuple(r) for r in df.select(*exprs).collect()]


def _string_rows(df: DataFrame, columns: list[str]) -> list[tuple]:
    return [tuple(r[c] for c in columns) for r in df.collect()]


def _write(rows: list[tuple], columns: list[str], select_sql: str, path: Path, fmt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    target = path.as_posix().replace("'", "''")
    options = "FORMAT PARQUET" if fmt == "parquet" else "FORMAT CSV, HEADER"
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE stage (" + ", ".join(f'"{c}" VARCHAR' for c in columns) + ")")
        if rows:
            marks = ", ".join("?" for _ in columns)
            con.executemany(f"INSERT INTO stage VALUES ({marks})", rows)
        # ORDER BY ALL makes the file contents deterministic
        con.execute(f"COPY ({select_sql} ORDER BY ALL) TO '{target}' ({options})")
    finally:
        con.close()


def publish(
    curated: DataFrame,
    quarantine: DataFrame,
    superseded: DataFrame,
    input_rows: int,
    output_dir: str | Path,
    quarantine_dir: str | Path,
) -> dict:
    """Write all three outputs and a manifest; overwrite on every run.

    Reconciliation is checked BEFORE anything is written, so a mismatch
    leaves no half-published output.
    """
    curated_rows = _typed_rows(curated)
    superseded_rows = _typed_rows(superseded)
    quarantine_rows = _string_rows(quarantine, QUARANTINE_COLUMNS)

    total = len(curated_rows) + len(superseded_rows) + len(quarantine_rows)
    if input_rows != total:
        raise ValueError(
            f"counts do not reconcile: input_rows={input_rows} but curated + "
            f"superseded + quarantined = {total}"
        )
    manifest = {
        "input_rows": input_rows,
        "curated_rows": len(curated_rows),
        "superseded_rows": len(superseded_rows),
        "quarantined_rows": len(quarantine_rows),
        "reconciled": True,
    }

    out, qdir = Path(output_dir), Path(quarantine_dir)
    cols = list(COLUMNS)
    _write(curated_rows, cols, TYPED_SELECT, out / "curated" / "transactions.parquet", "parquet")
    _write(superseded_rows, cols, TYPED_SELECT, out / "superseded" / "superseded.parquet", "parquet")
    _write(quarantine_rows, QUARANTINE_COLUMNS, PLAIN_SELECT, qdir / "quarantine.csv", "csv")
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def run_pipeline(
    spark: SparkSession,
    input_csv: str | Path,
    output_dir: str | Path = "data/output",
    quarantine_dir: str | Path = "data/quarantine",
) -> dict:
    """raw CSV -> validate -> dedup -> type -> publish. Returns the manifest."""
    raw = read_raw_transactions(spark, input_csv)
    input_rows = raw.count()
    valid, quarantine = validate_transactions(raw)
    latest, superseded = dedup_transactions(valid)
    return publish(
        cast_to_curated(latest),
        flatten_quarantine(quarantine),
        cast_to_curated(superseded),
        input_rows,
        output_dir,
        quarantine_dir,
    )


def main() -> int:
    spark = get_spark("finance-pipeline")
    try:
        manifest = run_pipeline(spark, "data/sample/transactions.csv")
    finally:
        spark.stop()
    print(json.dumps(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
