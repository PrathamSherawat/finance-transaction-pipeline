"""Incremental processing: merge new batch files into the published state."""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
from pathlib import Path

import duckdb
from pyspark.sql import SparkSession

from finance_pipeline.dedup import TIE_BREAK_COLUMNS, dedup_transactions
from finance_pipeline.config import load_settings
from finance_pipeline.errors import PipelineError, exit_code_for
from finance_pipeline.lock import RunLock
from finance_pipeline.logging_setup import setup_logging
from finance_pipeline.schema import COLUMNS
from finance_pipeline.ingest import get_spark, read_raw_transactions
from finance_pipeline.kpis import DEFAULT_SQL_DIR, compute_kpis, write_kpis
from finance_pipeline.ledger import (
    BatchConflictError,
    classify_batch,
    file_sha256,
    load_ledger,
    record_batch,
    save_ledger,
)
from finance_pipeline.publish import QUARANTINE_COLUMNS, TYPED_SELECT, _string_rows, _typed_rows
from finance_pipeline.quality import (
    assert_all_passed,
    format_report,
    run_quality_checks,
)
from finance_pipeline.transform import cast_to_curated, flatten_quarantine
from finance_pipeline.validate import validate_transactions

log = logging.getLogger(__name__)

QUARANTINE_WITH_BATCH = QUARANTINE_COLUMNS + ["batch_id"]
TYPED_DDL = (
    "transaction_id VARCHAR, account_id VARCHAR, transaction_ts TIMESTAMP, "
    "transaction_type VARCHAR, amount DECIMAL(18,2), currency VARCHAR, status VARCHAR, "
    "source_updated_ts TIMESTAMP"
)
# Same total order as dedup.py. amount is compared as text, like the Spark version.
_ORDER = ["source_updated_ts DESC"] + [
    ("CAST(amount AS VARCHAR)" if c == "amount" else c) + " DESC" for c in TIE_BREAK_COLUMNS
]
RANK_SQL = (
    "CREATE TABLE ranked AS SELECT *, row_number() OVER ("
    f"PARTITION BY transaction_id ORDER BY {', '.join(_ORDER)}) AS rn FROM cand"
)


def _q(path) -> str:
    return Path(path).as_posix().replace("'", "''")


def _load_stage(con, rows, columns) -> None:
    con.execute("CREATE OR REPLACE TABLE stage (" + ", ".join(f'"{c}" VARCHAR' for c in columns) + ")")
    if rows:
        marks = ", ".join("?" for _ in columns)
        con.executemany(f"INSERT INTO stage VALUES ({marks})", rows)


def _count(con, table: str) -> int:
    row = con.execute(f"SELECT count(*) FROM {table}").fetchone()
    assert row is not None
    return int(row[0])


def _process_with_spark(spark: SparkSession, csv_path: Path) -> dict:
    """Spark does what it did before: read, validate, dedup inside the batch, type."""
    raw = read_raw_transactions(spark, csv_path)
    input_rows = raw.count()
    valid, quarantine = validate_transactions(raw)
    latest, superseded = dedup_transactions(valid)
    return {
        "input_rows": input_rows,
        "curated": _typed_rows(cast_to_curated(latest)),
        "superseded": _typed_rows(cast_to_curated(superseded)),
        "quarantine": _string_rows(flatten_quarantine(quarantine), QUARANTINE_COLUMNS),
    }


def merge_into_snapshot(prev_dir, new_dir: Path, batch_id: str, batch: dict, prev_manifest: dict) -> dict:
    """Merge one processed batch into the previous snapshot; write the result to new_dir.

    Winner per transaction_id = best of (old curated + batch winners). Every loser
    goes to superseded, so the multiset of rows equals a one-shot full load.
    """
    new_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"CREATE TABLE cand ({TYPED_DDL})")
        con.execute(f"CREATE TABLE sup ({TYPED_DDL})")
        con.execute("CREATE TABLE quar (" + ", ".join(f'"{c}" VARCHAR' for c in QUARANTINE_WITH_BATCH) + ")")
        if prev_dir is not None:
            prev = Path(prev_dir)
            con.execute(f"INSERT INTO cand SELECT * FROM read_parquet('{_q(prev / 'curated.parquet')}')")
            con.execute(f"INSERT INTO sup SELECT * FROM read_parquet('{_q(prev / 'superseded.parquet')}')")
            cols = ", ".join(f'"{c}"' for c in QUARANTINE_WITH_BATCH)
            con.execute(
                f"INSERT INTO quar SELECT {cols} FROM read_csv("
                f"'{_q(prev / 'quarantine.csv')}', header = true, all_varchar = true)"
            )
        _load_stage(con, batch["curated"], list(COLUMNS))
        con.execute(f"INSERT INTO cand {TYPED_SELECT}")
        _load_stage(con, batch["superseded"], list(COLUMNS))
        con.execute(f"INSERT INTO sup {TYPED_SELECT}")
        _load_stage(con, [tuple(r) + (batch_id,) for r in batch["quarantine"]], QUARANTINE_WITH_BATCH)
        con.execute("INSERT INTO quar SELECT * FROM stage")

        con.execute(RANK_SQL)
        con.execute("INSERT INTO sup SELECT * EXCLUDE (rn) FROM ranked WHERE rn > 1")
        con.execute("CREATE TABLE cur_final AS SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1")

        n_cur, n_sup, n_q = _count(con, "cur_final"), _count(con, "sup"), _count(con, "quar")
        input_rows = prev_manifest.get("input_rows", 0) + batch["input_rows"]
        if input_rows != n_cur + n_sup + n_q:
            raise ValueError(
                f"counts do not reconcile: input_rows={input_rows} but curated + "
                f"superseded + quarantined = {n_cur + n_sup + n_q}"
            )
        manifest = {
            "input_rows": input_rows,
            "curated_rows": n_cur,
            "superseded_rows": n_sup,
            "quarantined_rows": n_q,
            "reconciled": True,
            "batches_processed": prev_manifest.get("batches_processed", 0) + 1,
        }
        con.execute(f"COPY (SELECT * FROM cur_final ORDER BY ALL) TO '{_q(new_dir / 'curated.parquet')}' (FORMAT PARQUET)")
        con.execute(f"COPY (SELECT * FROM sup ORDER BY ALL) TO '{_q(new_dir / 'superseded.parquet')}' (FORMAT PARQUET)")
        con.execute(f"COPY (SELECT * FROM quar ORDER BY ALL) TO '{_q(new_dir / 'quarantine.csv')}' (FORMAT CSV, HEADER)")
        (new_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        return manifest
    finally:
        con.close()


def export_snapshot(snapshot_dir: Path, output_dir, quarantine_dir) -> None:
    """Copy the current snapshot to the published layout. Idempotent, so a crash here
    is repaired by the next run."""
    out, qdir = Path(output_dir), Path(quarantine_dir)
    mapping = [
        (snapshot_dir / "curated.parquet", out / "curated" / "transactions.parquet"),
        (snapshot_dir / "superseded.parquet", out / "superseded" / "superseded.parquet"),
        (snapshot_dir / "manifest.json", out / "manifest.json"),
        (snapshot_dir / "quarantine.csv", qdir / "quarantine.csv"),
    ]
    for src, dst in mapping:
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_name(dst.name + ".tmp")
        shutil.copyfile(src, tmp)
        os.replace(tmp, dst)


def _run_incremental_locked(
    spark: SparkSession,
    incoming_dir,
    state_dir="data/state",
    output_dir="data/output",
    quarantine_dir="data/quarantine",
    kpi_dir=None,
    sql_dir=DEFAULT_SQL_DIR,
) -> dict:
    """Process every NEW batch file in name order, export, verify, write KPIs."""
    incoming, state = Path(incoming_dir), Path(state_dir)
    if not incoming.is_dir():
        raise FileNotFoundError(f"incoming directory not found: {incoming}")
    files = sorted(incoming.glob("*.csv"))
    ledger = load_ledger(state)
    if not files and ledger["current_snapshot"] is None:
        raise ValueError(f"no batch files found in {incoming}")

    hashes = {f: file_sha256(f) for f in files}
    conflicts = []
    for f in files:  # reject changed files BEFORE processing anything
        try:
            classify_batch(ledger, f.name, hashes[f])
        except BatchConflictError as exc:
            conflicts.append(str(exc))
    if conflicts:
        raise BatchConflictError("; ".join(conflicts))

    processed, skipped = [], []
    for f in files:
        if classify_batch(ledger, f.name, hashes[f]) == "skip":
            skipped.append(f.name)
            log.info("batch %s already processed, skipping", f.name)
            continue
        prev = state / "snapshots" / ledger["current_snapshot"] if ledger["current_snapshot"] else None
        prev_manifest = json.loads((prev / "manifest.json").read_text(encoding="utf-8")) if prev else {}
        snap_id = f"snap_{len(ledger['batches']) + 1:04d}"
        new_dir = state / "snapshots" / snap_id
        shutil.rmtree(new_dir, ignore_errors=True)  # orphan from a crashed run
        try:
            batch = _process_with_spark(spark, f)
            merged = merge_into_snapshot(prev, new_dir, f.name, batch, prev_manifest)
            new_ledger = record_batch(ledger, file=f.name, sha256=hashes[f],
                                      input_rows=batch["input_rows"], snapshot=snap_id)
            save_ledger(state, new_ledger)  # LAST: the commit point
            ledger = new_ledger
        except BaseException:
            shutil.rmtree(new_dir, ignore_errors=True)
            raise
        processed.append(f.name)
        log.info("batch %s merged: input=%d curated=%d superseded=%d quarantined=%d", f.name,
                 merged["input_rows"], merged["curated_rows"], merged["superseded_rows"],
                 merged["quarantined_rows"])

    current = state / "snapshots" / ledger["current_snapshot"]
    export_snapshot(current, output_dir, quarantine_dir)
    checks = run_quality_checks(output_dir, quarantine_dir, sql_dir)
    print(format_report(checks))
    assert_all_passed(checks)
    kpis = compute_kpis(Path(output_dir) / "curated" / "transactions.parquet", sql_dir)
    write_kpis(kpis, Path(output_dir) / "kpis" if kpi_dir is None else kpi_dir)
    manifest = json.loads((current / "manifest.json").read_text(encoding="utf-8"))
    return {"processed": processed, "skipped": skipped, "manifest": manifest,
            "checks": checks, "kpis": kpis}


def run_incremental(
    spark: SparkSession,
    incoming_dir,
    state_dir="data/state",
    output_dir="data/output",
    quarantine_dir="data/quarantine",
    kpi_dir=None,
    sql_dir=DEFAULT_SQL_DIR,
) -> dict:
    """Same as before, but only one run at a time may use a given state directory."""
    with RunLock(Path(state_dir) / "run.lock"):
        return _run_incremental_locked(
            spark, incoming_dir, state_dir, output_dir, quarantine_dir, kpi_dir, sql_dir
        )


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m finance_pipeline.incremental",
        description="Merge new batch files from the incoming folder into the published state.",
    )
    s = load_settings()
    p.add_argument("--incoming", default=s.incoming_dir)
    p.add_argument("--state-dir", default=s.state_dir)
    p.add_argument("--output-dir", default=s.output_dir)
    p.add_argument("--quarantine-dir", default=s.quarantine_dir)
    p.add_argument("--kpi-dir", default=None, help="default: <output-dir>/kpis")
    p.add_argument("--sql-dir", default=s.sql_dir)
    return p


def main(argv: list[str] | None = None, spark: SparkSession | None = None) -> int:
    """Exit codes: 0 ok, 1 quality check failed, 2 bad input, 3 batch file changed."""
    args = _parser().parse_args(argv)
    setup_logging(load_settings().log_level)
    owns_spark = spark is None
    try:
        if owns_spark:
            spark = get_spark("finance-pipeline-incremental")
        assert spark is not None
        result = run_incremental(spark, args.incoming, args.state_dir, args.output_dir,
                                 args.quarantine_dir, args.kpi_dir, args.sql_dir)
    except (PipelineError, ValueError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exit_code_for(exc)
    finally:
        if owns_spark and spark is not None:
            spark.stop()
    print(json.dumps({"processed": result["processed"], "skipped": result["skipped"],
                      "manifest": result["manifest"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
