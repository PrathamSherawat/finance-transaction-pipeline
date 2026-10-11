"""End-to-end run: publish -> data-quality checks -> KPIs."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from finance_pipeline.config import load_settings
from finance_pipeline.errors import PipelineError, exit_code_for
from finance_pipeline.ingest import get_spark
from finance_pipeline.kpis import DEFAULT_SQL_DIR, compute_kpis, write_kpis
from finance_pipeline.logging_setup import setup_logging
from finance_pipeline.publish import run_pipeline
from finance_pipeline.quality import (
    assert_all_passed,
    format_report,
    run_quality_checks,
)


log = logging.getLogger(__name__)


def run_all(
    spark: SparkSession,
    input_csv,
    output_dir="data/output",
    quarantine_dir="data/quarantine",
    kpi_dir=None,
    sql_dir=DEFAULT_SQL_DIR,
) -> dict:
    """Publish, verify the published files, then write KPIs.

    The report is printed BEFORE any failure is raised, and KPI files are only
    written when every check passed.
    """
    kpi_dir = Path(output_dir) / "kpis" if kpi_dir is None else kpi_dir
    manifest = run_pipeline(spark, input_csv, output_dir, quarantine_dir)
    log.info("published: %s", manifest)
    checks = run_quality_checks(output_dir, quarantine_dir, sql_dir)
    print(format_report(checks))
    assert_all_passed(checks)
    kpis = compute_kpis(Path(output_dir) / "curated" / "transactions.parquet", sql_dir)
    write_kpis(kpis, kpi_dir)
    return {"manifest": manifest, "checks": checks, "kpis": kpis}


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m finance_pipeline.pipeline",
        description="Run ingestion, validation, dedup, publishing, quality checks and KPIs.",
    )
    s = load_settings()
    p.add_argument("--input", default=s.input_csv)
    p.add_argument("--output-dir", default=s.output_dir)
    p.add_argument("--quarantine-dir", default=s.quarantine_dir)
    p.add_argument("--kpi-dir", default=None, help="default: <output-dir>/kpis")
    p.add_argument("--sql-dir", default=s.sql_dir)
    return p


def main(argv: list[str] | None = None, spark: SparkSession | None = None) -> int:
    """Exit codes: 0 ok, 1 a data-quality check failed, 2 bad input."""
    args = _parser().parse_args(argv)
    setup_logging(load_settings().log_level)
    owns_spark = spark is None
    try:
        if owns_spark:
            spark = get_spark("finance-pipeline")
        assert spark is not None
        result = run_all(spark, args.input, args.output_dir, args.quarantine_dir,
                         args.kpi_dir, args.sql_dir)
    except (PipelineError, ValueError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exit_code_for(exc)
    finally:
        if owns_spark and spark is not None:
            spark.stop()
    print(json.dumps(result["manifest"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
