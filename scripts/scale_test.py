"""One-off scale test: time each stage on N generated rows in a temp folder.

Run from the project root:  python scripts\scale_test.py --rows 200000
Never touches data/. Note the pipeline collects results into Python and writes them with DuckDB,
so this shows where that design starts to hurt.
"""
import argparse
import shutil
import tempfile
import time
from pathlib import Path

from finance_pipeline.generate_data import GeneratorConfig, generate_rows, write_csv
from finance_pipeline.ingest import get_spark
from finance_pipeline.kpis import compute_kpis
from finance_pipeline.publish import run_pipeline
from finance_pipeline.quality import run_quality_checks


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sql-dir", default="sql")
    args = ap.parse_args()

    root = Path(tempfile.mkdtemp(prefix="fin_scale_"))
    timings = []

    def timed(label, fn):
        start = time.perf_counter()
        value = fn()
        timings.append((label, time.perf_counter() - start))
        return value

    spark = get_spark("finance-pipeline-scale")
    try:
        spark.range(1).count()  # warm-up: JVM start is not counted below
        cfg = GeneratorConfig(num_rows=args.rows, seed=args.seed)
        rows = timed("generate rows (Python)", lambda: generate_rows(cfg))
        src = root / "in.csv"
        timed("write CSV", lambda: write_csv(rows, src))
        out, q = root / "output", root / "quarantine"
        manifest = timed("pipeline: read, validate, dedup, type, publish", lambda: run_pipeline(spark, src, out, q))
        checks = timed("13 quality checks", lambda: run_quality_checks(out, q, args.sql_dir))
        timed("KPI SQL", lambda: compute_kpis(out / "curated" / "transactions.parquet", args.sql_dir))
        size_mb = sum(f.stat().st_size for f in root.rglob("*") if f.is_file()) / 1e6
    finally:
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)

    print(f"\nrows={args.rows} seed={args.seed}\n")
    print("| stage | seconds |\n|---|---|")
    for label, secs in timings:
        print(f"| {label} | {secs:.1f} |")
    total = sum(s for _, s in timings)
    print(f"| total | {total:.1f} |")
    print(f"\npipeline throughput: {args.rows / dict(timings)['pipeline: read, validate, dedup, type, publish']:.0f} rows/s")
    print(f"files on disk (input + outputs): {size_mb:.1f} MB")
    print(f"manifest: {manifest}")
    print(f"quality checks passed: {sum(c.passed for c in checks)}/{len(checks)}")
    return 0 if all(c.passed for c in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
