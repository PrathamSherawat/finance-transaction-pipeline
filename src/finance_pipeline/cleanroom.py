"""Clean-room validation: run the whole pipeline from nothing in a fresh folder, check known numbers."""
from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from finance_pipeline.ingest import get_spark
from finance_pipeline.kpis import DEFAULT_SQL_DIR
from finance_pipeline.orchestrate import main as orchestrate_main

# Seed 42, 1000 rows, 3 batches, default generator settings.
EXPECTED_SEED_42 = {
    "input_rows": 1000,
    "curated_rows": 870,
    "superseded_rows": 80,
    "quarantined_rows": 50,
    "reconciled": True,
    "batches_processed": 3,
}
BATCHES = 3


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def run_cleanroom(spark: SparkSession, workdir, expected: dict | None = None,
                  sql_dir=DEFAULT_SQL_DIR) -> dict:
    """Generate batches, run the pipeline, run it again, and check the results.

    Everything happens inside `workdir`, which must be empty or absent.
    """
    expected = EXPECTED_SEED_42 if expected is None else expected
    root = Path(workdir)
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"workdir {root} is not empty; refusing to use it")
    base = [
        "--incoming", str(root / "incoming"), "--state-dir", str(root / "state"),
        "--output-dir", str(root / "output"), "--quarantine-dir", str(root / "quarantine"),
        "--sql-dir", str(sql_dir), "--run-log", str(root / "run.json"), "--backoff", "0",
    ]
    checks: list[dict] = []

    def check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(ok), "detail": detail})

    manifest_path = root / "output" / "manifest.json"
    code1 = orchestrate_main(base + ["--generate-batches", str(BATCHES), "--rows", "1000", "--seed", "42"],
                             spark=spark)
    check("first run exits 0", code1 == 0, f"exit code {code1}")
    manifest = _read_json(manifest_path)
    check("manifest matches expected", manifest == expected, f"got {manifest}")

    code2 = orchestrate_main(base, spark=spark)  # nothing new: every batch is skipped
    after = _read_json(manifest_path)
    check("second run is a no-op", code2 == 0 and after == manifest, f"exit code {code2}, manifest unchanged: {after == manifest}")

    ledger = _read_json(root / "state" / "ledger.json")
    n = len(ledger.get("batches", []))
    check("ledger records every batch once", n == BATCHES, f"{n} batches in the ledger")

    totals = root / "output" / "kpis" / "totals.csv"
    currencies = [ln.split(",")[0] for ln in totals.read_text(encoding="utf-8").splitlines()[1:]] if totals.is_file() else []
    check("KPI totals cover INR and USD separately", currencies == ["INR", "USD"], f"currencies: {currencies}")

    return {"passed": all(c["passed"] for c in checks), "checks": checks, "workdir": str(root)}


def main(argv: list[str] | None = None) -> int:
    """Exit codes: 0 all checks passed, 1 a check failed, 2 bad arguments."""
    p = argparse.ArgumentParser(prog="python -m finance_pipeline.cleanroom",
                                description="Run the pipeline from scratch in an empty folder and verify it.")
    p.add_argument("--workdir", default=None, help="must be empty or absent; default: a fresh temp folder")
    p.add_argument("--keep", action="store_true", help="keep the temp folder afterwards")
    p.add_argument("--sql-dir", default=DEFAULT_SQL_DIR)
    args = p.parse_args(argv)
    auto = args.workdir is None
    workdir = Path(tempfile.mkdtemp(prefix="fin_cleanroom_")) if auto else Path(args.workdir)
    spark = get_spark("finance-pipeline-cleanroom")
    try:
        result = run_cleanroom(spark, workdir, sql_dir=args.sql_dir)
    except ValueError as exc:
        print(f"error: {exc}")
        return 2
    finally:
        spark.stop()
        if auto and not args.keep:
            shutil.rmtree(workdir, ignore_errors=True)
    for c in result["checks"]:
        print(f"[{'PASS' if c['passed'] else 'FAIL'}] {c['name']}: {c['detail']}")
    ok = sum(c["passed"] for c in result["checks"])
    print(f"{ok}/{len(result['checks'])} clean-room checks passed")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
