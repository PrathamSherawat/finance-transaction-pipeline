"""Plain-Python orchestrator: ordered tasks, retries with backoff, per-task status, run log."""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from pyspark.sql import SparkSession

from finance_pipeline.batches import main as batches_main
from finance_pipeline.config import load_settings
from finance_pipeline.errors import LockError, exit_code_for
from finance_pipeline.incremental import run_incremental
from finance_pipeline.ingest import get_spark
from finance_pipeline.logging_setup import setup_logging

log = logging.getLogger(__name__)

# Only failures that can clear on their own are retried.
RETRYABLE = (LockError, OSError)


@dataclass
class TaskResult:
    name: str
    status: str  # "ok" | "failed" | "skipped"
    attempts: int = 0
    seconds: float = 0.0
    error: str | None = None
    exit_code: int = 0


def run_task(name: str, fn: Callable[[], object], retries: int = 2, backoff: float = 2.0,
             retry_on: tuple = RETRYABLE, sleep: Callable[[float], None] = time.sleep) -> TaskResult:
    """Run fn; retry only `retry_on` errors, waiting backoff * 2**(attempt-1) seconds between tries."""
    if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
        raise ValueError(f"retries must be an integer >= 0, got {retries!r}")
    start = time.monotonic()
    attempt = 0
    while True:
        attempt += 1
        try:
            fn()
        except Exception as exc:
            if isinstance(exc, retry_on) and attempt <= retries:
                delay = backoff * 2 ** (attempt - 1)
                log.warning("task %s attempt %d failed (%s); retrying in %.1fs", name, attempt, exc, delay)
                sleep(delay)
                continue
            log.error("task %s failed after %d attempt(s): %s", name, attempt, exc)
            return TaskResult(name, "failed", attempt, time.monotonic() - start, str(exc), exit_code_for(exc))
        return TaskResult(name, "ok", attempt, time.monotonic() - start)


def run_tasks(tasks: list[tuple[str, Callable[[], object]]], retries: int = 2, backoff: float = 2.0,
              sleep: Callable[[float], None] = time.sleep) -> list[TaskResult]:
    """Run tasks in order. After the first failure the rest are skipped, not run."""
    results: list[TaskResult] = []
    failed = False
    for name, fn in tasks:
        if failed:
            results.append(TaskResult(name, "skipped"))
            continue
        result = run_task(name, fn, retries, backoff, sleep=sleep)
        results.append(result)
        failed = result.status == "failed"
    return results


def overall_exit_code(results: list[TaskResult]) -> int:
    return next((r.exit_code for r in results if r.status == "failed"), 0)


def write_run_log(path, results: list[TaskResult], started: str) -> dict:
    summary = {
        "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "exit_code": overall_exit_code(results),
        "tasks": [asdict(r) for r in results],
    }
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def _parser() -> argparse.ArgumentParser:
    s = load_settings()
    p = argparse.ArgumentParser(
        prog="python -m finance_pipeline.orchestrate",
        description="Run the pipeline tasks in order with retries and a run log.",
    )
    p.add_argument("--incoming", default=s.incoming_dir)
    p.add_argument("--state-dir", default=s.state_dir)
    p.add_argument("--output-dir", default=s.output_dir)
    p.add_argument("--quarantine-dir", default=s.quarantine_dir)
    p.add_argument("--sql-dir", default=s.sql_dir)
    p.add_argument("--run-log", default=None, help="default: <state-dir>/last_run.json")
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--backoff", type=float, default=2.0, help="seconds; doubles after each retry")
    p.add_argument("--generate-batches", type=int, default=0,
                   help="demo only: first write N synthetic batch files into --incoming")
    p.add_argument("--rows", type=int, default=1000)
    p.add_argument("--seed", type=int, default=42)
    return p


def main(argv: list[str] | None = None, spark: SparkSession | None = None,
         sleep: Callable[[float], None] = time.sleep) -> int:
    """Exit code is the first failed task's code: 0 ok, 1 quality, 2 bad input, 3 conflict, 4 locked."""
    args = _parser().parse_args(argv)
    setup_logging(load_settings().log_level)
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    owns_spark = spark is None
    try:
        if owns_spark:
            spark = get_spark("finance-pipeline-orchestrator")

        def generate() -> None:
            code = batches_main(["--rows", str(args.rows), "--seed", str(args.seed),
                                 "--batches", str(args.generate_batches), "--output-dir", args.incoming])
            if code != 0:
                raise ValueError("batch generation failed")

        def incremental() -> None:
            assert spark is not None
            run_incremental(spark, args.incoming, args.state_dir, args.output_dir,
                            args.quarantine_dir, None, args.sql_dir)

        tasks: list[tuple[str, Callable[[], object]]] = []
        if args.generate_batches:
            tasks.append(("generate_batches", generate))
        tasks.append(("incremental", incremental))
        results = run_tasks(tasks, args.retries, args.backoff, sleep)
    finally:
        if owns_spark and spark is not None:
            spark.stop()
    log_path = args.run_log or str(Path(args.state_dir) / "last_run.json")
    summary = write_run_log(log_path, results, started)
    print(json.dumps({"exit_code": summary["exit_code"], "tasks": summary["tasks"]}))
    return int(summary["exit_code"])


if __name__ == "__main__":
    sys.exit(main())
