import ast
import json
from pathlib import Path

import pytest

from finance_pipeline.errors import LockError, QualityCheckError
from finance_pipeline.orchestrate import (
    TaskResult,
    main,
    overall_exit_code,
    run_task,
    run_tasks,
    write_run_log,
)

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "sql"


def _flaky(failures, exc):
    state = {"calls": 0}

    def fn():
        state["calls"] += 1
        if state["calls"] <= failures:
            raise exc
    return fn, state


def test_success_on_first_try():
    r = run_task("t", lambda: None, sleep=lambda s: None)
    assert (r.status, r.attempts, r.exit_code, r.error) == ("ok", 1, 0, None)


def test_lock_error_is_retried_with_exponential_backoff():
    fn, state = _flaky(2, LockError("busy"))
    sleeps = []
    r = run_task("t", fn, retries=2, backoff=1.0, sleep=sleeps.append)
    assert r.status == "ok" and r.attempts == 3 and sleeps == [1.0, 2.0]


def test_gives_up_after_the_retry_budget():
    fn, state = _flaky(99, LockError("busy"))
    r = run_task("t", fn, retries=1, backoff=0.5, sleep=lambda s: None)
    assert (r.status, r.attempts, r.exit_code) == ("failed", 2, 4) and state["calls"] == 2


def test_non_retryable_errors_fail_immediately():
    fn, state = _flaky(99, QualityCheckError("bad data"))
    sleeps = []
    r = run_task("t", fn, retries=5, sleep=sleeps.append)
    assert (r.status, r.attempts, r.exit_code) == ("failed", 1, 1) and sleeps == [] and state["calls"] == 1


def test_unexpected_value_error_maps_to_exit_2():
    fn, _ = _flaky(99, ValueError("bad input"))
    assert run_task("t", fn, sleep=lambda s: None).exit_code == 2


@pytest.mark.parametrize("bad", [-1, True, 1.5])
def test_bad_retry_count_is_rejected(bad):
    with pytest.raises(ValueError, match="retries"):
        run_task("t", lambda: None, retries=bad)


def test_tasks_after_a_failure_are_skipped_not_run():
    ran = []
    fail, _ = _flaky(99, QualityCheckError("x"))
    results = run_tasks([("a", lambda: ran.append("a")), ("b", fail), ("c", lambda: ran.append("c"))],
                        retries=0, sleep=lambda s: None)
    assert [(r.name, r.status) for r in results] == [("a", "ok"), ("b", "failed"), ("c", "skipped")]
    assert ran == ["a"] and overall_exit_code(results) == 1


def test_all_tasks_ok_gives_exit_0():
    results = run_tasks([("a", lambda: None), ("b", lambda: None)], sleep=lambda s: None)
    assert [r.status for r in results] == ["ok", "ok"] and overall_exit_code(results) == 0


def test_run_log_is_written_with_every_task(tmp_path):
    results = [TaskResult("a", "ok", 1, 0.1), TaskResult("b", "failed", 2, 0.2, "boom", 4)]
    summary = write_run_log(tmp_path / "logs" / "run.json", results, "2025-01-01T00:00:00+00:00")
    on_disk = json.loads((tmp_path / "logs" / "run.json").read_text(encoding="utf-8"))
    assert on_disk == summary and on_disk["exit_code"] == 4
    assert [t["name"] for t in on_disk["tasks"]] == ["a", "b"]


def _args(tmp, *extra):
    return ["--incoming", str(tmp / "incoming"), "--state-dir", str(tmp / "state"),
            "--output-dir", str(tmp / "output"), "--quarantine-dir", str(tmp / "quarantine"),
            "--sql-dir", str(SQL_DIR), "--run-log", str(tmp / "run.json"), "--backoff", "0", *extra]


def _last_json(capsys):
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_main_generates_batches_and_runs_the_pipeline(spark, tmp_path, capsys):
    code = main(_args(tmp_path, "--generate-batches", "2", "--rows", "200", "--seed", "3"), spark=spark)
    summary = _last_json(capsys)
    assert code == 0 and summary["exit_code"] == 0
    assert [(t["name"], t["status"]) for t in summary["tasks"]] == [("generate_batches", "ok"), ("incremental", "ok")]
    assert json.loads((tmp_path / "run.json").read_text(encoding="utf-8"))["exit_code"] == 0
    manifest = json.loads((tmp_path / "output" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["input_rows"] == 200 and manifest["reconciled"] is True


def test_main_run_twice_is_a_no_op_the_second_time(spark, tmp_path, capsys):
    args = _args(tmp_path, "--generate-batches", "2", "--rows", "200", "--seed", "3")
    assert main(args, spark=spark) == 0
    first = json.loads((tmp_path / "output" / "manifest.json").read_text(encoding="utf-8"))
    assert main(args, spark=spark) == 0
    assert json.loads((tmp_path / "output" / "manifest.json").read_text(encoding="utf-8")) == first


def test_main_returns_4_when_locked_and_still_writes_the_run_log(tmp_path, capsys):
    state = tmp_path / "state"
    state.mkdir()
    (state / "run.lock").write_text("{}", encoding="utf-8")
    code = main(_args(tmp_path, "--retries", "1"), spark=object(), sleep=lambda s: None)
    summary = _last_json(capsys)
    assert code == 4
    (task,) = summary["tasks"]
    assert (task["status"], task["attempts"], task["exit_code"]) == ("failed", 2, 4)
    assert (tmp_path / "run.json").is_file()


# ---- sample DAG and CI files: cheap drift guards -----------------------------

def test_sample_dag_is_valid_python_and_names_the_pipeline_tasks():
    text = (ROOT / "orchestration" / "finance_pipeline_dag.py").read_text(encoding="utf-8-sig")
    ast.parse(text)
    assert "fin-incremental" in text and "generate_batches >> incremental" in text


def test_ci_workflow_runs_fast_tests_on_push_and_full_suite_with_java():
    text = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8-sig")
    assert 'pytest -m "not slow"' in text and "github.event_name == 'push'" in text
    assert "setup-java" in text and "ruff check" in text and "mypy src" in text


def test_scheduled_workflow_checks_the_seed_42_manifest_and_uploads_artifacts():
    text = (ROOT / ".github" / "workflows" / "scheduled-run.yml").read_text(encoding="utf-8-sig")
    assert "cron:" in text and "(1000, 870, 80, 50)" in text and "upload-artifact" in text
