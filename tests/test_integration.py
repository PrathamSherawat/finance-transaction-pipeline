import json
import shutil
from pathlib import Path

import duckdb
import pytest

from finance_pipeline.generate_data import GeneratorConfig, generate_rows, write_csv
from finance_pipeline.pipeline import main, run_all
from finance_pipeline.quality import (
    KNOWN_REASON_CODES,
    QualityCheckError,
    format_report,
    run_quality_checks,
)
from finance_pipeline.validate import _rules

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

ALL_CHECKS = [
    "files_present", "manifest_matches_files", "reconciliation", "curated_ids_unique",
    "curated_no_nulls", "curated_domain_values", "curated_amount_positive", "curated_ts_order",
    "superseded_ids_in_curated", "no_superseded_newer", "quarantine_reasons_known",
    "kpi_totals_agree", "kpi_status_sums_to_curated",
]
SEED42 = {"input_rows": 1000, "curated_rows": 870, "superseded_rows": 80,
          "quarantined_rows": 50, "reconciled": True}


@pytest.fixture(scope="module")
def run42(spark, tmp_path_factory):
    d = tmp_path_factory.mktemp("integration")
    src = d / "in.csv"
    write_csv(generate_rows(GeneratorConfig(num_rows=1000, seed=42)), src)
    result = run_all(spark, src, d / "output", d / "quarantine", d / "kpis", SQL_DIR)
    return d, result


@pytest.fixture
def copy42(run42, tmp_path):
    d, _ = run42
    out, q = tmp_path / "output", tmp_path / "quarantine"
    shutil.copytree(d / "output", out)
    shutil.copytree(d / "quarantine", q)
    return out, q


def _failed(out, q, sql_dir=SQL_DIR):
    return {r.name for r in run_quality_checks(out, q, sql_dir) if not r.passed}


def _rewrite(path, select_sql):
    tmp = path.with_name("tmp_" + path.name)
    con = duckdb.connect()
    try:
        con.execute(f"COPY ({select_sql.format(src=path.as_posix())}) "
                    f"TO '{tmp.as_posix()}' (FORMAT PARQUET)")
    finally:
        con.close()
    tmp.replace(path)


def _broken_sql_dir(tmp_path, filename, old, new):
    d = tmp_path / "sql"
    shutil.copytree(SQL_DIR, d)
    f = d / filename
    text = f.read_text(encoding="utf-8-sig")
    assert text.count(old) == 1
    f.write_text(text.replace(old, new), encoding="utf-8")
    return d


def _args(tmp_path, src, sql_dir=SQL_DIR):
    return ["--input", str(src), "--output-dir", str(tmp_path / "output"),
            "--quarantine-dir", str(tmp_path / "quarantine"),
            "--kpi-dir", str(tmp_path / "kpis"), "--sql-dir", str(sql_dir)]


def _small_input(tmp_path, n=200, seed=3):
    src = tmp_path / "in.csv"
    write_csv(generate_rows(GeneratorConfig(num_rows=n, seed=seed)), src)
    return src


# ---- the happy path --------------------------------------------------------

def test_known_reason_codes_match_the_validator(spark):
    assert {code for code, _ in _rules()} == KNOWN_REASON_CODES


def test_all_checks_pass_on_seed_42(run42):
    d, result = run42
    results = run_quality_checks(d / "output", d / "quarantine", SQL_DIR)
    assert [r.name for r in results] == ALL_CHECKS
    assert all(r.passed for r in results), format_report(results)
    assert all(r.passed for r in result["checks"])


def test_report_summarises_the_run(run42):
    d, _ = run42
    report = format_report(run_quality_checks(d / "output", d / "quarantine", SQL_DIR))
    assert report.splitlines()[-1] == "13/13 checks passed"
    assert "[FAIL]" not in report


def test_end_to_end_manifest(run42):
    assert run42[1]["manifest"] == SEED42


def test_end_to_end_writes_the_four_kpi_files(run42):
    d, _ = run42
    assert sorted(p.name for p in (d / "kpis").iterdir()) == [
        "by_account.csv", "daily.csv", "status_summary.csv", "totals.csv"]
    lines = (d / "kpis" / "totals.csv").read_text(encoding="utf-8").splitlines()
    assert lines[0] == "currency,inflow,outflow,net,txn_count"
    assert [x.split(",")[0] for x in lines[1:]] == ["INR", "USD"]


# ---- check the checker: corrupt one thing, the matching check must fail ----

PARQUET_CORRUPTIONS = [
    ("curated_ids_unique", "curated",
     "SELECT * FROM read_parquet('{src}') UNION ALL "
     "SELECT * FROM (SELECT * FROM read_parquet('{src}') LIMIT 1)"),
    ("curated_no_nulls", "curated",
     "SELECT * REPLACE (CAST(NULL AS VARCHAR) AS account_id) FROM read_parquet('{src}')"),
    ("curated_domain_values", "curated",
     "SELECT * REPLACE ('EUR' AS currency) FROM read_parquet('{src}')"),
    ("curated_amount_positive", "curated",
     "SELECT * REPLACE (CAST(-1 AS DECIMAL(18,2)) AS amount) FROM read_parquet('{src}')"),
    ("curated_ts_order", "curated",
     "SELECT * REPLACE (transaction_ts - INTERVAL 1 DAY AS source_updated_ts) FROM read_parquet('{src}')"),
    ("superseded_ids_in_curated", "superseded",
     "SELECT * REPLACE ('TXN-999999' AS transaction_id) FROM read_parquet('{src}')"),
    ("no_superseded_newer", "superseded",
     "SELECT * REPLACE (source_updated_ts + INTERVAL 30 DAY AS source_updated_ts) FROM read_parquet('{src}')"),
]


@pytest.mark.parametrize("check, which, sql", PARQUET_CORRUPTIONS, ids=[c[0] for c in PARQUET_CORRUPTIONS])
def test_corrupted_parquet_is_caught(copy42, check, which, sql):
    out, q = copy42
    path = (out / "curated" / "transactions.parquet" if which == "curated"
            else out / "superseded" / "superseded.parquet")
    _rewrite(path, sql)
    failed = _failed(out, q)
    assert check in failed
    assert "quarantine_reasons_known" not in failed  # unrelated checks stay green


@pytest.mark.parametrize("key, check", [("curated_rows", "manifest_matches_files"),
                                        ("input_rows", "reconciliation")])
def test_edited_manifest_is_caught(copy42, key, check):
    out, q = copy42
    m = out / "manifest.json"
    data = json.loads(m.read_text(encoding="utf-8"))
    data[key] += 1
    m.write_text(json.dumps(data), encoding="utf-8")
    assert check in _failed(out, q)


def test_unknown_quarantine_reason_is_caught(copy42):
    out, q = copy42
    f = q / "quarantine.csv"
    text = f.read_text(encoding="utf-8")
    assert "INVALID_STATUS" in text
    f.write_text(text.replace("INVALID_STATUS", "BOGUS_CODE"), encoding="utf-8")
    assert "quarantine_reasons_known" in _failed(out, q)


def test_empty_quarantine_reason_is_caught(copy42):
    out, q = copy42
    f = q / "quarantine.csv"
    text = f.read_text(encoding="utf-8")
    assert ",INVALID_STATUS\n" in text
    f.write_text(text.replace(",INVALID_STATUS\n", ",\n"), encoding="utf-8")
    assert "quarantine_reasons_known" in _failed(out, q)


@pytest.mark.parametrize("filename, old, new, check", [
    ("kpi_totals.sql", "WHERE status = 'POSTED'", "WHERE 1 = 1", "kpi_totals_agree"),
    ("kpi_status_summary.sql", "GROUP BY status", "WHERE status = 'POSTED'\nGROUP BY status",
     "kpi_status_sums_to_curated"),
])
def test_a_wrong_kpi_query_is_caught(copy42, tmp_path, filename, old, new, check):
    out, q = copy42
    bad = _broken_sql_dir(tmp_path, filename, old, new)
    assert check in _failed(out, q, bad)


def test_missing_output_file_is_reported_alone(copy42):
    out, q = copy42
    (out / "superseded" / "superseded.parquet").unlink()
    results = run_quality_checks(out, q, SQL_DIR)
    assert [(r.name, r.passed) for r in results] == [("files_present", False)]


# ---- orchestrator behaviour ------------------------------------------------

def test_failed_check_prints_report_raises_and_skips_kpis(spark, tmp_path, capsys):
    bad = _broken_sql_dir(tmp_path, "kpi_totals.sql", "WHERE status = 'POSTED'", "WHERE 1 = 1")
    with pytest.raises(QualityCheckError, match="kpi_totals_agree"):
        run_all(spark, _small_input(tmp_path), tmp_path / "output", tmp_path / "quarantine",
                tmp_path / "kpis", bad)
    assert "[FAIL] kpi_totals_agree" in capsys.readouterr().out
    assert not (tmp_path / "kpis").exists()


def _snapshot(out, kpis):
    con = duckdb.connect()
    try:
        rows = con.execute("SELECT * FROM read_parquet('%s') ORDER BY ALL"
                           % (out / "curated" / "transactions.parquet").as_posix()).fetchall()
    finally:
        con.close()
    return rows, (kpis / "totals.csv").read_text(encoding="utf-8")


def test_running_twice_gives_identical_outputs_and_report(spark, tmp_path):
    src = _small_input(tmp_path, 300, 5)
    out, q, k = tmp_path / "output", tmp_path / "quarantine", tmp_path / "kpis"
    first = run_all(spark, src, out, q, k, SQL_DIR)
    snap1 = _snapshot(out, k)
    second = run_all(spark, src, out, q, k, SQL_DIR)
    assert first["manifest"] == second["manifest"]
    assert format_report(first["checks"]) == format_report(second["checks"])
    assert snap1 == _snapshot(out, k)


def test_header_only_input_passes_every_check(spark, tmp_path):
    src = tmp_path / "empty.csv"
    write_csv([], src)
    result = run_all(spark, src, tmp_path / "output", tmp_path / "quarantine",
                     tmp_path / "kpis", SQL_DIR)
    assert all(r.passed for r in result["checks"]), format_report(result["checks"])


# ---- CLI exit codes --------------------------------------------------------

def test_main_returns_0_and_writes_outputs(spark, tmp_path):
    assert main(_args(tmp_path, _small_input(tmp_path)), spark=spark) == 0
    assert (tmp_path / "kpis" / "totals.csv").is_file()


def test_main_returns_2_for_missing_input(spark, tmp_path, capsys):
    assert main(_args(tmp_path, tmp_path / "nope.csv"), spark=spark) == 2
    assert "error:" in capsys.readouterr().err


def test_main_returns_1_when_a_check_fails(spark, tmp_path, capsys):
    bad = _broken_sql_dir(tmp_path, "kpi_totals.sql", "WHERE status = 'POSTED'", "WHERE 1 = 1")
    assert main(_args(tmp_path, _small_input(tmp_path), bad), spark=spark) == 1
    assert "kpi_totals_agree" in capsys.readouterr().err
