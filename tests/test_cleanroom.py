from pathlib import Path

import pytest

from finance_pipeline.cleanroom import EXPECTED_SEED_42, run_cleanroom

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def test_cleanroom_reproduces_the_seed_42_numbers(spark, tmp_path):
    result = run_cleanroom(spark, tmp_path / "cr", sql_dir=SQL_DIR)
    assert result["passed"], result["checks"]
    assert len(result["checks"]) == 5


def test_cleanroom_reports_a_wrong_expectation_as_a_failed_check(spark, tmp_path):
    wrong = dict(EXPECTED_SEED_42, curated_rows=871)
    result = run_cleanroom(spark, tmp_path / "cr", expected=wrong, sql_dir=SQL_DIR)
    assert result["passed"] is False
    assert [c["name"] for c in result["checks"] if not c["passed"]] == ["manifest matches expected"]


def test_cleanroom_refuses_a_non_empty_workdir(spark, tmp_path):
    (tmp_path / "keep.txt").write_text("precious", encoding="utf-8")
    with pytest.raises(ValueError, match="not empty"):
        run_cleanroom(spark, tmp_path, sql_dir=SQL_DIR)
    assert (tmp_path / "keep.txt").read_text(encoding="utf-8") == "precious"
