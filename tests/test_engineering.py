import importlib.metadata as md
import logging
import os
import time
from pathlib import Path

import pytest

from finance_pipeline import generate_data, schema, validate
from finance_pipeline.config import Settings, load_settings
from finance_pipeline.errors import (
    BatchConflictError,
    LockError,
    PipelineError,
    QualityCheckError,
    exit_code_for,
)
from finance_pipeline.incremental import main as incremental_main
from finance_pipeline.incremental import run_incremental
from finance_pipeline.lock import RunLock
from finance_pipeline.logging_setup import setup_logging

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src" / "finance_pipeline"


# ---- schema: one definition -------------------------------------------------

def test_columns_have_one_definition_in_the_contract_order():
    assert generate_data.COLUMNS is schema.COLUMNS
    assert schema.COLUMNS == (
        "transaction_id", "account_id", "transaction_ts", "transaction_type",
        "amount", "currency", "status", "source_updated_ts",
    )


def test_validator_uses_the_schema_constants():
    assert validate.TS_FORMAT == schema.TS_FORMAT
    assert validate.VALID_TYPES is schema.VALID_TYPES
    assert validate.VALID_CURRENCIES is schema.VALID_CURRENCIES
    assert validate.VALID_STATUSES is schema.VALID_STATUSES


def test_production_code_does_not_import_the_data_generator():
    allowed = {"generate_data.py", "batches.py"}
    offenders = [p.name for p in SRC.glob("*.py")
                 if p.name not in allowed
                 and "finance_pipeline.generate_data" in p.read_text(encoding="utf-8-sig")]
    assert offenders == []


# ---- config -----------------------------------------------------------------

def test_defaults_when_no_file(tmp_path):
    assert load_settings(tmp_path / "none.toml", env={}) == Settings()


def test_file_overrides_defaults(tmp_path):
    f = tmp_path / "c.toml"
    f.write_text('output_dir = "out"\nlog_level = "DEBUG"\n', encoding="utf-8")
    s = load_settings(f, env={})
    assert s.output_dir == "out" and s.log_level == "DEBUG"
    assert s.state_dir == Settings().state_dir


def test_env_overrides_file(tmp_path):
    f = tmp_path / "c.toml"
    f.write_text('output_dir = "from_file"\n', encoding="utf-8")
    assert load_settings(f, env={"FIN_OUTPUT_DIR": "from_env"}).output_dir == "from_env"


def test_empty_env_value_is_ignored(tmp_path):
    f = tmp_path / "c.toml"
    f.write_text('output_dir = "from_file"\n', encoding="utf-8")
    assert load_settings(f, env={"FIN_OUTPUT_DIR": ""}).output_dir == "from_file"


def test_unknown_setting_raises(tmp_path):
    f = tmp_path / "c.toml"
    f.write_text('bogus = "x"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="unknown"):
        load_settings(f, env={})


def test_invalid_toml_raises(tmp_path):
    f = tmp_path / "c.toml"
    f.write_text("output_dir = \n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_settings(f, env={})


def test_non_string_setting_raises(tmp_path):
    f = tmp_path / "c.toml"
    f.write_text("output_dir = 5\n", encoding="utf-8")
    with pytest.raises(ValueError, match="string"):
        load_settings(f, env={})


def test_shipped_config_file_matches_the_defaults():
    assert load_settings(ROOT / "config" / "pipeline.toml", env={}) == Settings()


# ---- logging ----------------------------------------------------------------

def test_setup_logging_is_idempotent_and_writes_to_stderr_only(capsys):
    setup_logging("INFO")
    setup_logging("INFO")
    logging.getLogger("finance_pipeline.test").info("hello %s", "world")
    captured = capsys.readouterr()
    assert captured.err.count("hello world") == 1
    assert captured.out == ""


def test_log_level_filters_messages(capsys):
    setup_logging("WARNING")
    try:
        logging.getLogger("finance_pipeline.test").info("quiet")
        logging.getLogger("finance_pipeline.test").warning("loud")
        err = capsys.readouterr().err
        assert "quiet" not in err and "loud" in err
    finally:
        setup_logging("INFO")


def test_unknown_log_level_raises():
    with pytest.raises(ValueError, match="log level"):
        setup_logging("LOUD")


# ---- errors and exit codes --------------------------------------------------

@pytest.mark.parametrize("exc, code", [
    (QualityCheckError("x"), 1), (BatchConflictError("x"), 3), (LockError("x"), 4),
    (ValueError("x"), 2), (FileNotFoundError("x"), 2), (PipelineError("x"), 2),
])
def test_exit_code_mapping(exc, code):
    assert exit_code_for(exc) == code


def test_pipeline_errors_share_one_base():
    for cls in (QualityCheckError, BatchConflictError, LockError):
        assert issubclass(cls, PipelineError)


def test_errors_are_still_importable_from_their_original_modules():
    from finance_pipeline.ledger import BatchConflictError as FromLedger
    from finance_pipeline.quality import QualityCheckError as FromQuality
    assert FromLedger is BatchConflictError and FromQuality is QualityCheckError


# ---- lock -------------------------------------------------------------------

def test_lock_is_created_and_removed(tmp_path):
    p = tmp_path / "state" / "run.lock"
    with RunLock(p):
        assert p.is_file()
    assert not p.exists()


def test_second_holder_is_refused_and_the_first_keeps_its_lock(tmp_path):
    p = tmp_path / "run.lock"
    with RunLock(p):
        with pytest.raises(LockError, match="run.lock"):
            with RunLock(p):
                pass
        assert p.is_file()
    assert not p.exists()


def test_lock_is_released_after_an_exception(tmp_path):
    p = tmp_path / "run.lock"
    with pytest.raises(RuntimeError):
        with RunLock(p):
            raise RuntimeError("boom")
    assert not p.exists()


def test_stale_lock_is_replaced(tmp_path):
    p = tmp_path / "run.lock"
    p.write_text("{}", encoding="utf-8")
    old = time.time() - 100
    os.utime(p, (old, old))
    with RunLock(p, max_age_seconds=10):
        assert str(os.getpid()) in p.read_text(encoding="utf-8")


def test_fresh_lock_is_not_replaced(tmp_path):
    p = tmp_path / "run.lock"
    p.write_text("{}", encoding="utf-8")
    with pytest.raises(LockError):
        RunLock(p, max_age_seconds=3600).__enter__()
    assert p.read_text(encoding="utf-8") == "{}"


def test_run_incremental_refuses_when_locked_before_touching_anything(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "run.lock").write_text("{}", encoding="utf-8")
    with pytest.raises(LockError):
        run_incremental(None, tmp_path / "incoming", state, tmp_path / "o", tmp_path / "q")


def test_incremental_main_returns_4_when_locked(tmp_path, capsys):
    state = tmp_path / "state"
    state.mkdir()
    (state / "run.lock").write_text("{}", encoding="utf-8")
    args = ["--incoming", str(tmp_path / "incoming"), "--state-dir", str(state),
            "--output-dir", str(tmp_path / "o"), "--quarantine-dir", str(tmp_path / "q")]
    assert incremental_main(args, spark=object()) == 4
    assert "run.lock" in capsys.readouterr().err


# ---- entry points (needs: pip install -e . after this milestone) -------------

def test_console_scripts_are_installed():
    names = {e.name for e in md.entry_points(group="console_scripts")}
    assert {"fin-generate", "fin-run", "fin-incremental"} <= names
