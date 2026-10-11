import json
import shutil
from pathlib import Path

import duckdb
import pytest

import finance_pipeline.incremental as inc
from finance_pipeline.batches import split_into_batches
from finance_pipeline.generate_data import GeneratorConfig, generate_rows, write_csv
from finance_pipeline.incremental import main, run_incremental
from finance_pipeline.ledger import BatchConflictError, file_sha256, load_ledger
from finance_pipeline.publish import run_pipeline

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
BASE = {
    "transaction_id": "TXN-000001",
    "account_id": "ACC-0001",
    "transaction_ts": "2025-01-01T09:00:00",
    "transaction_type": "CREDIT",
    "amount": "10.00",
    "currency": "INR",
    "status": "POSTED",
    "source_updated_ts": "2025-01-01T10:00:00",
}


def _row(i, status="POSTED", updated="2025-01-01T10:00:00", **kw):
    return dict(BASE, transaction_id=f"TXN-{i:06d}", status=status, source_updated_ts=updated, **kw)


def _batch(tmp, name, rows):
    write_csv(rows, tmp / "incoming" / name)


def _run(spark, tmp):
    return run_incremental(spark, tmp / "incoming", tmp / "state", tmp / "output",
                           tmp / "quarantine", None, SQL_DIR)


def _query(sql):
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _cur(tmp):
    p = (tmp / "output" / "curated" / "transactions.parquet").as_posix()
    return {r[0]: (r[1], r[2]) for r in _query(
        f"SELECT transaction_id, status, CAST(amount AS VARCHAR) FROM read_parquet('{p}')")}


def _sup(tmp):
    p = (tmp / "output" / "superseded" / "superseded.parquet").as_posix()
    return sorted(_query(f"SELECT transaction_id, status FROM read_parquet('{p}')"))


def _quar(tmp):
    p = (tmp / "quarantine" / "quarantine.csv").as_posix()
    return _query(f"SELECT batch_id, quarantine_reasons FROM read_csv('{p}', header = true, "
                  "all_varchar = true) ORDER BY batch_id, quarantine_reasons")


def _ledger_bytes(tmp):
    return (tmp / "state" / "ledger.json").read_bytes()


def test_new_ids_from_later_batches_go_to_curated(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [_row(1)])
    _run(spark, tmp_path)
    _batch(tmp_path, "b2.csv", [_row(2)])
    _run(spark, tmp_path)
    assert set(_cur(tmp_path)) == {"TXN-000001", "TXN-000002"} and _sup(tmp_path) == []


def test_newer_version_replaces_curated_and_old_row_is_superseded(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [_row(1, "PENDING", "2025-01-01T10:00:00")])
    _batch(tmp_path, "b2.csv", [_row(1, "POSTED", "2025-01-01T11:00:00")])
    _run(spark, tmp_path)
    assert _cur(tmp_path)["TXN-000001"][0] == "POSTED"
    assert _sup(tmp_path) == [("TXN-000001", "PENDING")]


def test_late_arriving_older_version_goes_straight_to_superseded(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [_row(1, "POSTED", "2025-01-01T11:00:00")])
    _batch(tmp_path, "b2.csv", [_row(1, "PENDING", "2025-01-01T10:00:00")])
    _run(spark, tmp_path)
    assert _cur(tmp_path)["TXN-000001"][0] == "POSTED"
    assert _sup(tmp_path) == [("TXN-000001", "PENDING")]


@pytest.mark.parametrize("first, second", [("POSTED", "FAILED"), ("FAILED", "POSTED")])
def test_equal_timestamp_conflict_is_deterministic_across_batch_order(spark, tmp_path, first, second):
    _batch(tmp_path, "b1.csv", [_row(1, first)])
    _batch(tmp_path, "b2.csv", [_row(1, second)])
    _run(spark, tmp_path)
    assert _cur(tmp_path)["TXN-000001"][0] == "POSTED"  # same documented rule as dedup.py


def test_invalid_rows_are_quarantined_with_their_batch_id(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [_row(1)])
    _batch(tmp_path, "b2.csv", [_row(2), _row(3, currency="EUR")])
    _run(spark, tmp_path)
    assert _quar(tmp_path) == [("b2.csv", "UNSUPPORTED_CURRENCY")]


def test_quarantine_is_appended_across_runs(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [_row(1, currency="EUR"), _row(2)])
    _run(spark, tmp_path)
    _batch(tmp_path, "b2.csv", [_row(3, status="bogus")])
    _run(spark, tmp_path)
    assert _quar(tmp_path) == [("b1.csv", "UNSUPPORTED_CURRENCY"), ("b2.csv", "INVALID_STATUS")]


def test_second_run_processes_only_the_new_file(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [_row(1)])
    _run(spark, tmp_path)
    _batch(tmp_path, "b2.csv", [_row(2)])
    result = _run(spark, tmp_path)
    assert result["processed"] == ["b2.csv"] and result["skipped"] == ["b1.csv"]
    assert result["manifest"]["batches_processed"] == 2


def test_rerunning_the_same_batches_is_a_no_op(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [_row(1), _row(2, currency="EUR")])
    first = _run(spark, tmp_path)
    ledger_before, cur_before = _ledger_bytes(tmp_path), _cur(tmp_path)
    second = _run(spark, tmp_path)
    assert second["processed"] == [] and second["skipped"] == ["b1.csv"]
    assert second["manifest"] == first["manifest"]
    assert _ledger_bytes(tmp_path) == ledger_before and _cur(tmp_path) == cur_before


def test_same_name_different_content_is_rejected_and_changes_nothing(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [_row(1)])
    _run(spark, tmp_path)
    ledger_before = _ledger_bytes(tmp_path)
    _batch(tmp_path, "b1.csv", [_row(2)])
    with pytest.raises(BatchConflictError, match="b1.csv"):
        _run(spark, tmp_path)
    assert _ledger_bytes(tmp_path) == ledger_before and set(_cur(tmp_path)) == {"TXN-000001"}


def test_same_content_under_another_name_is_skipped(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [_row(1)])
    _run(spark, tmp_path)
    shutil.copyfile(tmp_path / "incoming" / "b1.csv", tmp_path / "incoming" / "b1_copy.csv")
    result = _run(spark, tmp_path)
    assert result["processed"] == [] and "b1_copy.csv" in result["skipped"]
    assert result["manifest"]["input_rows"] == 1 and _sup(tmp_path) == []


def test_ledger_records_file_hash_rows_and_snapshot(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [_row(1), _row(2)])
    _run(spark, tmp_path)
    ledger = load_ledger(tmp_path / "state")
    (entry,) = ledger["batches"]
    assert entry["file"] == "b1.csv" and entry["input_rows"] == 2 and entry["snapshot"] == "snap_0001"
    assert entry["sha256"] == file_sha256(tmp_path / "incoming" / "b1.csv")
    assert ledger["current_snapshot"] == "snap_0001"


def test_header_only_batch_then_data_batch(spark, tmp_path):
    _batch(tmp_path, "b1.csv", [])
    first = _run(spark, tmp_path)
    assert first["manifest"]["input_rows"] == 0 and first["manifest"]["curated_rows"] == 0
    _batch(tmp_path, "b2.csv", [_row(1)])
    assert _run(spark, tmp_path)["manifest"]["curated_rows"] == 1


def test_crash_before_ledger_update_keeps_old_state_and_recovers(spark, tmp_path, monkeypatch):
    _batch(tmp_path, "b1.csv", [_row(1, "PENDING", "2025-01-01T10:00:00")])
    _run(spark, tmp_path)
    ledger_before, cur_before = _ledger_bytes(tmp_path), _cur(tmp_path)
    _batch(tmp_path, "b2.csv", [_row(1, "POSTED", "2025-01-01T11:00:00")])

    def boom(*args, **kwargs):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(inc, "save_ledger", boom)
    with pytest.raises(RuntimeError, match="simulated"):
        _run(spark, tmp_path)
    assert _ledger_bytes(tmp_path) == ledger_before and _cur(tmp_path) == cur_before
    assert [p.name for p in (tmp_path / "state" / "snapshots").iterdir()] == ["snap_0001"]

    monkeypatch.undo()
    result = _run(spark, tmp_path)
    assert result["processed"] == ["b2.csv"]
    assert _cur(tmp_path)["TXN-000001"][0] == "POSTED"
    assert _sup(tmp_path) == [("TXN-000001", "PENDING")]  # not duplicated by the retry
    assert result["manifest"]["reconciled"] and result["manifest"]["input_rows"] == 2


def test_no_batch_files_raises(spark, tmp_path):
    (tmp_path / "incoming").mkdir()
    with pytest.raises(ValueError, match="no batch files"):
        _run(spark, tmp_path)


def test_missing_incoming_directory_raises(spark, tmp_path):
    with pytest.raises(FileNotFoundError, match="incoming"):
        _run(spark, tmp_path)


# ---- the key invariant: batches A then B == A+B in one load ----------------

@pytest.fixture(scope="module")
def full42(spark, tmp_path_factory):
    d = tmp_path_factory.mktemp("full42")
    rows = generate_rows(GeneratorConfig(num_rows=1000, seed=42))
    write_csv(rows, d / "in.csv")
    run_pipeline(spark, d / "in.csv", d / "output", d / "quarantine")
    return rows, d


def _dump(out, q):
    cur = (Path(out) / "curated" / "transactions.parquet").as_posix()
    sup = (Path(out) / "superseded" / "superseded.parquet").as_posix()
    qua = (Path(q) / "quarantine.csv").as_posix()
    return (
        _query(f"SELECT * FROM read_parquet('{cur}') ORDER BY ALL"),
        _query(f"SELECT * FROM read_parquet('{sup}') ORDER BY ALL"),
        _query("SELECT transaction_id, account_id, transaction_ts, transaction_type, amount, currency, "
               "status, source_updated_ts, quarantine_reasons "
               f"FROM read_csv('{qua}', header = true, all_varchar = true) ORDER BY ALL"),
    )


@pytest.mark.parametrize("n, reverse", [(2, False), (3, False), (3, True), (5, False)])
def test_incremental_result_equals_one_shot_full_load(spark, full42, tmp_path, n, reverse):
    rows, ref = full42
    for i, chunk in enumerate(split_into_batches(rows, n)):
        idx = n - i if reverse else i + 1  # reverse = process the chunks in the opposite order
        _batch(tmp_path, f"batch_{idx:02d}.csv", chunk)
    result = _run(spark, tmp_path)
    assert result["manifest"] == {"input_rows": 1000, "curated_rows": 870, "superseded_rows": 80,
                                  "quarantined_rows": 50, "reconciled": True, "batches_processed": n}
    assert all(c.passed for c in result["checks"])
    assert _dump(tmp_path / "output", tmp_path / "quarantine") == _dump(ref / "output", ref / "quarantine")


# ---- CLI --------------------------------------------------------------------

def _args(tmp):
    return ["--incoming", str(tmp / "incoming"), "--state-dir", str(tmp / "state"),
            "--output-dir", str(tmp / "output"), "--quarantine-dir", str(tmp / "quarantine"),
            "--sql-dir", str(SQL_DIR)]


def test_main_returns_0_and_prints_summary(spark, tmp_path, capsys):
    _batch(tmp_path, "b1.csv", [_row(1)])
    assert main(_args(tmp_path), spark=spark) == 0
    last = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert last["processed"] == ["b1.csv"] and last["manifest"]["curated_rows"] == 1


def test_main_returns_3_when_a_batch_file_changed(spark, tmp_path, capsys):
    _batch(tmp_path, "b1.csv", [_row(1)])
    assert main(_args(tmp_path), spark=spark) == 0
    _batch(tmp_path, "b1.csv", [_row(2)])
    assert main(_args(tmp_path), spark=spark) == 3
    assert "b1.csv" in capsys.readouterr().err


def test_main_returns_2_for_missing_incoming_directory(spark, tmp_path, capsys):
    assert main(_args(tmp_path), spark=spark) == 2
    assert "error:" in capsys.readouterr().err
