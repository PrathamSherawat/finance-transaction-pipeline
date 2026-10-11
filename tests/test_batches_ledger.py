import pytest

from finance_pipeline.batches import main as batches_main
from finance_pipeline.batches import split_into_batches
from finance_pipeline.ledger import (
    BatchConflictError,
    classify_batch,
    empty_ledger,
    file_sha256,
    load_ledger,
    record_batch,
    save_ledger,
)


def test_sha256_is_stable_and_content_sensitive(tmp_path):
    a, b, c = tmp_path / "a.csv", tmp_path / "b.csv", tmp_path / "c.csv"
    a.write_bytes(b"hello")
    b.write_bytes(b"hello")
    c.write_bytes(b"hellO")
    assert file_sha256(a) == file_sha256(b) != file_sha256(c)
    assert len(file_sha256(a)) == 64


def test_load_missing_ledger_is_empty(tmp_path):
    assert load_ledger(tmp_path / "nope") == empty_ledger()


def test_save_load_round_trip_leaves_no_temp_file(tmp_path):
    ledger = record_batch(empty_ledger(), file="a.csv", sha256="x" * 64, input_rows=3, snapshot="snap_0001")
    save_ledger(tmp_path / "state", ledger)
    assert load_ledger(tmp_path / "state") == ledger
    assert [p.name for p in (tmp_path / "state").iterdir()] == ["ledger.json"]


def test_unknown_file_is_new():
    assert classify_batch(empty_ledger(), "a.csv", "h1") == "new"


def test_same_content_is_skipped_even_under_another_name():
    ledger = record_batch(empty_ledger(), file="a.csv", sha256="h1", input_rows=1, snapshot="snap_0001")
    assert classify_batch(ledger, "a.csv", "h1") == "skip"
    assert classify_batch(ledger, "copy.csv", "h1") == "skip"


def test_same_name_different_content_raises():
    ledger = record_batch(empty_ledger(), file="a.csv", sha256="h1", input_rows=1, snapshot="snap_0001")
    with pytest.raises(BatchConflictError, match="a.csv"):
        classify_batch(ledger, "a.csv", "h2")


def test_record_batch_does_not_mutate_and_sets_current_snapshot():
    before = empty_ledger()
    after = record_batch(before, file="a.csv", sha256="h", input_rows=2, snapshot="snap_0001")
    assert before == empty_ledger()
    assert after["current_snapshot"] == "snap_0001" and len(after["batches"]) == 1


@pytest.mark.parametrize("n", [1, 2, 3, 7])
def test_split_partitions_rows_in_order(n):
    rows = [{"i": i} for i in range(10)]
    chunks = split_into_batches(rows, n)
    assert len(chunks) == n
    assert [r for c in chunks for r in c] == rows
    assert max(map(len, chunks)) - min(map(len, chunks)) <= 1


@pytest.mark.parametrize("n", [0, -1, True, 11])
def test_split_rejects_bad_n(n):
    with pytest.raises(ValueError, match="n must be"):
        split_into_batches([{"i": i} for i in range(10)], n)


def test_batches_cli_writes_n_files_covering_all_rows(tmp_path):
    assert batches_main(["--rows", "100", "--batches", "4", "--output-dir", str(tmp_path)]) == 0
    files = sorted(tmp_path.glob("*.csv"))
    assert [f.name for f in files] == [f"transactions_000{i}.csv" for i in range(1, 5)]
    assert sum(len(f.read_text(encoding="utf-8").splitlines()) - 1 for f in files) == 100
