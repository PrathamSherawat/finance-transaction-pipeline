import csv
import dataclasses
import random
import re
import subprocess
import sys
from datetime import timedelta
from datetime import date, datetime

import pytest

from finance_pipeline.generate_data import (
    GeneratorConfig,
    compute_row_counts,
    format_amount,
    format_ts,
    make_account_pool,
    make_valid_transaction,
    make_duplicate_version,
    apply_defect,
    generate_rows,
    write_csv,
    main,
    DEFECTS,
    COLUMNS,
)


def test_defaults():
    cfg = GeneratorConfig()
    assert cfg.num_rows == 1000
    assert cfg.seed == 42
    assert cfg.invalid_rate == 0.05
    assert cfg.duplicate_rate == 0.08
    assert cfg.start_date == date(2025, 1, 1)
    assert cfg.window_days == 90


def test_config_is_frozen():
    cfg = GeneratorConfig()
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.num_rows = 5


@pytest.mark.parametrize(
    "field, bad_value",
    [
        ("num_rows", 0),
        ("num_rows", -5),
        ("num_rows", True),
        ("num_rows", 1000.0),
        ("invalid_rate", -0.1),
        ("invalid_rate", 1.0),
        ("duplicate_rate", 1.5),
        ("window_days", 0),
        ("start_date", "2025-01-01"),
    ],
)
def test_bad_values_raise(field, bad_value):
    with pytest.raises(ValueError, match=field):
        GeneratorConfig(**{field: bad_value})


def test_rates_must_leave_room_for_valid_rows():
    with pytest.raises(ValueError, match="sum"):
        GeneratorConfig(invalid_rate=0.6, duplicate_rate=0.5)



def test_default_counts():
    c = compute_row_counts(GeneratorConfig())
    assert (c.valid, c.invalid, c.duplicate) == (870, 50, 80)


@pytest.mark.parametrize("n", [1, 2, 3, 10, 99, 100, 1000, 12345])
@pytest.mark.parametrize(
    "rates", [(0, 0), (0.05, 0.08), (0.29, 0), (0.4, 0.29), (0.9, 0.04)]
)
def test_counts_partition_num_rows(n, rates):
    cfg = GeneratorConfig(num_rows=n, invalid_rate=rates[0], duplicate_rate=rates[1])
    c = compute_row_counts(cfg)
    assert c.valid + c.invalid + c.duplicate == n
    assert c.valid >= 1
    assert c.invalid >= 0 and c.duplicate >= 0


def test_zero_rates_means_all_valid():
    cfg = GeneratorConfig(num_rows=50, invalid_rate=0, duplicate_rate=0)
    assert compute_row_counts(cfg) == (50, 0, 0)


def test_float_error_does_not_lose_a_row():
    # 0.29 * 100 == 28.999999999999996 in floating point
    cfg = GeneratorConfig(num_rows=100, invalid_rate=0.29, duplicate_rate=0)
    assert compute_row_counts(cfg).invalid == 29



@pytest.mark.parametrize(
    "cents, expected",
    [(12345, "123.45"), (5, "0.05"), (0, "0.00"), (100, "1.00"), (-5, "-0.05"), (-12345, "-123.45")],
)
def test_format_amount(cents, expected):
    assert format_amount(cents) == expected


@pytest.mark.parametrize("bad", [1.5, "100", True, None])
def test_format_amount_rejects_non_int(bad):
    with pytest.raises(ValueError, match="cents"):
        format_amount(bad)


def test_format_ts_has_no_microseconds_or_timezone():
    dt = datetime(2025, 3, 4, 5, 6, 7, 891011)
    assert format_ts(dt) == "2025-03-04T05:06:07"


def test_account_pool_is_deterministic_and_unique():
    pool = make_account_pool(random.Random(1), 25)
    assert pool == make_account_pool(random.Random(999), 25)
    assert len(pool) == len(set(pool)) == 25
    assert pool[0] == "ACC-0001" and pool[-1] == "ACC-0025"


@pytest.mark.parametrize("bad", [0, -1, True, 2.5])
def test_account_pool_rejects_bad_size(bad):
    with pytest.raises(ValueError, match="size"):
        make_account_pool(random.Random(1), bad)



def _make(seed=1, index=1, cfg=None, pool_size=10):
    cfg = cfg or GeneratorConfig()
    rng = random.Random(seed)
    pool = make_account_pool(rng, pool_size)
    return make_valid_transaction(rng, index, pool, cfg), pool, cfg


def test_valid_transaction_keys_match_columns_in_order():
    rec, _, _ = _make()
    assert tuple(rec.keys()) == COLUMNS


def test_valid_transaction_same_seed_same_record_different_seed_differs():
    assert _make(seed=7)[0] == _make(seed=7)[0]
    recs = {tuple(_make(seed=s)[0].items()) for s in range(20)}
    assert len(recs) > 1


def test_valid_transaction_id_comes_from_index():
    assert _make(index=42)[0]["transaction_id"] == "TXN-000042"


def test_valid_transaction_field_invariants():
    cfg = GeneratorConfig(window_days=30)
    rng = random.Random(123)
    pool = make_account_pool(rng, 15)
    start = datetime.combine(cfg.start_date, datetime.min.time())
    end = start + timedelta(days=cfg.window_days)
    for i in range(1, 501):
        r = make_valid_transaction(rng, i, pool, cfg)
        assert r["account_id"] in pool
        assert r["transaction_type"] in {"CREDIT", "DEBIT"}
        assert r["currency"] in {"INR", "USD"}
        assert r["status"] in {"POSTED", "PENDING", "FAILED"}
        assert re.fullmatch(r"\d+\.\d{2}", r["amount"])
        assert float(r["amount"]) > 0
        txn = datetime.fromisoformat(r["transaction_ts"])
        upd = datetime.fromisoformat(r["source_updated_ts"])
        assert start <= txn < end
        assert upd > txn


def test_valid_transaction_rejects_empty_accounts():
    with pytest.raises(ValueError, match="accounts"):
        make_valid_transaction(random.Random(1), 1, [], GeneratorConfig())


@pytest.mark.parametrize("bad", [0, -1, True])
def test_valid_transaction_rejects_bad_index(bad):
    with pytest.raises(ValueError, match="index"):
        make_valid_transaction(random.Random(1), bad, ["ACC-0001"], GeneratorConfig())


def test_valid_transaction_distribution_is_plausible():
    cfg = GeneratorConfig()
    rng = random.Random(5)
    pool = make_account_pool(rng, 20)
    recs = [make_valid_transaction(rng, i, pool, cfg) for i in range(1, 2001)]
    statuses = [r["status"] for r in recs]
    assert {r["currency"] for r in recs} == {"INR", "USD"}
    assert {r["transaction_type"] for r in recs} == {"CREDIT", "DEBIT"}
    assert statuses.count("POSTED") > statuses.count("PENDING") > 0
    assert statuses.count("FAILED") > 0



IMMUTABLE_FIELDS = (
    "transaction_id",
    "account_id",
    "transaction_ts",
    "transaction_type",
    "currency",
)


def _base_record(seed=1):
    rec, _, _ = _make(seed=seed)
    return rec


def test_duplicate_version_keeps_identity_fields():
    orig = _base_record()
    dup = make_duplicate_version(random.Random(2), orig)
    for f in IMMUTABLE_FIELDS:
        assert dup[f] == orig[f]
    assert tuple(dup.keys()) == COLUMNS


def test_duplicate_version_is_strictly_later():
    for seed in range(200):
        orig = _base_record(seed=seed)
        dup = make_duplicate_version(random.Random(seed), orig)
        assert datetime.fromisoformat(dup["source_updated_ts"]) > datetime.fromisoformat(
            orig["source_updated_ts"]
        )


def test_duplicate_version_status_always_changes():
    for seed in range(200):
        orig = _base_record(seed=seed)
        dup = make_duplicate_version(random.Random(seed), orig)
        assert dup["status"] != orig["status"]
        assert dup["status"] in {"POSTED", "PENDING", "FAILED"}


def test_duplicate_version_does_not_mutate_original():
    orig = _base_record()
    snapshot = dict(orig)
    make_duplicate_version(random.Random(3), orig)
    assert orig == snapshot


def test_duplicate_version_amount_sometimes_changes_and_stays_valid():
    changed = 0
    for seed in range(300):
        orig = _base_record(seed=seed)
        dup = make_duplicate_version(random.Random(seed), orig)
        assert re.fullmatch(r"\d+\.\d{2}", dup["amount"])
        assert float(dup["amount"]) > 0
        changed += dup["amount"] != orig["amount"]
    assert 0 < changed < 300


def test_duplicate_version_is_reproducible():
    orig = _base_record()
    a = make_duplicate_version(random.Random(9), orig)
    b = make_duplicate_version(random.Random(9), orig)
    assert a == b


def test_duplicate_version_rejects_missing_columns():
    orig = _base_record()
    del orig["status"]
    with pytest.raises(ValueError, match="missing"):
        make_duplicate_version(random.Random(1), orig)



def _changed_fields(a, b):
    return [k for k in COLUMNS if a[k] != b[k]]


@pytest.mark.parametrize("defect", DEFECTS)
def test_defect_changes_exactly_one_field(defect):
    for seed in range(50):
        orig = _base_record(seed=seed)
        bad = apply_defect(random.Random(seed), orig, defect)
        assert len(_changed_fields(orig, bad)) == 1
        assert tuple(bad.keys()) == COLUMNS


@pytest.mark.parametrize("defect", DEFECTS)
def test_defect_does_not_mutate_input(defect):
    orig = _base_record()
    snapshot = dict(orig)
    apply_defect(random.Random(1), orig, defect)
    assert orig == snapshot


def test_defect_values_are_actually_invalid():
    rng = random.Random(4)
    for seed in range(100):
        orig = _base_record(seed=seed)
        assert apply_defect(rng, orig, "missing_transaction_id")["transaction_id"] == ""
        assert apply_defect(rng, orig, "missing_account_id")["account_id"] == ""
        with pytest.raises(ValueError):
            datetime.fromisoformat(
                apply_defect(rng, orig, "malformed_transaction_ts")["transaction_ts"]
            )
        assert apply_defect(rng, orig, "invalid_transaction_type")["transaction_type"] == "TRANSFER"
        assert float(apply_defect(rng, orig, "non_positive_amount")["amount"]) <= 0
        assert apply_defect(rng, orig, "unsupported_currency")["currency"] not in {"INR", "USD"}
        assert apply_defect(rng, orig, "invalid_status")["status"] not in {
            "POSTED",
            "PENDING",
            "FAILED",
        }


def test_unknown_defect_raises_with_valid_names():
    with pytest.raises(ValueError, match="valid defects"):
        apply_defect(random.Random(1), _base_record(), "bogus")


def test_defect_is_reproducible():
    orig = _base_record()
    for defect in DEFECTS:
        assert apply_defect(random.Random(8), orig, defect) == apply_defect(
            random.Random(8), orig, defect
        )



from collections import Counter, defaultdict


def _defect_of(row):
    """Independent classifier: return the defect name, or None if row is valid."""
    if row["transaction_id"] == "":
        return "missing_transaction_id"
    if row["account_id"] == "":
        return "missing_account_id"
    try:
        datetime.fromisoformat(row["transaction_ts"])
    except ValueError:
        return "malformed_transaction_ts"
    if row["transaction_type"] not in {"CREDIT", "DEBIT"}:
        return "invalid_transaction_type"
    if float(row["amount"]) <= 0:
        return "non_positive_amount"
    if row["currency"] not in {"INR", "USD"}:
        return "unsupported_currency"
    if row["status"] not in {"POSTED", "PENDING", "FAILED"}:
        return "invalid_status"
    return None


def _duplicate_groups(rows):
    """Map transaction_id -> list of (file_position, row), only ids seen twice."""
    groups = defaultdict(list)
    for pos, row in enumerate(rows):
        if row["transaction_id"]:
            groups[row["transaction_id"]].append((pos, row))
    return {k: v for k, v in groups.items() if len(v) > 1}


@pytest.mark.parametrize("n", [1, 2, 3, 10, 99, 1000, 2500])
def test_generate_rows_returns_exactly_num_rows(n):
    assert len(generate_rows(GeneratorConfig(num_rows=n))) == n


def test_generate_rows_columns_in_order():
    for row in generate_rows(GeneratorConfig(num_rows=200)):
        assert tuple(row.keys()) == COLUMNS


def test_generate_rows_is_reproducible():
    cfg = GeneratorConfig(num_rows=300, seed=7)
    assert generate_rows(cfg) == generate_rows(cfg)


def test_generate_rows_differs_across_seeds():
    a = generate_rows(GeneratorConfig(num_rows=300, seed=1))
    b = generate_rows(GeneratorConfig(num_rows=300, seed=2))
    assert a != b


@pytest.mark.parametrize(
    "n, inv, dup",
    [(1, 0.05, 0.08), (10, 0.2, 0.2), (99, 0.05, 0.08), (1000, 0.05, 0.08)],
)
def test_generate_rows_breakdown_matches_counts(n, inv, dup):
    cfg = GeneratorConfig(num_rows=n, invalid_rate=inv, duplicate_rate=dup)
    counts = compute_row_counts(cfg)
    rows = generate_rows(cfg)
    invalid_rows = [r for r in rows if _defect_of(r) is not None]
    ids = Counter(r["transaction_id"] for r in rows if r["transaction_id"])
    extra_versions = sum(c - 1 for c in ids.values())
    assert len(invalid_rows) == counts.invalid
    assert extra_versions == counts.duplicate
    assert len(rows) - len(invalid_rows) - extra_versions == counts.valid


def test_generate_rows_contains_every_defect_type():
    rows = generate_rows(GeneratorConfig(num_rows=1000))
    found = {_defect_of(r) for r in rows} - {None}
    assert found == set(DEFECTS)


def test_generate_rows_duplicate_versions_are_ordered_and_consistent():
    rows = generate_rows(GeneratorConfig(num_rows=1000))
    groups = _duplicate_groups(rows)
    assert groups
    for versions in groups.values():
        assert len(versions) == 2
        older, newer = sorted(
            (r for _, r in versions), key=lambda r: r["source_updated_ts"]
        )
        assert newer["source_updated_ts"] > older["source_updated_ts"]
        for f in IMMUTABLE_FIELDS:
            assert newer[f] == older[f]


def test_generate_rows_file_order_does_not_reveal_newest_version():
    rows = generate_rows(GeneratorConfig(num_rows=1000))
    newest_first = 0
    for versions in _duplicate_groups(rows).values():
        by_pos = sorted(versions, key=lambda pr: pr[0])
        if by_pos[0][1]["source_updated_ts"] > by_pos[1][1]["source_updated_ts"]:
            newest_first += 1
    assert newest_first > 0


def test_generate_rows_zero_rates_gives_only_unique_valid_rows():
    cfg = GeneratorConfig(num_rows=50, invalid_rate=0, duplicate_rate=0)
    rows = generate_rows(cfg)
    assert all(_defect_of(r) is None for r in rows)
    assert len({r["transaction_id"] for r in rows}) == 50


def test_config_rejects_duplicates_exceeding_base_rows():
    with pytest.raises(ValueError, match="duplicate"):
        GeneratorConfig(num_rows=100, invalid_rate=0.05, duplicate_rate=0.9)


@pytest.mark.parametrize("n", [1, 2, 3, 7, 10, 99, 100, 1000])
@pytest.mark.parametrize(
    "rates", [(0.05, 0.08), (0.4, 0.29), (0.0, 0.49), (0.98, 0.0), (0.1, 0.44)]
)
def test_duplicates_never_exceed_valid_rows(n, rates):
    cfg = GeneratorConfig(num_rows=n, invalid_rate=rates[0], duplicate_rate=rates[1])
    c = compute_row_counts(cfg)
    assert c.duplicate <= c.valid



def _rows(n=50, seed=1):
    return generate_rows(GeneratorConfig(num_rows=n, seed=seed))


def test_write_csv_header_is_columns_exactly(tmp_path):
    p = tmp_path / "t.csv"
    write_csv(_rows(), p)
    with p.open(newline="", encoding="utf-8") as f:
        assert next(csv.reader(f)) == list(COLUMNS)


def test_write_csv_round_trip_matches_rows(tmp_path):
    rows = _rows(300)
    p = tmp_path / "t.csv"
    write_csv(rows, p)
    with p.open(newline="", encoding="utf-8") as f:
        assert list(csv.DictReader(f)) == rows


def test_write_csv_creates_nested_directories(tmp_path):
    p = tmp_path / "a" / "b" / "c" / "t.csv"
    write_csv(_rows(10), p)
    assert p.is_file()


def test_write_csv_accepts_string_path(tmp_path):
    p = tmp_path / "t.csv"
    write_csv(_rows(10), str(p))
    assert p.is_file()


def test_write_csv_returns_row_count(tmp_path):
    assert write_csv(_rows(37), tmp_path / "t.csv") == 37


def test_write_csv_empty_rows_writes_header_only(tmp_path):
    p = tmp_path / "t.csv"
    assert write_csv([], p) == 0
    assert p.read_text(encoding="utf-8") == ",".join(COLUMNS) + "\n"


def test_write_csv_uses_lf_line_endings(tmp_path):
    p = tmp_path / "t.csv"
    write_csv(_rows(20), p)
    assert b"\r" not in p.read_bytes()


def test_write_csv_same_seed_gives_identical_bytes(tmp_path):
    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    write_csv(_rows(200, seed=5), a)
    write_csv(_rows(200, seed=5), b)
    assert a.read_bytes() == b.read_bytes()


def test_write_csv_overwrites_existing_file(tmp_path):
    p = tmp_path / "t.csv"
    write_csv(_rows(30), p)
    write_csv(_rows(5), p)
    assert len(p.read_text(encoding="utf-8").splitlines()) == 6


def test_write_csv_rejects_missing_column_and_writes_nothing(tmp_path):
    rows = _rows(5)
    del rows[2]["status"]
    p = tmp_path / "t.csv"
    with pytest.raises(ValueError, match="columns"):
        write_csv(rows, p)
    assert not p.exists()


def test_write_csv_rejects_extra_column(tmp_path):
    rows = _rows(5)
    rows[0]["extra"] = "x"
    with pytest.raises(ValueError, match="columns"):
        write_csv(rows, tmp_path / "t.csv")



def test_main_writes_file_and_returns_zero(tmp_path):
    p = tmp_path / "out" / "t.csv"
    assert main(["--rows", "100", "--output", str(p)]) == 0
    with p.open(newline="", encoding="utf-8") as f:
        assert len(list(csv.DictReader(f))) == 100


def test_main_prints_summary(tmp_path, capsys):
    main(["--rows", "100", "--seed", "3", "--output", str(tmp_path / "t.csv")])
    out = capsys.readouterr().out
    assert "wrote 100 rows" in out and "seed=3" in out


def test_main_defaults_give_1000_rows(tmp_path):
    p = tmp_path / "t.csv"
    assert main(["--output", str(p)]) == 0
    assert len(p.read_text(encoding="utf-8").splitlines()) == 1001


def test_main_same_args_give_identical_bytes(tmp_path):
    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    main(["--rows", "200", "--seed", "9", "--output", str(a)])
    main(["--rows", "200", "--seed", "9", "--output", str(b)])
    assert a.read_bytes() == b.read_bytes()


def test_main_invalid_config_returns_2_and_writes_nothing(tmp_path, capsys):
    p = tmp_path / "t.csv"
    assert main(["--rows", "0", "--output", str(p)]) == 2
    assert "error:" in capsys.readouterr().err
    assert not p.exists()


def test_main_too_many_duplicates_returns_2(tmp_path, capsys):
    p = tmp_path / "t.csv"
    code = main(["--rows", "100", "--duplicate-rate", "0.9", "--output", str(p)])
    assert code == 2
    assert "duplicate" in capsys.readouterr().err
    assert not p.exists()


def test_main_non_integer_rows_is_rejected_by_argparse(tmp_path):
    with pytest.raises(SystemExit) as exc:
        main(["--rows", "abc", "--output", str(tmp_path / "t.csv")])
    assert exc.value.code == 2


def test_module_runs_via_python_dash_m(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "finance_pipeline.generate_data",
         "--rows", "100", "--seed", "42", "--output", "out/t.csv"],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "out" / "t.csv").is_file()
