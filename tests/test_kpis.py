from collections import defaultdict
from datetime import datetime
from decimal import Decimal

import duckdb
import pytest

from finance_pipeline.generate_data import COLUMNS, GeneratorConfig, generate_rows, write_csv
from finance_pipeline.kpis import KPI_FILES, compute_kpis, write_kpis
from finance_pipeline.publish import run_pipeline

SQL_DIR = __import__("pathlib").Path(__file__).resolve().parent.parent / "sql"

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
_counter = iter(range(1, 10_000))


def _row(**kw):
    r = dict(BASE, **kw)
    if "transaction_id" not in kw:
        r["transaction_id"] = f"TXN-{next(_counter):06d}"
    return r


def _fixture(tmp_path, rows, name="curated.parquet"):
    """Write typed rows as a curated-style Parquet file, independent of the pipeline."""
    p = tmp_path / name
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE stage (" + ", ".join(f"{c} VARCHAR" for c in COLUMNS) + ")")
        if rows:
            con.executemany("INSERT INTO stage VALUES (?,?,?,?,?,?,?,?)",
                            [tuple(r[c] for c in COLUMNS) for r in rows])
        con.execute(
            "COPY (SELECT transaction_id, account_id, CAST(transaction_ts AS TIMESTAMP) AS transaction_ts, "
            "transaction_type, CAST(amount AS DECIMAL(18,2)) AS amount, currency, status, "
            "CAST(source_updated_ts AS TIMESTAMP) AS source_updated_ts FROM stage) "
            f"TO '{p.as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    return p


def _totals(tmp_path, rows):
    return compute_kpis(_fixture(tmp_path, rows), SQL_DIR)["totals"]


def test_only_posted_rows_count(tmp_path):
    rows = [_row(amount="10.00"), _row(amount="5.00", status="PENDING"),
            _row(amount="7.00", status="FAILED")]
    (t,) = _totals(tmp_path, rows)
    assert (t["inflow"], t["outflow"], t["net"], t["txn_count"]) == (Decimal("10.00"), Decimal("0.00"), Decimal("10.00"), 1)


def test_credit_is_inflow_debit_is_outflow_net_is_difference(tmp_path):
    rows = [_row(amount="100.00"), _row(amount="30.25", transaction_type="DEBIT")]
    (t,) = _totals(tmp_path, rows)
    assert (t["inflow"], t["outflow"], t["net"]) == (Decimal("100.00"), Decimal("30.25"), Decimal("69.75"))


def test_one_sided_currency_shows_zero_not_null(tmp_path):
    rows = [_row(currency="USD", amount="20.00")]
    (t,) = _totals(tmp_path, rows)
    assert t["outflow"] == Decimal("0.00") and t["outflow"] is not None


def test_net_can_be_negative(tmp_path):
    rows = [_row(amount="5.00"), _row(amount="8.00", transaction_type="DEBIT")]
    assert _totals(tmp_path, rows)[0]["net"] == Decimal("-3.00")


def test_currencies_are_never_merged(tmp_path):
    rows = [_row(currency="INR", amount="100.00"), _row(currency="USD", amount="1.00")]
    got = {t["currency"]: t["net"] for t in _totals(tmp_path, rows)}
    assert got == {"INR": Decimal("100.00"), "USD": Decimal("1.00")}


def test_decimal_sums_are_exact(tmp_path):
    rows = [_row(amount="0.10"), _row(amount="0.20")]
    assert _totals(tmp_path, rows)[0]["inflow"] == Decimal("0.30")


@pytest.mark.parametrize("kpi", list(KPI_FILES))
def test_empty_input_gives_no_rows(tmp_path, kpi):
    assert compute_kpis(_fixture(tmp_path, []), SQL_DIR)[kpi] == []


def test_daily_groups_by_utc_day_boundary(tmp_path):
    rows = [_row(transaction_ts="2025-01-01T23:59:59", amount="1.00"),
            _row(transaction_ts="2025-01-02T00:00:00", amount="2.00")]
    daily = compute_kpis(_fixture(tmp_path, rows), SQL_DIR)["daily"]
    assert [(str(d["txn_date"]), d["inflow"]) for d in daily] == [
        ("2025-01-01", Decimal("1.00")), ("2025-01-02", Decimal("2.00"))]


def test_by_account_separates_accounts(tmp_path):
    rows = [_row(account_id="ACC-0001", amount="1.00"), _row(account_id="ACC-0002", amount="2.00"),
            _row(account_id="ACC-0002", amount="3.00", transaction_type="DEBIT")]
    got = {a["account_id"]: a["net"] for a in compute_kpis(_fixture(tmp_path, rows), SQL_DIR)["by_account"]}
    assert got == {"ACC-0001": Decimal("1.00"), "ACC-0002": Decimal("-1.00")}


def test_status_summary_counts_all_statuses(tmp_path):
    rows = [_row(), _row(), _row(status="PENDING"), _row(status="FAILED")]
    got = {s["status"]: s["txn_count"] for s in compute_kpis(_fixture(tmp_path, rows), SQL_DIR)["status_summary"]}
    assert got == {"POSTED": 2, "PENDING": 1, "FAILED": 1}


def test_write_kpis_creates_one_csv_per_kpi(tmp_path):
    res = compute_kpis(_fixture(tmp_path, [_row(amount="10.00")]), SQL_DIR)
    paths = write_kpis(res, tmp_path / "kpis")
    assert sorted(p.name for p in paths) == sorted(f"{k}.csv" for k in KPI_FILES)
    lines = (tmp_path / "kpis" / "totals.csv").read_text(encoding="utf-8").splitlines()
    assert lines == ["currency,inflow,outflow,net,txn_count", "INR,10.00,0.00,10.00,1"]


def test_missing_parquet_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="not found"):
        compute_kpis(tmp_path / "nope.parquet", SQL_DIR)


# ---- independent oracle on the generated seed-42 data ----------------------

def _is_valid(r):
    if not r["transaction_id"].strip() or not r["account_id"].strip():
        return False
    for col in ("transaction_ts", "source_updated_ts"):
        try:
            datetime.strptime(r[col], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return False
    return (r["transaction_type"] in {"CREDIT", "DEBIT"} and Decimal(r["amount"]) > 0
            and r["currency"] in {"INR", "USD"} and r["status"] in {"POSTED", "PENDING", "FAILED"})


def _latest(rows):
    best = {}
    for r in rows:
        if not _is_valid(r):
            continue
        key = (r["source_updated_ts"], r["status"], r["amount"], r["account_id"],
               r["transaction_ts"], r["transaction_type"], r["currency"])
        k = r["transaction_id"]
        if k not in best or key > best[k][0]:
            best[k] = (key, r)
    return [v[1] for v in best.values()]


def _oracle(rows, group):
    acc = defaultdict(lambda: [Decimal("0.00"), Decimal("0.00"), 0])
    for r in _latest(rows):
        if r["status"] != "POSTED":
            continue
        a = acc[group(r)]
        a[0 if r["transaction_type"] == "CREDIT" else 1] += Decimal(r["amount"])
        a[2] += 1
    return {k: (v[0], v[1], v[0] - v[1], v[2]) for k, v in acc.items()}


def _as_map(rows, key_fields):
    return {tuple(str(r[k]) for k in key_fields): (r["inflow"], r["outflow"], r["net"], r["txn_count"])
            for r in rows}


@pytest.fixture(scope="module")
def seed42(spark, tmp_path_factory):
    d = tmp_path_factory.mktemp("kpi42")
    rows = generate_rows(GeneratorConfig(num_rows=1000, seed=42))
    src = d / "in.csv"
    write_csv(rows, src)
    out = d / "output"
    run_pipeline(spark, src, out, d / "quarantine")
    return rows, out, compute_kpis(out / "curated" / "transactions.parquet", SQL_DIR)


def test_seed42_totals_match_oracle(seed42):
    rows, _, kpis = seed42
    assert _as_map(kpis["totals"], ["currency"]) == _oracle(rows, lambda r: (r["currency"],))
    assert {t["currency"] for t in kpis["totals"]} == {"INR", "USD"}


def test_seed42_daily_matches_oracle(seed42):
    rows, _, kpis = seed42
    expected = _oracle(rows, lambda r: (r["currency"], r["transaction_ts"][:10]))
    assert _as_map(kpis["daily"], ["currency", "txn_date"]) == expected


def test_seed42_by_account_matches_oracle(seed42):
    rows, _, kpis = seed42
    expected = _oracle(rows, lambda r: (r["currency"], r["account_id"]))
    assert _as_map(kpis["by_account"], ["currency", "account_id"]) == expected


def test_seed42_status_summary_covers_all_870_curated_rows(seed42):
    assert sum(s["txn_count"] for s in seed42[2]["status_summary"]) == 870


def test_adding_superseded_rows_would_give_wrong_totals(seed42):
    _, out, kpis = seed42
    con = duckdb.connect()
    try:
        q = ("SELECT currency, SUM(amount) FILTER (WHERE transaction_type='CREDIT') FROM ("
             f"SELECT * FROM read_parquet('{(out / 'curated' / 'transactions.parquet').as_posix()}') UNION ALL "
             f"SELECT * FROM read_parquet('{(out / 'superseded' / 'superseded.parquet').as_posix()}')) "
             "WHERE status='POSTED' GROUP BY currency ORDER BY currency")
        wrong = {c: v for c, v in con.execute(q).fetchall()}
    finally:
        con.close()
    right = {t["currency"]: t["inflow"] for t in kpis["totals"]}
    assert wrong != right


def test_posted_version_later_flipped_to_failed_is_not_counted(spark, tmp_path):
    rows = [
        _row(transaction_id="TXN-900001", status="POSTED", source_updated_ts="2025-01-01T10:00:00", amount="50.00"),
        _row(transaction_id="TXN-900001", status="FAILED", source_updated_ts="2025-01-01T11:00:00", amount="50.00"),
        _row(transaction_id="TXN-900002", status="POSTED", amount="7.00"),
    ]
    src = tmp_path / "in.csv"
    write_csv(rows, src)
    run_pipeline(spark, src, tmp_path / "out", tmp_path / "q")
    totals = compute_kpis(tmp_path / "out" / "curated" / "transactions.parquet", SQL_DIR)["totals"]
    assert [(t["currency"], t["inflow"], t["txn_count"]) for t in totals] == [("INR", Decimal("7.00"), 1)]
