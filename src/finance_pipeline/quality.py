"""Data-quality checks that re-read the PUBLISHED files, not the Spark DataFrames."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import duckdb

from finance_pipeline.errors import QualityCheckError
from finance_pipeline.schema import COLUMNS
from finance_pipeline.kpis import DEFAULT_SQL_DIR, compute_kpis
from finance_pipeline.validate import VALID_CURRENCIES, VALID_STATUSES, VALID_TYPES

# Must stay equal to the rule codes in validate._rules (a test enforces this).
KNOWN_REASON_CODES = frozenset({
    "MISSING_TRANSACTION_ID", "MISSING_ACCOUNT_ID",
    "MISSING_TRANSACTION_TS", "MALFORMED_TRANSACTION_TS",
    "INVALID_TRANSACTION_TYPE",
    "MISSING_AMOUNT", "MALFORMED_AMOUNT", "NON_POSITIVE_AMOUNT",
    "UNSUPPORTED_CURRENCY", "INVALID_STATUS",
    "MISSING_SOURCE_UPDATED_TS", "MALFORMED_SOURCE_UPDATED_TS",
})


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def _lit(values) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in sorted(values))


def _sql_path(p: Path) -> str:
    return p.as_posix().replace("'", "''")


def _scalar(con, sql: str):
    return con.execute(sql).fetchone()[0]


class _Ctx:
    """Open DuckDB connection with views over the published files."""

    def __init__(self, con, manifest: dict, parquet: Path, sql_dir):
        self.con, self.manifest, self.parquet, self.sql_dir = con, manifest, parquet, sql_dir
        self._kpis: dict | None = None

    def kpis(self) -> dict:
        if self._kpis is None:
            self._kpis = compute_kpis(self.parquet, self.sql_dir)
        return self._kpis


def _count_check(sql: str, what: str):
    def check(c: _Ctx):
        n = _scalar(c.con, sql)
        return n == 0, "ok" if n == 0 else f"{n} {what}"
    return check


def _manifest_matches_files(c: _Ctx):
    actual = {
        "curated_rows": _scalar(c.con, "SELECT count(*) FROM curated"),
        "superseded_rows": _scalar(c.con, "SELECT count(*) FROM superseded"),
        "quarantined_rows": _scalar(c.con, "SELECT count(*) FROM quarantine"),
    }
    diffs = {k: (c.manifest.get(k), v) for k, v in actual.items() if c.manifest.get(k) != v}
    if diffs:
        return False, f"manifest vs files (manifest, file): {diffs}"
    return True, ", ".join(f"{k}={v}" for k, v in actual.items())


def _reconciliation(c: _Ctx):
    total = sum(_scalar(c.con, f"SELECT count(*) FROM {t}") for t in ("curated", "superseded", "quarantine"))
    expected = c.manifest.get("input_rows")
    return total == expected, f"files hold {total} rows, manifest input_rows={expected}"


def _quarantine_reasons_known(c: _Ctx):
    empty = _scalar(c.con, "SELECT count(*) FROM quarantine "
                           "WHERE quarantine_reasons IS NULL OR trim(quarantine_reasons) = ''")
    unknown = _scalar(
        c.con,
        "SELECT count(*) FROM (SELECT unnest(string_split(quarantine_reasons, ';')) AS code "
        f"FROM quarantine) WHERE code NOT IN ({_lit(KNOWN_REASON_CODES)})",
    )
    if empty or unknown:
        return False, f"{empty} row(s) without a reason, {unknown} unknown reason code(s)"
    return True, "ok"


def _kpi_totals_agree(c: _Ctx):
    totals = c.kpis()["totals"]
    problems = []
    posted = _scalar(c.con, "SELECT count(*) FROM curated WHERE status = 'POSTED'")
    if sum(t["txn_count"] for t in totals) != posted:
        problems.append(f"txn_count sums to {sum(t['txn_count'] for t in totals)}, curated has {posted} POSTED rows")
    bad_net = [t["currency"] for t in totals if t["net"] != t["inflow"] - t["outflow"]]
    if bad_net:
        problems.append(f"net != inflow - outflow for {bad_net}")
    direct = {
        cur: (i, o) for cur, i, o in c.con.execute(
            "SELECT currency, "
            "COALESCE(SUM(amount) FILTER (WHERE transaction_type = 'CREDIT'), 0), "
            "COALESCE(SUM(amount) FILTER (WHERE transaction_type = 'DEBIT'), 0) "
            "FROM curated WHERE status = 'POSTED' GROUP BY currency"
        ).fetchall()
    }
    if {t["currency"]: (t["inflow"], t["outflow"]) for t in totals} != direct:
        problems.append("per-currency inflow/outflow differ from a direct query on the curated file")
    return not problems, "; ".join(problems) or "ok"


def _kpi_status_sums_to_curated(c: _Ctx):
    s = sum(r["txn_count"] for r in c.kpis()["status_summary"])
    n = _scalar(c.con, "SELECT count(*) FROM curated")
    return s == n, f"status summary adds to {s}, curated has {n} rows"


_NO_NULLS = " OR ".join(f'"{col}" IS NULL' for col in COLUMNS)
_DOMAIN = (
    f"NOT coalesce(transaction_type IN ({_lit(VALID_TYPES)}), false) "
    f"OR NOT coalesce(currency IN ({_lit(VALID_CURRENCIES)}), false) "
    f"OR NOT coalesce(status IN ({_lit(VALID_STATUSES)}), false)"
)

CHECKS = (
    ("manifest_matches_files", _manifest_matches_files),
    ("reconciliation", _reconciliation),
    ("curated_ids_unique", _count_check(
        "SELECT count(*) - count(DISTINCT transaction_id) FROM curated", "duplicate transaction_id(s)")),
    ("curated_no_nulls", _count_check(
        f"SELECT count(*) FROM curated WHERE {_NO_NULLS}", "row(s) with a null")),
    ("curated_domain_values", _count_check(
        f"SELECT count(*) FROM curated WHERE {_DOMAIN}", "row(s) with a type/currency/status outside the allowed set")),
    ("curated_amount_positive", _count_check(
        "SELECT count(*) FROM curated WHERE amount <= 0", "row(s) with amount <= 0")),
    ("curated_ts_order", _count_check(
        "SELECT count(*) FROM curated WHERE source_updated_ts < transaction_ts",
        "row(s) updated before they occurred")),
    ("superseded_ids_in_curated", _count_check(
        "SELECT count(*) FROM superseded WHERE transaction_id NOT IN (SELECT transaction_id FROM curated)",
        "superseded row(s) without a curated winner")),
    ("no_superseded_newer", _count_check(
        "SELECT count(*) FROM superseded s JOIN curated c USING (transaction_id) "
        "WHERE s.source_updated_ts > c.source_updated_ts",
        "superseded row(s) newer than their winner")),
    ("quarantine_reasons_known", _quarantine_reasons_known),
    ("kpi_totals_agree", _kpi_totals_agree),
    ("kpi_status_sums_to_curated", _kpi_status_sums_to_curated),
)


def run_quality_checks(
    output_dir="data/output", quarantine_dir="data/quarantine", sql_dir=DEFAULT_SQL_DIR
) -> list[CheckResult]:
    """Run every check against the published files; never stops at the first failure."""
    out, q = Path(output_dir), Path(quarantine_dir)
    curated = out / "curated" / "transactions.parquet"
    superseded = out / "superseded" / "superseded.parquet"
    manifest_path = out / "manifest.json"
    quarantine = q / "quarantine.csv"

    missing = [str(p) for p in (curated, superseded, manifest_path, quarantine) if not p.is_file()]
    if missing:
        return [CheckResult("files_present", False, f"missing: {missing}")]
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except ValueError as exc:
        return [CheckResult("files_present", False, f"manifest.json is not valid JSON: {exc}")]

    con = duckdb.connect()
    try:
        try:
            con.execute(f"CREATE VIEW curated AS SELECT * FROM read_parquet('{_sql_path(curated)}')")
            con.execute(f"CREATE VIEW superseded AS SELECT * FROM read_parquet('{_sql_path(superseded)}')")
            con.execute("CREATE VIEW quarantine AS SELECT * FROM read_csv("
                        f"'{_sql_path(quarantine)}', header = true, all_varchar = true)")
        except Exception as exc:
            return [CheckResult("files_present", False, f"cannot read outputs: {exc}")]

        ctx = _Ctx(con, manifest, curated, sql_dir)
        results = [CheckResult("files_present", True, "all four files found and readable")]
        for name, fn in CHECKS:
            try:
                passed, detail = fn(ctx)
            except Exception as exc:  # a crashing check is a failed check
                passed, detail = False, f"check raised {type(exc).__name__}: {exc}"
            results.append(CheckResult(name, bool(passed), detail))
        return results
    finally:
        con.close()


def format_report(results: list[CheckResult]) -> str:
    lines = [f"[{'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}" for r in results]
    lines.append(f"{sum(r.passed for r in results)}/{len(results)} checks passed")
    return "\n".join(lines)


def assert_all_passed(results: list[CheckResult]) -> None:
    failed = [r.name for r in results if not r.passed]
    if failed:
        raise QualityCheckError(f"{len(failed)} data-quality check(s) failed: {', '.join(failed)}")
