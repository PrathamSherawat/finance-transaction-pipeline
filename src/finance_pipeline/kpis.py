"""Run the KPI SQL files against the curated Parquet file with DuckDB."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb

KPI_FILES = {
    "totals": "kpi_totals.sql",
    "daily": "kpi_daily.sql",
    "by_account": "kpi_by_account.sql",
    "status_summary": "kpi_status_summary.sql",
}
DEFAULT_PARQUET = "data/output/curated/transactions.parquet"
DEFAULT_SQL_DIR = "sql"
DEFAULT_OUT_DIR = "data/output/kpis"


def compute_kpis(
    parquet_path: str | Path = DEFAULT_PARQUET, sql_dir: str | Path = DEFAULT_SQL_DIR
) -> dict[str, list[dict]]:
    """Return {kpi name: list of row dicts}. Each SQL file reads the view `curated`."""
    pq = Path(parquet_path)
    if not pq.is_file():
        raise FileNotFoundError(f"curated file not found: {pq}")
    con = duckdb.connect()
    try:
        quoted = pq.as_posix().replace("'", "''")
        con.execute(f"CREATE VIEW curated AS SELECT * FROM read_parquet('{quoted}')")
        results: dict[str, list[dict]] = {}
        for name, filename in KPI_FILES.items():
            sql = (Path(sql_dir) / filename).read_text(encoding="utf-8-sig")
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description]
            results[name] = [dict(zip(cols, row)) for row in cur.fetchall()]
        return results
    finally:
        con.close()


def write_kpis(results: dict[str, list[dict]], out_dir: str | Path = DEFAULT_OUT_DIR) -> list[Path]:
    """Write one CSV per KPI (header always present). Overwrites existing files."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for name, rows in results.items():
        path = out / f"{name}.csv"
        # header comes from the SQL; for empty results fall back to no header row
        fields = list(rows[0].keys()) if rows else []
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, lineterminator="\n")
            if fields:
                writer.writerow(fields)
                writer.writerows([[str(r[c]) for c in fields] for r in rows])
        written.append(path)
    return written


def main() -> int:
    results = compute_kpis()
    write_kpis(results)
    print(json.dumps({k: [{c: str(v) for c, v in r.items()} for r in rows]
                      for k, rows in results.items() if k == "totals"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
