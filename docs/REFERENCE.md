# Project reference

Every file in the project and every function in the code. The function lists, signatures, imports
and test counts below are generated from the source by `scripts/gen_reference.py`; run
`python scripts/gen_reference.py` again after changing code. See `ARCHITECTURE.md` for diagrams.

## 1. Every file in the project

### Project root

| File | Purpose |
|---|---|
| `README.md` | Quick start, business rules, outputs, design decisions, limitations, measured scale results |
| `README.old.md` | Backup of the original README (only exists if the README was overwritten with the backup step) |
| `requirements.txt` | Runtime dependencies: pyspark, duckdb, pytest |
| `requirements-dev.txt` | Dev-only tools: ruff, mypy |
| `pyproject.toml` | Package definition (src layout), console scripts, pytest markers, ruff and mypy settings |
| `.gitignore`, `.gitattributes` | Git housekeeping, if present. `.gitignore` excludes `data/output/` and `data/quarantine/`; the generated folders `data/incoming/` and `data/state/` are not excluded, so consider adding them |

### Configuration, SQL, scripts, CI

| File | Purpose |
|---|---|
| `config/pipeline.toml` | Default settings (paths, SQL dir, log level); a test asserts it equals the built-in defaults |
| `sql/kpi_totals.sql` | POSTED-only inflow, outflow, net, count per currency |
| `sql/kpi_daily.sql` | Same measures per currency per UTC day of `transaction_ts` |
| `sql/kpi_by_account.sql` | Same measures per currency per account |
| `sql/kpi_status_summary.sql` | Row counts per status over all curated rows (sanity check, not a financial KPI) |
| `scripts/demo.ps1` | Deletes generated data, generates 3 batches, runs the incremental pipeline, prints the totals |
| `scripts/scale_test.py` | One-off per-stage benchmark on N generated rows |
| `scripts/gen_reference.py` | Generates this file |
| `orchestration/finance_pipeline_dag.py` | Sample Airflow DAG (not run, Airflow not a dependency) |
| `.github/workflows/ci.yml` | Lint always; fast tests on push; full suite with Java 17 on pull requests and nightly |
| `.github/workflows/scheduled-run.yml` | Daily demo run that asserts the seed-42 manifest and uploads artifacts |
| `docs/ARCHITECTURE.md` | Diagrams and data flow |
| `docs/REFERENCE.md` | This file |
| `docs/INTERVIEW.md` | Pitch and one answer per design decision |

### Data folders (created by running the code)

| Path | Contents |
|---|---|
| `data/sample/transactions.csv` | Generated sample: seed 42, 1000 rows (870 valid, 80 duplicate versions, 50 invalid) |
| `data/incoming/` | Batch CSV files waiting to be processed |
| `data/state/` | `ledger.json`, `run.lock` (during a run), `last_run.json`, `snapshots/snap_000N/` |
| `data/output/` | `curated/transactions.parquet`, `superseded/superseded.parquet`, `manifest.json`, `kpis/*.csv` |
| `data/quarantine/` | `quarantine.csv` with reason codes |

### Source modules (`src/finance_pipeline/`)

Listed in detail in section 2. `__init__.py` marks the package.

### Tests (`tests/`)

Listed in section 4.

## 2. Source modules and functions

Private helpers start with an underscore. Signatures are shown as written in the code.

### `src/finance_pipeline/batches.py`

Splits generated rows into several batch files for incremental-processing demos and tests.

- **Imports from the project:** `generate_data`
- **Used by:** `orchestrate`

- `split_into_batches(rows: list[dict], n: int) -> list[list[dict]]`
  Split rows into n contiguous chunks (sizes differ by at most 1), keeping order. The generator shuffles rows, so older versions of a transaction naturally land in later batches than newer ones: late arrivals.
- `write_batches(rows: list[dict], n: int, output_dir, prefix: str='transactions') -> list[Path]`
  Write each chunk as <prefix>_NNNN.csv in the output directory and return the paths.
- `main(argv: list[str] | None=None) -> int`
  CLI: generate rows and write them as N batch files.

### `src/finance_pipeline/cleanroom.py`

Runs the whole pipeline from nothing in an empty folder and checks the known seed-42 numbers.

- **Imports from the project:** `ingest`, `kpis`, `orchestrate`
- **Constants:** `EXPECTED_SEED_42`, `BATCHES`

- `_read_json(path: Path) -> dict`
  Read a JSON file, or return an empty dict if it does not exist.
- `run_cleanroom(spark: SparkSession, workdir, expected: dict | None=None, sql_dir=DEFAULT_SQL_DIR) -> dict`
  Generate batches, run the pipeline, run it again, and check the results. Everything happens inside `workdir`, which must be empty or absent.
- `main(argv: list[str] | None=None) -> int`
  Exit codes: 0 all checks passed, 1 a check failed, 2 bad arguments.

### `src/finance_pipeline/config.py`

Settings: CLI flag > FIN_<NAME> environment variable > config/pipeline.toml > defaults.

- **Imports from the project:** none
- **Used by:** `incremental`, `orchestrate`, `pipeline`
- **Constants:** `DEFAULT_CONFIG_PATH`, `ENV_PREFIX`

- **class `Settings`**
  Frozen dataclass of string settings: input_csv, incoming_dir, state_dir, output_dir, quarantine_dir, sql_dir, log_level.
- `load_settings(path=DEFAULT_CONFIG_PATH, env=None) -> Settings`
  Read the TOML file if present (missing file is fine), then apply env overrides. CLI flags win over all of this because they are argparse values whose defaults come from here.

### `src/finance_pipeline/dedup.py`

Keeps the latest version of each transaction_id with a deterministic total order; counts tie conflicts.

- **Imports from the project:** `schema`, `validate`
- **Used by:** `incremental`, `publish`
- **Constants:** `KEY`, `TIE_BREAK_COLUMNS`, `_RANK`, `_TS`

- `_require_columns(df: DataFrame) -> None`
  Raise ValueError if the input frame lacks any contract column.
- `dedup_transactions(valid_df: DataFrame) -> tuple[DataFrame, DataFrame]`
  Return (latest, superseded) from a validated all-string frame. latest has exactly one row per transaction_id. superseded holds every other row, so latest + superseded always equals the input.
- `count_tie_conflicts(valid_df: DataFrame) -> int`
  Count transaction_ids whose newest timestamp is shared by rows with different content.

### `src/finance_pipeline/errors.py`

Exception hierarchy (PipelineError and subclasses) and the one mapping from exceptions to CLI exit codes.

- **Imports from the project:** none
- **Used by:** `incremental`, `ledger`, `lock`, `orchestrate`, `pipeline`, `quality`

- **class `PipelineError`**
  Base class for expected pipeline failures.
- **class `QualityCheckError`**
  Raised when at least one data-quality check failed.
- **class `BatchConflictError`**
  A file name is already in the ledger but its contents changed.
- **class `LockError`**
  Another run holds the lock file.
- `exit_code_for(exc: BaseException) -> int`
  0 ok (never here), 1 quality check failed, 2 bad input, 3 batch conflict, 4 locked.

### `src/finance_pipeline/generate_data.py`

Seeded synthetic transaction generator: valid rows, duplicate versions, ties, deliberately bad rows, CSV writer, CLI.

- **Imports from the project:** `schema`
- **Used by:** `batches`
- **Constants:** `TYPE_CHOICES`, `CURRENCY_CHOICES`, `STATUS_CHOICES`, `MIN_CENTS`, `MAX_CENTS`, `MAX_UPDATE_DELAY_SECONDS`, `MUTABLE_AMOUNT_CHANGE_PROBABILITY`, `MAX_VERSION_DELAY_SECONDS`, `ALL_STATUSES`, `DEFECTS`, `BAD_TIMESTAMPS`, `BAD_CURRENCIES`, `BAD_STATUSES`, `EXTRA_DEFECTS`, `ALL_DEFECTS`, `BAD_AMOUNTS`, `DEFAULT_OUTPUT`

- `_require_int(name: str, value: object, minimum: int | None=None) -> None`
  Validate an integer setting (rejects bool, which is a subclass of int) with an optional minimum; raises ValueError naming the field.
- `_require_rate(name: str, value: object) -> None`
  Validate a rate in [0, 1); rejects bool and non-numbers.
- **class `GeneratorConfig`**
  Frozen dataclass holding every generator setting (rows, seed, rates, start date, window, tie rate, extra_defects). Invalid values raise ValueError at construction.
  - `GeneratorConfig.__post_init__(self) -> None`
    Runs every validation, including invalid_rate + 2 * duplicate_rate < 1 so each duplicate has its own valid base row.
- **class `RowCounts`**
  Named tuple (valid, invalid, duplicate): the mutually exclusive split of num_rows.
- `compute_row_counts(config: GeneratorConfig) -> RowCounts`
  Split config.num_rows into mutually exclusive categories. invalid and duplicate are floored (epsilon guards float error such as 0.29 * 100 == 28.999...). valid is the remainder, so the three counts always sum to num_rows and valid >= 1 (guaranteed by the config's rate-sum check).
- `format_amount(cents: int) -> str`
  Format integer cents as a string with exactly two decimals.
- `format_ts(dt: datetime) -> str`
  Format a naive datetime as YYYY-MM-DDTHH:MM:SS (treated as UTC).
- `make_account_pool(rng: random.Random, size: int) -> list[str]`
  Return `size` synthetic account IDs: ACC-0001, ACC-0002, ...
- `make_valid_transaction(rng: random.Random, index: int, accounts: list[str], config: GeneratorConfig) -> dict[str, str]`
  Build one clean transaction. All values are strings, keys follow COLUMNS.
- `make_duplicate_version(rng: random.Random, original: dict[str, str]) -> dict[str, str]`
  Return a strictly later version of `original`; does not mutate it.
- `apply_defect(rng: random.Random, record: dict[str, str], defect: str) -> dict[str, str]`
  Return a copy of `record` with exactly one field corrupted.
- `make_tie_version(rng: random.Random, original: dict[str, str], conflict: bool) -> dict[str, str]`
  Return a version with the SAME source_updated_ts as `original`. conflict=False: an identical re-delivery. conflict=True: same timestamp but a different status (an ambiguous source).
- `generate_rows(config: GeneratorConfig) -> list[dict[str, str]]`
  Generate exactly config.num_rows rows: valid + duplicate versions + invalid.
- `write_csv(rows: list[dict[str, str]], path: str | Path) -> int`
  Write rows to `path` with the exact COLUMNS schema; return rows written.
- `build_parser() -> argparse.ArgumentParser`
  Builds the argparse parser for the generator CLI; defaults come from GeneratorConfig().
- `main(argv: list[str] | None=None) -> int`
  Generator CLI: validates the config, writes the CSV, prints a summary; returns 2 on an invalid configuration.

### `src/finance_pipeline/incremental.py`

Incremental processing: Spark per batch, DuckDB merge, snapshots, ledger as commit point, export, checks, KPIs.

- **Imports from the project:** `config`, `dedup`, `errors`, `ingest`, `kpis`, `ledger`, `lock`, `logging_setup`, `publish`, `quality`, `schema`, `transform`, `validate`
- **Used by:** `orchestrate`
- **Constants:** `QUARANTINE_WITH_BATCH`, `TYPED_DDL`, `_ORDER`, `RANK_SQL`

- `_q(path) -> str`
  Render a path as a DuckDB-safe quoted string body.
- `_load_stage(con, rows, columns) -> None`
  Create a VARCHAR staging table in DuckDB and bulk-insert rows.
- `_count(con, table: str) -> int`
  Row count of a DuckDB table.
- `_process_with_spark(spark: SparkSession, csv_path: Path) -> dict`
  Spark does what it did before: read, validate, dedup inside the batch, type.
- `merge_into_snapshot(prev_dir, new_dir: Path, batch_id: str, batch: dict, prev_manifest: dict) -> dict`
  Merge one processed batch into the previous snapshot; write the result to new_dir. Winner per transaction_id = best of (old curated + batch winners). Every loser goes to superseded, so the multiset of rows equals a one-shot full load.
- `export_snapshot(snapshot_dir: Path, output_dir, quarantine_dir) -> None`
  Copy the current snapshot to the published layout. Idempotent, so a crash here is repaired by the next run.
- `_run_incremental_locked(spark: SparkSession, incoming_dir, state_dir='data/state', output_dir='data/output', quarantine_dir='data/quarantine', kpi_dir=None, sql_dir=DEFAULT_SQL_DIR) -> dict`
  Process every NEW batch file in name order, export, verify, write KPIs.
- `run_incremental(spark: SparkSession, incoming_dir, state_dir='data/state', output_dir='data/output', quarantine_dir='data/quarantine', kpi_dir=None, sql_dir=DEFAULT_SQL_DIR) -> dict`
  Same as before, but only one run at a time may use a given state directory.
- `_parser() -> argparse.ArgumentParser`
  Argparse parser for the incremental CLI; defaults come from the settings.
- `main(argv: list[str] | None=None, spark: SparkSession | None=None) -> int`
  Exit codes: 0 ok, 1 quality check failed, 2 bad input, 3 batch file changed.

### `src/finance_pipeline/ingest.py`

Spark session factory and the CSV reader: explicit all-string schema, header check, FAILFAST.

- **Imports from the project:** `schema`
- **Used by:** `cleanroom`, `incremental`, `orchestrate`, `pipeline`, `publish`
- **Constants:** `RAW_SCHEMA`

- `get_spark(app_name: str='finance-pipeline') -> SparkSession`
  Return a small local SparkSession tuned for tests and sample-sized data.
- `check_header(path: str | Path) -> None`
  Raise if the CSV header is not exactly COLUMNS, in order. Spark assigns columns by position when given an explicit schema, so a wrong header would silently mislabel data.
- `read_raw_transactions(spark: SparkSession, path: str | Path) -> DataFrame`
  Read the CSV with the explicit all-string schema, failing fast on bad structure.

### `src/finance_pipeline/kpis.py`

Runs the SQL files in sql/ against the curated Parquet file and writes one CSV per KPI.

- **Imports from the project:** none
- **Used by:** `cleanroom`, `incremental`, `pipeline`, `quality`
- **Constants:** `KPI_FILES`, `DEFAULT_PARQUET`, `DEFAULT_SQL_DIR`, `DEFAULT_OUT_DIR`

- `compute_kpis(parquet_path: str | Path=DEFAULT_PARQUET, sql_dir: str | Path=DEFAULT_SQL_DIR) -> dict[str, list[dict]]`
  Return {kpi name: list of row dicts}. Each SQL file reads the view `curated`.
- `write_kpis(results: dict[str, list[dict]], out_dir: str | Path=DEFAULT_OUT_DIR) -> list[Path]`
  Write one CSV per KPI (header always present). Overwrites existing files.
- `main() -> int`
  CLI: computes KPIs from the curated file, writes the CSVs and prints the totals.

### `src/finance_pipeline/ledger.py`

Ledger of processed batch files (SHA-256) and which snapshot is current; atomic save.

- **Imports from the project:** `errors`
- **Used by:** `incremental`
- **Constants:** `LEDGER_NAME`

- `file_sha256(path) -> str`
  (no description)
- `empty_ledger() -> dict`
  A fresh ledger: version 1, no current snapshot, no batches.
- `load_ledger(state_dir) -> dict`
  Read ledger.json (or return an empty ledger if absent); reject files that are not a version-1 ledger.
- `save_ledger(state_dir, ledger: dict) -> None`
  Atomic write: temp file, then os.replace. Either the old or the new ledger exists.
- `classify_batch(ledger: dict, name: str, sha256: str) -> str`
  Return 'new' or 'skip'; raise BatchConflictError if the name was seen with other content.
- `record_batch(ledger: dict, *, file: str, sha256: str, input_rows: int, snapshot: str) -> dict`
  Return a NEW ledger with the batch appended; the input is not mutated.

### `src/finance_pipeline/lock.py`

Single-writer lock file (atomic exclusive create) so two incremental runs cannot overlap.

- **Imports from the project:** `errors`
- **Used by:** `incremental`

- **class `RunLock`**
  Context manager that atomically creates the lock file and removes it on exit; a lock older than max_age_seconds counts as stale.
  - `RunLock.__init__(self, path, max_age_seconds: float=6 * 3600)`
    Store the lock path and the staleness limit (default 6 hours).
  - `RunLock._is_stale(self) -> bool`
    True if the lock file is older than max_age_seconds (or vanished).
  - `RunLock.__enter__(self) -> 'RunLock'`
    Atomically create the lock file (O_EXCL); replace one stale lock; otherwise raise LockError.
  - `RunLock.__exit__(self, exc_type, exc, tb) -> None`
    Remove the lock file if this instance created it.

### `src/finance_pipeline/logging_setup.py`

Logging to stderr only, so stdout stays reserved for the JSON summary line.

- **Imports from the project:** none
- **Used by:** `incremental`, `orchestrate`, `pipeline`
- **Constants:** `_FORMAT`

- **class `_StderrHandler`**
  Logging handler that looks up sys.stderr at emit time, so a stream swapped by a test never goes stale.
  - `_StderrHandler.emit(self, record: logging.LogRecord) -> None`
    Format the record and write it to the current sys.stderr.
- `setup_logging(level: str='INFO') -> None`
  Configure the 'finance_pipeline' logger. Safe to call repeatedly.

### `src/finance_pipeline/orchestrate.py`

Plain-Python task runner: ordered tasks, retries with backoff for retryable errors, per-task status, run log.

- **Imports from the project:** `batches`, `config`, `errors`, `incremental`, `ingest`, `logging_setup`
- **Used by:** `cleanroom`
- **Constants:** `RETRYABLE`

- **class `TaskResult`**
  Dataclass describing one task outcome: status (ok/failed/skipped), attempts, seconds, error, exit code.
- `run_task(name: str, fn: Callable[[], object], retries: int=2, backoff: float=2.0, retry_on: tuple=RETRYABLE, sleep: Callable[[float], None]=time.sleep) -> TaskResult`
  Run fn; retry only `retry_on` errors, waiting backoff * 2**(attempt-1) seconds between tries.
- `run_tasks(tasks: list[tuple[str, Callable[[], object]]], retries: int=2, backoff: float=2.0, sleep: Callable[[float], None]=time.sleep) -> list[TaskResult]`
  Run tasks in order. After the first failure the rest are skipped, not run.
- `overall_exit_code(results: list[TaskResult]) -> int`
  Exit code of the first failed task, else 0.
- `write_run_log(path, results: list[TaskResult], started: str) -> dict`
  (no description)
- `_parser() -> argparse.ArgumentParser`
  Argparse parser for the orchestrator CLI.
- `main(argv: list[str] | None=None, spark: SparkSession | None=None, sleep: Callable[[float], None]=time.sleep) -> int`
  Exit code is the first failed task's code: 0 ok, 1 quality, 2 bad input, 3 conflict, 4 locked.

### `src/finance_pipeline/pipeline.py`

One-shot orchestration: publish, quality checks, KPIs. CLI with exit codes 0/1/2.

- **Imports from the project:** `config`, `errors`, `ingest`, `kpis`, `logging_setup`, `publish`, `quality`

- `run_all(spark: SparkSession, input_csv, output_dir='data/output', quarantine_dir='data/quarantine', kpi_dir=None, sql_dir=DEFAULT_SQL_DIR) -> dict`
  Publish, verify the published files, then write KPIs. The report is printed BEFORE any failure is raised, and KPI files are only written when every check passed.
- `_parser() -> argparse.ArgumentParser`
  (no description)
- `main(argv: list[str] | None=None, spark: SparkSession | None=None) -> int`
  Exit codes: 0 ok, 1 a data-quality check failed, 2 bad input.

### `src/finance_pipeline/publish.py`

Reconcile-before-write publishing with DuckDB writers, the manifest, and the one-shot run_pipeline.

- **Imports from the project:** `dedup`, `ingest`, `schema`, `transform`, `validate`
- **Used by:** `incremental`, `pipeline`
- **Constants:** `QUARANTINE_COLUMNS`, `TYPED_SELECT`, `PLAIN_SELECT`

- `_typed_rows(df: DataFrame) -> list[tuple]`
  Collect a typed frame into tuples of strings (timestamps formatted, decimals as text) so DuckDB re-casts them without Python conversions.
- `_string_rows(df: DataFrame, columns: list[str]) -> list[tuple]`
  Collect selected columns of an all-string frame into tuples.
- `_write(rows: list[tuple], columns: list[str], select_sql: str, path: Path, fmt: str) -> None`
  Load rows into a DuckDB staging table and COPY them to a Parquet or CSV file, ordered deterministically.
- `publish(curated: DataFrame, quarantine: DataFrame, superseded: DataFrame, input_rows: int, output_dir: str | Path, quarantine_dir: str | Path) -> dict`
  Write all three outputs and a manifest; overwrite on every run. Reconciliation is checked BEFORE anything is written, so a mismatch leaves no half-published output.
- `run_pipeline(spark: SparkSession, input_csv: str | Path, output_dir: str | Path='data/output', quarantine_dir: str | Path='data/quarantine') -> dict`
  raw CSV -> validate -> dedup -> type -> publish. Returns the manifest.
- `main() -> int`
  CLI: runs run_pipeline on the sample CSV and prints the manifest.

### `src/finance_pipeline/quality.py`

13 data-quality checks that re-read the PUBLISHED files and cross-check the manifest and KPIs.

- **Imports from the project:** `errors`, `kpis`, `schema`, `validate`
- **Used by:** `incremental`, `pipeline`
- **Constants:** `KNOWN_REASON_CODES`, `_NO_NULLS`, `_DOMAIN`, `CHECKS`

- **class `CheckResult`**
  Frozen dataclass (name, passed, detail) returned by every check.
- `_lit(values) -> str`
  Render a set of values as a sorted SQL string-literal list.
- `_sql_path(p: Path) -> str`
  Render a path as a DuckDB-safe quoted string body.
- `_scalar(con, sql: str)`
  Run a query and return its single value.
- **class `_Ctx`**
  Holds the open DuckDB connection with views over the published files, the manifest, and lazily computed KPIs.
  - `_Ctx.__init__(self, con, manifest: dict, parquet: Path, sql_dir)`
    Store the connection, manifest, curated path and SQL directory.
  - `_Ctx.kpis(self) -> dict`
    Compute the KPIs once and cache them for the KPI checks.
- `_count_check(sql: str, what: str)`
  Build a check from a COUNT query that must return 0; the detail names what was found.
- `_manifest_matches_files(c: _Ctx)`
  Compare manifest row counts with the real row counts of the three output files.
- `_reconciliation(c: _Ctx)`
  Check that the rows in all three files add up to the manifest's input_rows.
- `_quarantine_reasons_known(c: _Ctx)`
  Every quarantine row must have a reason, and every code must be in KNOWN_REASON_CODES.
- `_kpi_totals_agree(c: _Ctx)`
  KPI totals must agree with a direct query on the curated file (counts, net = inflow - outflow, per-currency sums).
- `_kpi_status_sums_to_curated(c: _Ctx)`
  The status summary must add up to the number of curated rows.
- `run_quality_checks(output_dir='data/output', quarantine_dir='data/quarantine', sql_dir=DEFAULT_SQL_DIR) -> list[CheckResult]`
  Run every check against the published files; never stops at the first failure.
- `format_report(results: list[CheckResult]) -> str`
  (no description)
- `assert_all_passed(results: list[CheckResult]) -> None`
  (no description)

### `src/finance_pipeline/schema.py`

The data contract: column order, timestamp format, allowed types, currencies and statuses. Single source of truth.

- **Imports from the project:** none
- **Used by:** `dedup`, `generate_data`, `incremental`, `ingest`, `publish`, `quality`, `transform`, `validate`
- **Constants:** `COLUMNS`, `TS_FORMAT`, `VALID_TYPES`, `VALID_CURRENCIES`, `VALID_STATUSES`

No functions: constants only.

### `src/finance_pipeline/transform.py`

Casts validated, deduplicated rows to real types (decimal, timestamp) with a no-silent-null safeguard.

- **Imports from the project:** `schema`, `validate`
- **Used by:** `incremental`, `publish`
- **Constants:** `AMOUNT_TYPE`, `TS_COLUMNS`

- `_require_columns(df: DataFrame, needed) -> None`
  Raise ValueError if the input frame lacks any of the given columns.
- `cast_to_curated(df: DataFrame) -> DataFrame`
  Cast amount to decimal(18,2) and the two timestamps to timestamp. Safeguard: validation guarantees every cast succeeds. If any cast would give null anyway, raise instead of publishing silent nulls.
- `flatten_quarantine(df: DataFrame) -> DataFrame`
  Turn the reasons array into a ';'-joined string so it fits in a CSV cell.

### `src/finance_pipeline/validate.py`

Per-row rules producing a list of failed-rule codes; splits rows into valid and quarantine.

- **Imports from the project:** `schema`
- **Used by:** `dedup`, `incremental`, `publish`, `quality`, `transform`
- **Constants:** `REASONS_COL`, `AMOUNT_PATTERN`, `ZERO_PATTERN`

- `_is_missing(name: str) -> Column`
  True when the column is null, empty or whitespace-only. Never null.
- `_not_in(name: str, allowed: tuple[str, ...]) -> Column`
  True when the value is not exactly one of `allowed` (null counts as not in). NULL IN (...) is NULL, so coalesce it to False before negating; otherwise null rows would match neither this rule nor its opposite.
- `_malformed_ts(name: str) -> Column`
  True when the value is present but is not a real YYYY-MM-DDTHH:MM:SS timestamp.
- `_amount_is_wellformed(name: str='amount') -> Column`
  True when the value is exactly -?digits.dd. Never null.
- `_malformed_amount() -> Column`
  True when the amount is present but not exactly -?digits.dd (max 16 integer digits).
- `_non_positive_amount() -> Column`
  Well-formed and zero or negative. String logic only, so huge values cannot overflow.
- `_rules() -> list[tuple[str, Column]]`
  (reason code, failure condition) in fixed order, so output arrays are deterministic.
- `rule_failures(df: DataFrame) -> DataFrame`
  Add `quarantine_reasons` (array<string>); an empty array means the row is valid.
- `split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]`
  Split a frame that already has REASONS_COL into (valid, quarantine). size() of a non-null array is never null, so every row lands in exactly one output.
- `validate_transactions(df: DataFrame) -> tuple[DataFrame, DataFrame]`
  Raw all-string frame in; (valid, quarantine) out.

## 3. Scripts and orchestration

### `scripts/scale_test.py`

One-off benchmark: times each stage on N generated rows in a temp folder.

- **Imports from the project:** `generate_data`, `ingest`, `kpis`, `publish`, `quality`

- `main() -> int`
  Time each stage on N generated rows in a temp folder, print a markdown table, never touch data/.

### `orchestration/finance_pipeline_dag.py`

SAMPLE Airflow DAG (not run in CI, Airflow is not a dependency).

- **Imports from the project:** none
- **Constants:** `PROJECT_DIR`

No functions: constants only.

## 4. Tests

Counts are test functions; `@pytest.mark.parametrize` expands them into more cases at run time.

| File | Test functions | What it covers |
|---|---|---|
| `tests/conftest.py` | 0 | Session-scoped Spark fixture; auto-marks every test that uses it as slow. |
| `tests/test_batches_ledger.py` | 10 | Batch splitting, SHA-256 ledger, atomic save, new/skip/conflict classification. |
| `tests/test_cleanroom.py` | 3 | Clean-room run reproduces seed-42 numbers; wrong expectation fails; non-empty workdir refused. |
| `tests/test_dedup.py` | 13 | Latest-version winner, ties, chains, order independence, reconciliation latest + superseded == valid. |
| `tests/test_engineering.py` | 25 | Schema single source, config precedence, logging, exit codes, lock, entry points. |
| `tests/test_extra_defects.py` | 9 | Opt-in defect kinds: all 11 appear when enabled, none by default, counts unchanged, quarantined with their own codes. |
| `tests/test_generate_data.py` | 70 | Generator: config validation, row counts, formatting, duplicates, ties, defects, CSV round-trip, CLI. |
| `tests/test_incremental.py` | 19 | Late arrivals, quarantine append, crash recovery, no-op re-run, incremental == full load. |
| `tests/test_ingest.py` | 16 | Schema, header check, FAILFAST read, empty-field and timestamp-parsing behavior pinned. |
| `tests/test_integration.py` | 17 | End to end run, 13 quality checks, corrupt-one-thing-per-check tests, CLI exit codes. |
| `tests/test_kpis.py` | 18 | KPI SQL against an independent Python oracle, edge cases, superseded-status flips. |
| `tests/test_orchestrate.py` | 15 | Retries and backoff, skip-after-failure, run log, sample DAG and workflow drift guards. |
| `tests/test_publish.py` | 8 | Manifest, Parquet types, quarantine CSV, idempotence, reconcile-before-write. |
| `tests/test_transform.py` | 9 | Typing, exact decimals, no-silent-null safeguard, quarantine flattening. |
| `tests/test_validate.py` | 13 | Every rule code, multiple defects per row, null-safety, split and reconciliation on the seed-42 file. |

Total test functions: 245.
