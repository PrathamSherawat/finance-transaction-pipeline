# Finance Transaction Intelligence Pipeline

A portfolio data-engineering project that simulates a financial transaction feed:
it generates synthetic transactions (including deliberately bad ones), ingests them
with PySpark, validates and quarantines bad records, resolves duplicate versions,
publishes typed curated data, computes KPIs in DuckDB SQL, and verifies its own
output with data-quality checks. All data is synthetic.

**Stack:** Python 3.12, PySpark 3.5.6, DuckDB, pytest. Java 17 is required for Spark.

## Quick start (Windows PowerShell)

    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    pip install -r requirements.txt
    pip install -e .

    python -m finance_pipeline.generate_data --rows 1000 --seed 42   # data/sample/transactions.csv
    python -m finance_pipeline.pipeline                              # publish + checks + KPIs
    python -m pytest -q                                              # takes a couple of minutes (Spark)

`pipeline` exits 0 on success, 1 if a data-quality check fails, 2 on bad input.

## Architecture

    generate_data -> CSV -> ingest (all-string, header check)
        -> validate -> valid | quarantine (reason codes)
        -> dedup (valid only) -> latest | superseded
        -> transform (typed) -> publish (Parquet/CSV + manifest)
        -> quality checks (re-read published files) -> KPIs (DuckDB SQL)

| Module | Responsibility |
|---|---|
| `generate_data.py` | Seeded synthetic data: valid rows, duplicate versions, ties, 7 defect types |
| `ingest.py` | Spark session, explicit all-string schema, header check, FAILFAST read |
| `validate.py` | Per-row rule failures collected into `quarantine_reasons`; valid/quarantine split |
| `dedup.py` | Latest version per `transaction_id`; deterministic tie-break; conflict counter |
| `transform.py` | Cast to `decimal(18,2)` and timestamps, with a no-silent-null safeguard |
| `publish.py` | Reconcile-before-write, DuckDB writers, manifest |
| `quality.py` | 13 checks over the published files |
| `kpis.py` + `sql/*.sql` | POSTED-only inflow/outflow/net per currency (totals, daily, per account) |
| `pipeline.py` | Orchestrator and CLI |

## Data contract

Columns, in order: `transaction_id, account_id, transaction_ts, transaction_type, amount,
currency, status, source_updated_ts`. Timestamps are naive ISO `YYYY-MM-DDTHH:MM:SS`,
treated as UTC. Amounts are strings with exactly two decimals (at most 16 integer digits)
in the CSV and `DECIMAL(18,2)` in the curated Parquet.

## Business rules

- Only `POSTED` transactions count toward KPIs. `CREDIT` is inflow, `DEBIT` is outflow,
  net = inflow - outflow.
- Currencies are never added together; every KPI groups by currency.
- Deduplicate by `transaction_id`, keeping the latest `source_updated_ts`. On equal
  timestamps the greatest tuple of (status, amount, account_id, transaction_ts,
  transaction_type, currency) wins. This is arbitrary but deterministic. Identical
  re-deliveries are harmless; same-timestamp rows with different content are counted
  as conflicts.
- Invalid rows are quarantined, never dropped or repaired. Matching is strict:
  `" INR"` and `"posted"` are quarantined.

Quarantine codes: `MISSING_TRANSACTION_ID`, `MISSING_ACCOUNT_ID`, `MISSING_TRANSACTION_TS`,
`MALFORMED_TRANSACTION_TS`, `INVALID_TRANSACTION_TYPE`, `MISSING_AMOUNT`, `MALFORMED_AMOUNT`,
`NON_POSITIVE_AMOUNT`, `UNSUPPORTED_CURRENCY`, `INVALID_STATUS`, `MISSING_SOURCE_UPDATED_TS`,
`MALFORMED_SOURCE_UPDATED_TS`.

## Outputs

- `data/output/curated/transactions.parquet` - latest version per transaction, typed
- `data/output/superseded/superseded.parquet` - older versions
- `data/quarantine/quarantine.csv` - rejected rows plus `;`-joined reason codes
- `data/output/manifest.json` - counts; `input = curated + superseded + quarantined`
- `data/output/kpis/*.csv` - KPI results

## Design decisions and trade-offs

- **All-string ingestion.** A typed reader schema turns bad values into nulls before
  validation can see them, so the evidence is lost. Types are applied after validation.
- **Validate before dedup.** An invalid row must never win a "latest version" contest.
- **Collect all failed rules per row.** First-fail hides problems.
- **Null-safe rules.** `NULL IN (...)` is NULL, so rows could fall out of both a filter
  and its negation. Every rule is coalesced to a definite true/false and the split uses
  `size(reasons) == 0`.
- **Money as decimals, never floats.** Generator uses integer cents; Spark and DuckDB use
  `DECIMAL`. Sums are exact.
- **Shuffled generator output.** File order never reveals the newest version, so dedup
  must really use `source_updated_ts`.
- **Reconcile before writing.** A mismatch raises and nothing is published.
- **Quality checks read the published files**, and the manifest is cross-checked, not trusted.
- **Tests use independent oracles** (plain Python re-derivation of validity, dedup and KPIs).
  Tests also corrupt the output to prove each check can fail.

## Known limitations

- File writing uses `collect()` plus DuckDB. That suits thousands of rows, not billions;
  at scale use Spark's Parquet writer, date partitioning and no `collect()`.
- The tie-break rule is arbitrary, not a business priority.
- KPI days use the UTC date, not a local business day.
- The generator never emits `MISSING_AMOUNT`, `MALFORMED_AMOUNT` or bad
  `source_updated_ts` rows; those rules are covered by hand-built tests.
- On the author's Windows machine Spark Python workers crash, so the code uses only
  built-in column expressions (no UDFs, `createDataFrame` or `toPandas`). Spark prints
  harmless `winutils`/`NativeCodeLoader` warnings.
- `check_header` works on local paths only.

## Incremental processing

Batch files dropped in `data/incoming/` are merged into the published state without
reprocessing old data:

    python -m finance_pipeline.batches --rows 1000 --seed 42 --batches 3   # demo batches
    python -m finance_pipeline.incremental                                 # processes only NEW files

- **Ledger** (`data/state/ledger.json`): file name, SHA-256, row count and snapshot per
  processed batch. Same content again is skipped. Same name with changed content is
  rejected (exit code 3) before anything is processed.
- **Merge:** Spark validates, dedups and types each batch. DuckDB then ranks old curated
  plus the batch winners with the same total order as `dedup.py`. Losers go to superseded,
  so batches A then B give the same result as A+B in one load (tested for 2, 3 and 5
  splits, forward and reversed).
- **Late arrivals:** an older version of an existing transaction goes straight to superseded.
- **Atomic publish:** each batch writes a new snapshot folder. The ledger is replaced last
  and is the commit point; a crash leaves the previous state intact and the batch is retried.
  The current snapshot is then copied to `data/output` and `data/quarantine`.
- **Quarantine** is appended per batch with a `batch_id` column. The manifest is cumulative.
- Limitations: one writer at a time (no locking), snapshots accumulate (no pruning), and
  merging still uses `collect()` per batch.

## Engineering conventions

- **Config:** `config/pipeline.toml`, overridden by `FIN_<NAME>` environment variables
  (e.g. `FIN_OUTPUT_DIR`), overridden by CLI flags.
- **Logging:** standard `logging` to stderr; stdout carries only the JSON summary.
  Level comes from `log_level` in the config.
- **Exit codes:** 0 ok, 1 quality check failed, 2 bad input, 3 batch file changed,
  4 another run holds `data/state/run.lock` (stale after 6 hours).
- **Entry points** (after `pip install -e .`): `fin-generate`, `fin-run`, `fin-incremental`.
- **Dev tools:** `pip install -r requirements-dev.txt`, then `ruff check src tests` and `mypy src`.
- **Fast tests:** `python -m pytest -m "not slow"` skips everything that starts Spark.
- **Schema:** `schema.py` is the single source of truth for columns, timestamp format and
  allowed values; production code never imports the test-data generator.

## Orchestration and CI

    python -m finance_pipeline.orchestrate --generate-batches 3 --rows 1000 --seed 42

- **Orchestrator:** ordered tasks, retries with exponential backoff (lock errors and OS errors
  only; a changed file, failed quality check or bad input fails immediately), per-task status,
  later tasks skipped after a failure, and a run log at `data/state/last_run.json`.
  Exit code = first failed task's code (0/1/2/3/4), so any scheduler can alert on it.
- **Airflow:** `orchestration/finance_pipeline_dag.py` is a sample DAG (not run in CI, Airflow
  is not a dependency).
- **CI** (`.github/workflows/ci.yml`): lint (ruff, mypy) always; fast tests on every push;
  full suite with Java 17 on pull requests and nightly.
- **Scheduled demo** (`scheduled-run.yml`): daily run on generated data that asserts the
  seed-42 manifest and uploads the manifest, KPIs and run log as artifacts.

## Verification and portfolio

    python -m finance_pipeline.cleanroom                       # whole pipeline from nothing, in a temp folder
    python -m finance_pipeline.generate_data --extra-defects   # opt-in: 11 defect kinds instead of 7
    python scripts\scale_test.py --rows 200000                 # per-stage timings on generated data

- **Clean-room run:** generates 3 batches, runs the pipeline, runs it again, and checks the seed-42
  manifest (1000 / 870 / 80 / 50), that the second run is a no-op, the ledger and the KPI totals.
  It uses an empty temp folder and refuses to use a non-empty `--workdir`.
- **Opt-in defects:** `MISSING_AMOUNT`, `MALFORMED_AMOUNT` and bad `source_updated_ts` rows. Off by
  default so the sample file and exact-count tests are unchanged.
- **Interview pack:** `docs/INTERVIEW.md` has the pitch and one answer per design decision.

### Seed-42 results

| | INR | USD |
|---|---|---|
| inflow | 539,926.47 | 186,576.53 |
| outflow | 654,543.80 | 286,894.51 |
| net | -114,617.33 | -100,317.98 |
| POSTED rows | 476 | 184 |

1000 input rows -> 870 curated, 80 superseded, 50 quarantined.

## Measured scale results

**Local Windows benchmark, single run, not a CI result.** Measured with
`python scripts/scale_test.py --rows 20000` (seed 42) on the author's Windows laptop
(local Spark, `local[2]`, Java 17). Timings depend on the machine and were measured once, so treat
them as indicative.

| stage | seconds |
|---|---|
| generate rows (Python) | 0.4 |
| write CSV | 0.1 |
| pipeline: read, validate, dedup, type, publish | 245.1 |
| 13 quality checks | 1.3 |
| KPI SQL | 0.1 |
| total | 247.0 |

- **Throughput:** about 82 rows/s through the pipeline stage.
- **Correctness at this size:** 20,000 input rows -> 17,400 curated, 1,600 superseded,
  1,000 quarantined (the same 87% / 8% / 5% split as the 1,000-row sample). All 13 quality
  checks passed.
- **Where the time goes:** the pipeline stage is about 99% of the total. The generator, the
  DuckDB-based checks and the KPI SQL take about 2 seconds combined.
- **Not measured:** 200,000 rows (extrapolating the rate gives roughly 40 minutes, so it was not
  run), Linux, a Spark-native Parquet writer, and any profile of the pipeline stage.
- **Hypothesis (untested):** the stage runs several Spark actions on uncached DataFrames, so each
  one re-reads the CSV and re-runs validation and the dedup window, and `collect()` moves rows
  through Python. The planned experiment is to `.cache()` the reused frames and re-time 20,000
  rows; if that barely helps, the next step is Spark's own Parquet writer with date partitioning.

## Verification status

- **Verified locally (Windows):** the full test suite (451 tests), `ruff`, `mypy`, the clean-room
  run (5/5 checks) and the benchmark above.
- **Not yet verified:** the GitHub Actions workflows in `.github/workflows/` were written but have
  not been run. Their first runs will also be the first runs on Linux. Check the Actions tab after
  the first push.
