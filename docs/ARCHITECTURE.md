# Architecture

How the pipeline is built, how data flows between files, and which module calls which.
See `REFERENCE.md` for every file and function.

## 1. The idea in one picture

Raw CSV rows are read as strings, checked row by row, split into valid and quarantined,
deduplicated, typed, published, verified by independent checks, and summarised as KPIs.

```mermaid
flowchart LR
    GEN["generate_data / batches<br/>synthetic CSV files"] --> CSV[("data/incoming<br/>batch CSVs")]
    CSV --> ING["ingest<br/>all-string read + header check"]
    ING --> VAL["validate<br/>rule_failures + split"]
    VAL -- "invalid rows + reason codes" --> QUA[("quarantine")]
    VAL -- "valid rows (strings)" --> DED["dedup<br/>latest version per transaction_id"]
    DED -- "older versions" --> SUP[("superseded")]
    DED -- "latest" --> TRN["transform<br/>cast to decimal + timestamp"]
    TRN --> CUR[("curated Parquet")]
    CUR --> QC["quality<br/>13 checks on the published files"]
    SUP --> QC
    QUA --> QC
    QC --> KPI["kpis + sql/*.sql<br/>POSTED-only inflow / outflow / net per currency"]
    KPI --> OUT[("data/output/kpis/*.csv")]
```

## 2. Module dependencies

An arrow means "imports from". Low-level modules at the bottom know nothing about the ones above.
`config`, `errors` and `logging_setup` are used by the entry-point modules (`pipeline`,
`incremental`, `orchestrate`) and are left out of the picture to keep it readable.

```mermaid
flowchart TD
    cleanroom --> orchestrate
    orchestrate --> incremental
    orchestrate --> batches
    pipeline --> publish
    pipeline --> quality
    incremental --> publish
    incremental --> ledger
    incremental --> lock
    incremental --> quality
    incremental --> dedup
    incremental --> transform
    publish --> dedup
    publish --> transform
    publish --> ingest
    quality --> kpis
    transform --> validate
    dedup --> validate
    publish --> validate
    ingest --> schema
    validate --> schema
    batches --> generate_data
    generate_data --> schema
    ledger --> errors
    lock --> errors
```

Rule enforced by a test: production code never imports `generate_data`, except `batches.py`.
The data contract (`COLUMNS`, timestamp format, allowed values) lives only in `schema.py`.

## 3. What the data looks like at each stage

| Stage | Where | Shape |
|---|---|---|
| Raw | Spark DataFrame from `ingest.read_raw_transactions` | 8 string columns; an empty CSV field is `null`; bad values are untouched |
| Validated | `validate.rule_failures` | raw columns + `quarantine_reasons` (`array<string>`, empty = valid) |
| Valid / quarantine | `validate.split_valid_quarantine` | valid: 8 string columns; quarantine: 8 columns + reasons |
| Latest / superseded | `dedup.dedup_transactions` | 8 string columns each; `latest + superseded == valid` |
| Curated | `transform.cast_to_curated` | `amount` is `decimal(18,2)`, the two timestamps are `timestamp`, the rest strings |
| Published | `publish` (DuckDB writers) | Parquet for curated and superseded, CSV for quarantine (reasons joined with `;`), `manifest.json` |
| Snapshot | `incremental.merge_into_snapshot` | `curated.parquet`, `superseded.parquet`, `quarantine.csv` (+ `batch_id`), `manifest.json` |

Why strings first: a typed reader would turn `not-a-timestamp` into `null` before validation could
see it. Casting happens only after validation and dedup, with a safeguard that raises if any cast
would produce a null.

## 4. One-shot run (`pipeline.py`)

```mermaid
sequenceDiagram
    participant CLI as pipeline.main
    participant PUB as publish.run_pipeline
    participant SP as Spark (ingest, validate, dedup, transform)
    participant DB as DuckDB writers
    participant QC as quality.run_quality_checks
    participant K as kpis
    CLI->>PUB: input CSV
    PUB->>SP: read, validate, dedup, cast
    SP-->>PUB: curated, superseded, quarantine rows (collect)
    PUB->>PUB: input_rows == curated + superseded + quarantined ?
    PUB->>DB: write Parquet, CSV, manifest (only if counts reconcile)
    CLI->>QC: re-read the PUBLISHED files
    QC-->>CLI: 13 results (all failures reported)
    CLI->>K: compute and write KPIs (only if every check passed)
```

## 5. Incremental run (`incremental.py`)

New batch files in `data/incoming` are merged into the existing state without reprocessing old data.

```mermaid
sequenceDiagram
    participant R as run_incremental
    participant L as lock.RunLock
    participant LG as ledger
    participant SP as Spark (per batch)
    participant M as merge_into_snapshot (DuckDB)
    participant S as state/snapshots/snap_000N
    participant O as data/output + data/quarantine
    R->>L: acquire state/run.lock (atomic create)
    R->>LG: hash every file, classify new / skip / conflict
    Note over R,LG: a changed file under a known name is rejected before any work
    loop each NEW batch, in name order
        R->>SP: read, validate, dedup inside the batch, type
        R->>M: old curated + batch winners, same total order as dedup
        M->>S: write new snapshot folder
        R->>LG: save ledger LAST (atomic replace): the commit point
    end
    R->>O: export current snapshot (idempotent copy)
    R->>R: 13 quality checks, then KPIs
    R->>L: release lock
```

Why the ledger is written last: if it were written first and the process crashed, the batch would
be marked done but its data would never have been published. Written last, a crash just means the
batch is retried from the previous snapshot. Orphan snapshot folders are deleted on the next run.

How a late-arriving version is handled:

| New batch contains | Result |
|---|---|
| A new `transaction_id` | goes to curated |
| A newer version of an existing id | becomes curated; the old curated row moves to superseded |
| An older version of an existing id | goes straight to superseded; curated unchanged |
| Same timestamp as the current winner | deterministic tie-break (status, amount, account, ...) |
| An invalid row | quarantined, tagged with the batch file name |

## 6. Orchestration and failure handling

```mermaid
flowchart TD
    A["orchestrate.main"] --> B{"--generate-batches N ?"}
    B -- yes --> C["task: generate_batches"]
    B -- no --> D
    C --> D["task: incremental"]
    D --> E{"failed?"}
    E -- "LockError / OSError" --> F["retry with exponential backoff"]
    F --> D
    E -- "other error" --> G["mark task failed, skip remaining tasks"]
    E -- no --> H["all tasks ok"]
    G --> I["write data/state/last_run.json"]
    H --> I
    I --> J["exit code = first failed task's code"]
```

| Exit code | Meaning | Raised as |
|---|---|---|
| 0 | success | |
| 1 | a data-quality check failed | `QualityCheckError` |
| 2 | bad input or configuration | `ValueError`, `FileNotFoundError` |
| 3 | a batch file changed after it was processed | `BatchConflictError` |
| 4 | another run holds `state/run.lock` | `LockError` |

The mapping lives in one place: `errors.exit_code_for`.

## 7. Where files live on disk

```text
data/
  sample/transactions.csv          generated sample (seed 42, 1000 rows)
  incoming/                        batch CSVs waiting to be processed
  state/
    ledger.json                    processed batches (name, SHA-256, rows, snapshot)
    run.lock                       exists only while a run is active
    last_run.json                  orchestrator run log
    snapshots/snap_0001 ...        one folder per processed batch (audit trail)
  output/
    curated/transactions.parquet
    superseded/superseded.parquet
    manifest.json                  input = curated + superseded + quarantined
    kpis/*.csv                     totals, daily, by_account, status_summary
  quarantine/quarantine.csv        rejected rows + reason codes (+ batch_id when incremental)
```

## 8. Design decisions that shape the architecture

- **Validate before dedup:** an invalid row must never win a "latest version" contest.
- **Collect all failed rules per row:** first-fail hides problems.
- **Null-safe rules:** `NULL IN (...)` is `NULL`, so every rule is coalesced to true/false and the
  split uses `size(reasons) == 0`; nothing can fall out of both outputs.
- **Reconcile before writing:** a count mismatch raises and nothing is published.
- **Checks read the published files:** a corrupted write cannot hide behind in-memory DataFrames,
  and the manifest is cross-checked rather than trusted.
- **Money as decimals:** integer cents in the generator, `DECIMAL` in Spark and DuckDB.
- **Spark for typing, DuckDB for writing:** files are written with DuckDB after `collect()`. This
  suits thousands of rows, not billions (see the measured scale results in the README).

## 9. Test architecture

- Spark tests share one session through a session-scoped fixture in `tests/conftest.py`; every test
  that uses it is auto-marked `slow`, so `pytest -m "not slow"` is a fast loop.
- Test data is written as small CSV files and read through the real ingestion path.
- Independent oracles (plain Python) re-derive validity, dedup and KPIs, so tests do not check the
  implementation against itself.
- Corruption tests break one thing in a copy of the output and assert that the matching check fails.
