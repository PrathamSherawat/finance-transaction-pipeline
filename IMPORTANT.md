# Finance Transaction Pipeline

A small, end-to-end data engineering project that turns messy financial
transaction CSV files into validated, deduplicated, typed, and verified
datasets---then produces useful financial summaries.

The project demonstrates practical data engineering concepts including
data validation, deduplication, incremental processing, late-arriving
data, data quality checks, file-based snapshots, recovery,
orchestration, SQL analytics, and automated testing.

> **In one sentence:** Read raw transactions, identify bad records, keep
> the latest valid version of each transaction, publish the results,
> verify the files, and calculate KPIs.

------------------------------------------------------------------------

## Contents

-   [1. What problem does this solve?](#1-what-problem-does-this-solve)
-   [2. Pipeline at a glance](#2-pipeline-at-a-glance)
-   [3. Example: what happens to the
    data?](#3-example-what-happens-to-the-data)
-   [4. Outputs explained](#4-outputs-explained)
-   [5. Technologies used](#5-technologies-used)
-   [6. How to set up the project](#6-how-to-set-up-the-project)
-   [7. How to run it](#7-how-to-run-it)
-   [8. Full load vs incremental
    processing](#8-full-load-vs-incremental-processing)
-   [9. How the modules work together](#9-how-the-modules-work-together)
-   [10. Data quality and safety](#10-data-quality-and-safety)
-   [11. Testing and code quality](#11-testing-and-code-quality)
-   [12. Project structure](#12-project-structure)
-   [13. Important design decisions](#13-important-design-decisions)
-   [14. Current limitations](#14-current-limitations)
-   [15. Suggested learning path](#15-suggested-learning-path)
-   [16. Further documentation](#16-further-documentation)

------------------------------------------------------------------------

## 1. What problem does this solve?

Imagine receiving transaction files from a financial system every day.
Those files may contain:

-   Correct transaction records.
-   Multiple versions of the same transaction.
-   Invalid amounts, timestamps, currencies, or statuses.
-   Older records that arrive after newer versions.
-   Files that have already been processed---or files whose contents
    have unexpectedly changed.

Simply reading a CSV and writing another CSV is not enough. A dependable
pipeline must decide which records are valid, how duplicates are
handled, how failures are reported, and how to prove that the published
results are correct.

This project addresses those problems with a small, locally runnable
pipeline.

### The main processing stages

1.  **Read:** Load CSV files and check their headers.
2.  **Validate:** Evaluate each row against the data rules.
3.  **Separate:** Send invalid rows to quarantine and keep valid rows
    for further processing.
4.  **Deduplicate:** Select the latest version of each `transaction_id`;
    retain losing versions for auditing.
5.  **Transform:** Convert validated strings into appropriate data
    types.
6.  **Publish:** Write curated, superseded, and quarantine outputs, plus
    a manifest.
7.  **Verify:** Re-read the published files and run 13 data-quality
    checks.
8.  **Report:** Calculate KPIs only when all quality checks pass.

------------------------------------------------------------------------

## 2. Pipeline at a glance

``` mermaid
flowchart TD
    A["Incoming CSV files"] --> B["Ingest: read as strings"]
    B --> C["Validate each row"]
    C -->|Invalid rows + reason codes| D[("Quarantine")]
    C -->|Valid rows| E["Deduplicate by transaction_id"]
    E -->|Older / losing versions| F[("Superseded")]
    E -->|Latest valid versions| G["Transform: decimal and timestamps"]
    G --> H[("Curated Parquet")]
    D --> I["Quality checks on published files"]
    F --> I
    H --> I
    I -->|All checks pass| J["Run KPI SQL"]
    J --> K[("KPI CSV reports")]
```

### The three main data outcomes

  -----------------------------------------------------------------------
  Output                              Meaning
  ----------------------------------- -----------------------------------
  **Curated**                         The latest valid version of each
                                      transaction.

  **Superseded**                      Valid transaction versions that
                                      lost the deduplication decision
                                      because another version ranked
                                      higher.

  **Quarantine**                      Invalid rows, along with reason
                                      codes that explain why they were
                                      rejected.
  -----------------------------------------------------------------------

The pipeline preserves rejected and outdated records instead of silently
deleting them. This makes the processing easier to investigate and
audit.

------------------------------------------------------------------------

## 3. Example: what happens to the data?

Suppose an input file contains these simplified records:

  transaction_id     amount status    source_updated_ts
  ---------------- -------- --------- -------------------
  TXN101             500.00 POSTED    10:00
  TXN101             650.00 POSTED    11:00
  TXN102                abc POSTED    10:30
  TXN103             200.00 PENDING   10:45

Assume the timestamps are on the same day and `abc` is not a valid
amount.

-   **TXN101, amount 650.00:** This is the newer valid version, so it is
    kept in curated output.
-   **TXN101, amount 500.00:** This is an older valid version, so it is
    kept in superseded output.
-   **TXN102, amount `abc`:** This fails amount validation and is sent
    to quarantine with a reason code.
-   **TXN103, amount 200.00:** `PENDING` is an allowed status, so the
    row can remain curated if it passes all other rules. It does not
    contribute to the POSTED-only financial inflow/outflow KPIs.

The latest-arriving record is not automatically the winner. The
version-ordering rules decide which record wins.

------------------------------------------------------------------------

## 4. Outputs explained

The main outputs are written beneath the configured data directories.

### Curated data

`data/output/curated/transactions.parquet`

Contains the latest valid version of each transaction. The `amount`
column is stored as `DECIMAL(18,2)` and timestamp columns are stored as
timestamps.

### Superseded data

`data/output/superseded/superseded.parquet`

Contains valid records that lost the version-selection process. Keeping
these rows makes it possible to understand which versions were replaced.

### Quarantine data

`data/quarantine/quarantine.csv`

Contains invalid rows and their reason codes. Multiple reason codes are
joined with `;` so they can fit in a CSV cell. Incremental processing
also associates quarantined records with their batch.

### Manifest

`data/output/manifest.json`

Records processing metadata and row counts. The quality checks compare
its counts with the actual published files instead of treating the
manifest as unquestionable truth.

The central reconciliation rule is:

``` text
input_rows = curated_rows + superseded_rows + quarantined_rows
```

If the counts do not reconcile, publishing must not proceed in the
one-shot pipeline.

### KPI reports

`data/output/kpis/`

The SQL files produce these reports:

  -----------------------------------------------------------------------
  SQL file                            What it reports
  ----------------------------------- -----------------------------------
  `sql/kpi_totals.sql`                POSTED-only inflow, outflow, net
                                      amount, and count per currency.

  `sql/kpi_daily.sql`                 The same financial measures per
                                      currency and UTC transaction day.

  `sql/kpi_by_account.sql`            The same financial measures per
                                      currency and account.

  `sql/kpi_status_summary.sql`        Row counts by status across curated
                                      records; this is a sanity check,
                                      not a financial KPI.
  -----------------------------------------------------------------------

Financial measures are grouped by currency. Amounts in different
currencies should not be added together as though they were the same
unit.

------------------------------------------------------------------------

## 5. Technologies used

  -----------------------------------------------------------------------
  Technology                          Role in this project
  ----------------------------------- -----------------------------------
  **Python**                          Application logic, validation
                                      coordination, file management,
                                      orchestration, and error handling.

  **Apache Spark / PySpark**          Reads the raw data, evaluates
                                      validation rules, deduplicates
                                      records, and casts data types.

  **DuckDB**                          Writes Parquet and CSV outputs,
                                      queries published files, and runs
                                      SQL for verification and KPI
                                      calculations.

  **SQL**                             Defines the financial summaries and
                                      status report.

  **pytest**                          Automated tests for individual
                                      modules, integration flows, data
                                      quality, and recovery behavior.

  **Ruff**                            Python linting.

  **Mypy**                            Static type checking.

  **GitHub Actions**                  CI workflows for linting and
                                      automated tests.

  **Apache Airflow (sample only)**    A sample DAG is included; Airflow
                                      is not a project runtime
                                      dependency.
  -----------------------------------------------------------------------

### Why use Spark and DuckDB together?

Spark handles the main DataFrame processing. DuckDB handles file writing
and SQL queries over the published data.

This separation is useful for demonstrating how different tools can work
together. There is also a trade-off: the current publishing path
collects Spark results into Python memory before DuckDB writes them.
That is reasonable for the project's small benchmark workloads, but it
is not designed for arbitrarily large datasets.

------------------------------------------------------------------------

## 6. How to set up the project

### Prerequisites

-   Python compatible with the versions of the installed dependencies.
-   Java 17 for the Spark setup used by this project.
-   A terminal such as PowerShell, Command Prompt, or a Unix shell.
-   Git, if you want to clone the repository.

Check your versions:

``` powershell
python --version
java -version
```

Use the Java version configured for the project. If Spark fails to
start, verify that `JAVA_HOME` and the active Java executable point to
the intended installation.

### Create a virtual environment

From the project root:

``` powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

If PowerShell blocks activation, use an approved activation method for
your environment or run the virtual environment's Python executable
directly.

### Install dependencies

``` powershell
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

The runtime requirements include PySpark, DuckDB, and pytest.
Development requirements include Ruff and Mypy.

If the repository has already been configured as an installable package,
install it in editable mode as appropriate for your environment:

``` powershell
pip install -e .
```

------------------------------------------------------------------------

## 7. How to run it

Run commands from the project root unless the command says otherwise.

### A. Generate a sample CSV

The generator creates synthetic transactions with a controlled mixture
of valid rows, duplicate versions, and deliberately invalid records.

``` powershell
python -m finance_pipeline.generate_data
```

The default sample is configured with seed `42` and 1,000 rows. The
generated sample is documented as containing 870 valid rows, 80
duplicate-version rows, and 50 invalid rows. These are generator
categories, not the final curated/superseded/quarantine counts.

The default sample path is `data/sample/transactions.csv`.

### B. Run the one-shot pipeline

``` powershell
python -m finance_pipeline.pipeline
```

This runs the full pipeline on the configured input, publishes the
outputs, checks the published files, and writes KPIs only when all
checks pass.

### C. Run the incremental pipeline

Place new batch CSV files in `data/incoming/`, following the expected
schema and naming conventions, then run:

``` powershell
python -m finance_pipeline.incremental
```

The incremental pipeline checks the ledger, processes new batches,
updates snapshots, exports the current state, verifies the outputs, and
calculates KPIs.

Running it again without new batches should not reprocess already
committed batches.

### D. Run the orchestrator

``` powershell
python -m finance_pipeline.orchestrate
```

The orchestrator runs tasks in order, records each task's status, and
retries selected errors such as lock and operating-system errors using
exponential backoff. After a non-retryable task failure, remaining tasks
are skipped.

The orchestrator can also generate batches when its
`--generate-batches N` option is used. Check the command's help output
for available arguments:

``` powershell
python -m finance_pipeline.orchestrate --help
```

### E. Run the clean-room verification

``` powershell
python -m finance_pipeline.cleanroom
```

The clean-room runner creates a fresh working area, generates batches,
executes the pipeline, runs it again, and checks the expected seed-42
results. Its working directory must be empty or absent.

### F. Run the tests

Run the fast tests without tests marked as slow:

``` powershell
python -m pytest -m "not slow" -q
```

Run the full test suite:

``` powershell
python -m pytest -q
```

### G. Run linting and type checks

``` powershell
ruff check src tests
mypy src
```

### H. Measure scale performance

``` powershell
python scripts/scale_test.py --rows 20000
```

This benchmarks pipeline stages using generated data in a temporary
working directory. It is a measurement tool, not a guarantee that the
architecture can scale linearly to production-sized datasets.

> **Note:** The commands above describe the project's documented entry
> points and default behavior. If you use custom paths or configuration,
> consult `config/pipeline.toml`, the CLI help output, and the source
> modules.

------------------------------------------------------------------------

## 8. Full load vs incremental processing

The project supports two processing patterns.

### One-shot full load --- `pipeline.py`

The full-load pipeline processes the supplied CSV from the beginning:

1.  Read the input.
2.  Validate each row.
3.  Deduplicate valid records.
4.  Cast the winning records to the curated types.
5.  Reconcile counts and publish the files.
6.  Run quality checks on the published outputs.
7.  Calculate KPIs only if all checks pass.

Use this approach when you want to rebuild outputs from a source file.

### Incremental load --- `incremental.py`

The incremental pipeline processes new batch files and merges their
winners into the previously saved state.

``` mermaid
flowchart TD
    A["Start incremental run"] --> B["Acquire run lock"]
    B --> C["Hash files and inspect ledger"]
    C --> D{"New batch?"}
    D -->|No| H["Export current snapshot"]
    D -->|Yes| E["Process new batch with Spark"]
    E --> F["Merge winners with existing curated data"]
    F --> G["Write new snapshot, then save ledger"]
    G --> D
    H --> I["Run quality checks"]
    I --> J{"All pass?"}
    J -->|Yes| K["Write KPI reports"]
    J -->|No| L["Report quality failure"]
    K --> M["Release lock"]
    L --> M
```

### What happens to late-arriving versions?

  -----------------------------------------------------------------------
  New record                          Result
  ----------------------------------- -----------------------------------
  A new `transaction_id`              It can become a curated record if
                                      valid.

  A newer version of an existing ID   The new version wins; the previous
                                      curated version moves to
                                      superseded.

  An older version of an existing ID  It goes to superseded; the current
                                      curated version remains.

  A version with the same timestamp   Deterministic tie-break rules
                                      select a winner.

  An invalid row                      It is quarantined with reason codes
                                      and batch identification.
  -----------------------------------------------------------------------

### Ledger, snapshots, and lock

-   **Ledger:** Remembers processed batch names, SHA-256 hashes, row
    counts, and associated snapshots.
-   **Snapshot:** Saves the state resulting from processing a batch. The
    snapshot folders form an audit trail of successive states.
-   **Run lock:** Prevents concurrent incremental runs from modifying
    the same state directory.
-   **Atomic ledger save:** Writes a temporary file and replaces the
    previous ledger atomically.
-   **Recovery:** If a snapshot was written but the ledger was not
    committed, the batch can be retried from the previous committed
    state. Orphan snapshot folders are cleaned up on a subsequent run.
-   **Batch conflict:** If a previously processed filename reappears
    with different contents, processing rejects it rather than silently
    treating it as the same batch.
-   **Idempotent export:** Re-exporting the current snapshot can repair
    outputs after an interrupted export.

The ledger is the commit point for a batch. It is not a single atomic
transaction covering every output file and KPI.

------------------------------------------------------------------------

## 9. How the modules work together

Each module has a defined responsibility. This makes the project easier
to understand, test, and maintain.

### Main data-processing modules

  -----------------------------------------------------------------------
  Module                              Responsibility
  ----------------------------------- -----------------------------------
  `schema.py`                         Single source of truth for column
                                      names/order, timestamp format, and
                                      allowed values.

  `ingest.py`                         Creates the local Spark session,
                                      checks CSV headers, and reads all
                                      columns as strings.

  `validate.py`                       Evaluates row-level rules, produces
                                      reason codes, and separates valid
                                      and quarantined rows.

  `dedup.py`                          Selects the latest version per
                                      `transaction_id` and returns
                                      superseded versions.

  `transform.py`                      Casts amounts and timestamps,
                                      checks that casts do not silently
                                      produce nulls, and flattens
                                      quarantine reasons.

  `publish.py`                        Coordinates the full-load
                                      transformation and writes outputs
                                      and the manifest.

  `quality.py`                        Runs 13 checks against published
                                      files and cross-checks the manifest
                                      and KPIs.

  `kpis.py`                           Executes the SQL reports and writes
                                      KPI CSV files.
  -----------------------------------------------------------------------

### Entry points and supporting modules

  -----------------------------------------------------------------------
  Module                              Responsibility
  ----------------------------------- -----------------------------------
  `pipeline.py`                       Entry point for the one-shot
                                      full-load run.

  `incremental.py`                    Entry point and workflow for
                                      incremental processing, snapshots,
                                      and ledger updates.

  `orchestrate.py`                    Runs tasks in order, retries
                                      selected errors, and writes a run
                                      log.

  `cleanroom.py`                      Runs an end-to-end clean-room
                                      verification against expected
                                      results.

  `generate_data.py`                  Produces synthetic records,
                                      including controlled defects and
                                      duplicate versions.

  `batches.py`                        Splits generated rows into batch
                                      files for demos and tests.

  `ledger.py`                         Reads, classifies, and atomically
                                      saves processed-batch metadata.

  `lock.py`                           Implements the single-writer lock.

  `config.py`                         Loads settings from defaults, TOML,
                                      environment variables, and CLI
                                      overrides.

  `errors.py`                         Defines expected pipeline
                                      exceptions and maps them to process
                                      exit codes.

  `logging_setup.py`                  Configures diagnostic logging.
  -----------------------------------------------------------------------

### One-shot call flow

``` mermaid
sequenceDiagram
    participant CLI as pipeline.py
    participant PUB as publish.py
    participant SP as Spark processing
    participant DB as DuckDB
    participant QC as quality.py
    participant KPI as kpis.py

    CLI->>PUB: Run pipeline on input CSV
    PUB->>SP: Read, validate, deduplicate, transform
    SP-->>PUB: Curated, superseded, and quarantine rows
    PUB->>PUB: Reconcile row counts
    PUB->>DB: Write output files and manifest
    CLI->>QC: Check published files
    QC-->>CLI: Return all check results
    CLI->>KPI: Compute and write KPIs if all pass
```

A module can import another module without calling every function in it.
The diagrams show the intended dependency/processing flow;
`docs/REFERENCE.md` contains the generated file-by-file import and
function reference.

------------------------------------------------------------------------

## 10. Data quality and safety

The pipeline is designed to fail visibly rather than silently publish
questionable data.

### Read strings first, cast later

A typed CSV reader might convert a malformed timestamp to `NULL` before
the validation logic can inspect the original text. This project reads
all eight columns as strings first, validates the original values, and
only then casts the valid winners.

### Collect every failed rule

A row can violate more than one rule. The validation stage collects all
detected reason codes for that row, which gives users a clearer
explanation than reporting only the first failure.

### Handle nulls explicitly

Spark SQL expressions involving `NULL` can evaluate to unknown rather
than true or false. The validation rules account for this, and the split
is based on the size of the non-null reasons array. That ensures each
row is assigned to the valid or quarantine output.

### Validate before deduplication

Invalid records must not compete with valid records to become the latest
version of a transaction.

### Reconcile before publishing

The pipeline checks that every input row is accounted for as curated,
superseded, or quarantined. A mismatch is treated as an error.

### Verify files after writing

Quality checks read the published Parquet and CSV files, compare their
row counts with the manifest, check quarantine reasons, and cross-check
KPI calculations. This tests the output on disk rather than assuming
that a correct in-memory DataFrame guarantees correct files.

### Use decimal types for money

The generator represents amounts as integer cents before formatting
them. The processing and output layers use decimal types for financial
amounts rather than relying on binary floating-point arithmetic.

------------------------------------------------------------------------

## 11. Testing and code quality

The project reference documents **245 test functions**. Parameterized
tests can create more test cases at runtime than the function count
alone suggests.

The tests cover:

-   Generator configuration, row counts, duplicate versions, ties, and
    invalid data.
-   CSV ingestion, header validation, and string-first behavior.
-   Validation rules, multiple defects per row, and null handling.
-   Deduplication, timestamp ties, order independence, and row
    reconciliation.
-   Exact decimal and timestamp typing.
-   Publishing, manifests, and idempotence.
-   Incremental processing, late arrivals, batch conflicts, and
    recovery.
-   All 13 quality checks and corruption tests.
-   KPI results compared against an independent Python calculation.
-   Configuration precedence, logging, locks, retries, run logs, and CLI
    exit codes.
-   Clean-room reproducibility.

Spark tests share a session-scoped fixture and are marked slow. Use
`pytest -m "not slow"` for a faster development loop and `pytest -q` for
the full suite.

### Exit codes

    Code Meaning
  ------ -------------------------------------------------------------
     `0` Success.
     `1` A data-quality check failed.
     `2` Bad input, configuration, or arguments.
     `3` A previously processed batch filename has changed contents.
     `4` Another run holds the state lock.

The exception-to-exit-code mapping is centralized in
`errors.exit_code_for`. Individual entry points may document a narrower
subset of the codes they use.

------------------------------------------------------------------------

## 12. Project structure

``` text
finance-transaction-pipeline/
├── config/
│   └── pipeline.toml
├── data/
│   ├── sample/
│   │   └── transactions.csv
│   ├── incoming/
│   ├── state/
│   │   ├── ledger.json
│   │   ├── run.lock
│   │   ├── last_run.json
│   │   └── snapshots/
│   ├── output/
│   │   ├── curated/
│   │   ├── superseded/
│   │   ├── kpis/
│   │   └── manifest.json
│   └── quarantine/
│       └── quarantine.csv
├── docs/
│   ├── ARCHITECTURE.md
│   ├── REFERENCE.md
│   └── INTERVIEW.md
├── orchestration/
│   └── finance_pipeline_dag.py
├── scripts/
│   ├── demo.ps1
│   ├── gen_reference.py
│   └── scale_test.py
├── sql/
│   ├── kpi_totals.sql
│   ├── kpi_daily.sql
│   ├── kpi_by_account.sql
│   └── kpi_status_summary.sql
├── src/
│   └── finance_pipeline/
│       ├── __init__.py
│       ├── batches.py
│       ├── cleanroom.py
│       ├── config.py
│       ├── dedup.py
│       ├── errors.py
│       ├── generate_data.py
│       ├── incremental.py
│       ├── ingest.py
│       ├── kpis.py
│       ├── ledger.py
│       ├── lock.py
│       ├── logging_setup.py
│       ├── orchestrate.py
│       ├── pipeline.py
│       ├── publish.py
│       ├── quality.py
│       ├── schema.py
│       ├── transform.py
│       └── validate.py
├── tests/
├── requirements.txt
├── requirements-dev.txt
├── pyproject.toml
└── README.md
```

Some paths are created only when the corresponding command runs.
`run.lock` exists only while a run is active. Generated output and state
folders may not exist in a fresh checkout.

------------------------------------------------------------------------

## 13. Important design decisions

  -----------------------------------------------------------------------
  Decision                            Why it matters
  ----------------------------------- -----------------------------------
  Keep raw columns as strings at      Preserve malformed values for
  ingestion                           validation.

  Keep the schema contract in one     Avoid inconsistent column names and
  module                              allowed values across modules.

  Validate before deduplication       Prevent invalid rows from winning
                                      version selection.

  Keep superseded and quarantined     Preserve visibility into rejected
  records                             and replaced data.

  Use deterministic tie-breaks        Make deduplication reproducible
                                      even when timestamps tie.

  Reconcile row counts before         Detect records that were lost or
  publishing                          counted incorrectly.

  Write the ledger after the snapshot Make interrupted batches safely
                                      retryable from the last committed
                                      state.

  Use a lock for incremental runs     Prevent concurrent writers from
                                      corrupting shared state.

  Re-read published files for quality Verify the actual deliverables, not
  checks                              just intermediate DataFrames.

  Use independent test oracles        Avoid testing the implementation
                                      only against itself.

  Use Spark for processing and DuckDB Demonstrate a practical combination
  for file writing/SQL                of data-processing and analytical
                                      tools, with a known in-memory
                                      scaling trade-off.
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## 14. Current limitations

This project is an educational, locally runnable data engineering
pipeline. It demonstrates important engineering patterns, but it should
not be presented as a production banking platform.

Known limitations include:

1.  **In-memory publishing:** Spark output is collected into Python
    memory before DuckDB writes files. This limits practical data
    volume.
2.  **Local file-based state:** The ledger, lock, and snapshots use the
    filesystem rather than a distributed state store.
3.  **Synthetic data:** Generated transactions are for testing and
    demonstration, not real financial records.
4.  **Sample orchestration:** The included Airflow DAG is an example;
    Airflow is not a runtime dependency and the DAG is not part of the
    normal CI execution.
5.  **No currency conversion:** Financial summaries are grouped by
    currency; they do not convert currencies to a common base currency.
6.  **No guarantee of production scale:** Use the scale-test script to
    measure the current implementation. Larger workloads would require
    additional design and performance testing.

Possible future extensions include distributed or object-storage-based
publishing, stronger operational monitoring, configurable data
contracts, a production orchestration platform, and load testing at
larger data volumes.

------------------------------------------------------------------------

## 15. Suggested learning path

If you are new to the codebase, explore it in this order:

1.  **Start with `schema.py`.** Understand the eight columns and allowed
    values.
2.  **Read `ingest.py`.** See how the input CSV becomes a Spark
    DataFrame.
3.  **Read `validate.py`.** Follow how failed rules become reason codes.
4.  **Read `dedup.py`.** Understand the winner-selection ordering and
    superseded records.
5.  **Read `transform.py`.** Follow the string-to-decimal/timestamp
    conversion.
6.  **Read `publish.py`.** Understand reconciliation, file writing, and
    the manifest.
7.  **Read `quality.py`.** See how the output files are checked
    independently.
8.  **Read the SQL files and `kpis.py`.** Understand the reports.
9.  **Read `pipeline.py`.** Connect the one-shot workflow.
10. **Read `ledger.py`, `lock.py`, and `incremental.py`.** Understand
    state, snapshots, batch conflicts, and recovery.
11. **Read `orchestrate.py` and `cleanroom.py`.** Understand task
    execution and end-to-end verification.
12. **Run the tests.** Use them to check your understanding of expected
    behavior.

A useful exercise is to trace one transaction from the incoming CSV to
its final output and explain why it ended up in curated, superseded, or
quarantine.

------------------------------------------------------------------------

## 16. Further documentation

-   **`docs/ARCHITECTURE.md`** --- Diagrams of the data flow, module
    dependencies, one-shot execution, incremental execution, and
    orchestration.
-   **`docs/REFERENCE.md`** --- Generated file-by-file reference of
    modules, imports, functions, and tests. Regenerate it after source
    changes with `python scripts/gen_reference.py`.
-   **`docs/INTERVIEW.md`** --- Project pitch and explanations of the
    design decisions.
-   **`config/pipeline.toml`** --- Default configuration values and
    paths.
-   **`sql/`** --- KPI query definitions.
-   **`tests/`** --- Executable examples of expected behavior and edge
    cases.

------------------------------------------------------------------------

## Final summary

This project is more than a CSV transformation script. It demonstrates a
complete data engineering workflow:

**Ingest → Validate → Deduplicate → Transform → Publish → Verify →
Report**

It also demonstrates incremental processing, late-arriving data, batch
tracking, snapshots, locking, recovery, SQL analytics, and independent
automated testing.

The central goal is simple: **produce trustworthy data, preserve
evidence of what was rejected or replaced, and make failures visible
instead of silently publishing incorrect results.**
