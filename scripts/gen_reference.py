"""Generate docs/REFERENCE.md from the real source files (ast, no imports executed).

Run from the project root:  python scripts/gen_reference.py
Function lists, signatures, docstrings, imports and test counts come from the code itself.
Descriptions for functions without a docstring come from the DESC table below.
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src" / "finance_pipeline"

MODULE = {
    "schema": "The data contract: column order, timestamp format, allowed types, currencies and statuses. Single source of truth.",
    "errors": "Exception hierarchy (PipelineError and subclasses) and the one mapping from exceptions to CLI exit codes.",
    "config": "Settings: CLI flag > FIN_<NAME> environment variable > config/pipeline.toml > defaults.",
    "logging_setup": "Logging to stderr only, so stdout stays reserved for the JSON summary line.",
    "lock": "Single-writer lock file (atomic exclusive create) so two incremental runs cannot overlap.",
    "generate_data": "Seeded synthetic transaction generator: valid rows, duplicate versions, ties, deliberately bad rows, CSV writer, CLI.",
    "batches": "Splits generated rows into several batch files for incremental-processing demos and tests.",
    "ingest": "Spark session factory and the CSV reader: explicit all-string schema, header check, FAILFAST.",
    "validate": "Per-row rules producing a list of failed-rule codes; splits rows into valid and quarantine.",
    "dedup": "Keeps the latest version of each transaction_id with a deterministic total order; counts tie conflicts.",
    "transform": "Casts validated, deduplicated rows to real types (decimal, timestamp) with a no-silent-null safeguard.",
    "publish": "Reconcile-before-write publishing with DuckDB writers, the manifest, and the one-shot run_pipeline.",
    "kpis": "Runs the SQL files in sql/ against the curated Parquet file and writes one CSV per KPI.",
    "quality": "13 data-quality checks that re-read the PUBLISHED files and cross-check the manifest and KPIs.",
    "pipeline": "One-shot orchestration: publish, quality checks, KPIs. CLI with exit codes 0/1/2.",
    "ledger": "Ledger of processed batch files (SHA-256) and which snapshot is current; atomic save.",
    "incremental": "Incremental processing: Spark per batch, DuckDB merge, snapshots, ledger as commit point, export, checks, KPIs.",
    "orchestrate": "Plain-Python task runner: ordered tasks, retries with backoff for retryable errors, per-task status, run log.",
    "cleanroom": "Runs the whole pipeline from nothing in an empty folder and checks the known seed-42 numbers.",
    "scale_test": "One-off benchmark: times each stage on N generated rows in a temp folder.",
    "finance_pipeline_dag": "SAMPLE Airflow DAG (not run in CI, Airflow is not a dependency).",
}

DESC = {
    "generate_data._require_int": "Validate an integer setting (rejects bool, which is a subclass of int) with an optional minimum; raises ValueError naming the field.",
    "generate_data._require_rate": "Validate a rate in [0, 1); rejects bool and non-numbers.",
    "generate_data.GeneratorConfig": "Frozen dataclass holding every generator setting (rows, seed, rates, start date, window, tie rate, extra_defects). Invalid values raise ValueError at construction.",
    "generate_data.GeneratorConfig.__post_init__": "Runs every validation, including invalid_rate + 2 * duplicate_rate < 1 so each duplicate has its own valid base row.",
    "generate_data.RowCounts": "Named tuple (valid, invalid, duplicate): the mutually exclusive split of num_rows.",
    "generate_data.build_parser": "Builds the argparse parser for the generator CLI; defaults come from GeneratorConfig().",
    "generate_data.main": "Generator CLI: validates the config, writes the CSV, prints a summary; returns 2 on an invalid configuration.",
    "ingest.RAW_SCHEMA": "StructType of 8 StringType fields built from COLUMNS.",
    "validate._malformed_amount": "True when the amount is present but not exactly -?digits.dd (max 16 integer digits).",
    "dedup._require_columns": "Raise ValueError if the input frame lacks any contract column.",
    "transform._require_columns": "Raise ValueError if the input frame lacks any of the given columns.",
    "publish._typed_rows": "Collect a typed frame into tuples of strings (timestamps formatted, decimals as text) so DuckDB re-casts them without Python conversions.",
    "publish._string_rows": "Collect selected columns of an all-string frame into tuples.",
    "publish._write": "Load rows into a DuckDB staging table and COPY them to a Parquet or CSV file, ordered deterministically.",
    "publish.main": "CLI: runs run_pipeline on the sample CSV and prints the manifest.",
    "kpis.main": "CLI: computes KPIs from the curated file, writes the CSVs and prints the totals.",
    "quality._lit": "Render a set of values as a sorted SQL string-literal list.",
    "quality._sql_path": "Render a path as a DuckDB-safe quoted string body.",
    "quality._scalar": "Run a query and return its single value.",
    "quality._Ctx": "Holds the open DuckDB connection with views over the published files, the manifest, and lazily computed KPIs.",
    "quality._Ctx.__init__": "Store the connection, manifest, curated path and SQL directory.",
    "quality._Ctx.kpis": "Compute the KPIs once and cache them for the KPI checks.",
    "quality._count_check": "Build a check from a COUNT query that must return 0; the detail names what was found.",
    "quality._manifest_matches_files": "Compare manifest row counts with the real row counts of the three output files.",
    "quality._reconciliation": "Check that the rows in all three files add up to the manifest's input_rows.",
    "quality._quarantine_reasons_known": "Every quarantine row must have a reason, and every code must be in KNOWN_REASON_CODES.",
    "quality._kpi_totals_agree": "KPI totals must agree with a direct query on the curated file (counts, net = inflow - outflow, per-currency sums).",
    "quality._kpi_status_sums_to_curated": "The status summary must add up to the number of curated rows.",
    "quality.CheckResult": "Frozen dataclass (name, passed, detail) returned by every check.",
    "ledger.empty_ledger": "A fresh ledger: version 1, no current snapshot, no batches.",
    "ledger.load_ledger": "Read ledger.json (or return an empty ledger if absent); reject files that are not a version-1 ledger.",
    "incremental._q": "Render a path as a DuckDB-safe quoted string body.",
    "incremental._load_stage": "Create a VARCHAR staging table in DuckDB and bulk-insert rows.",
    "incremental._count": "Row count of a DuckDB table.",
    "incremental._parser": "Argparse parser for the incremental CLI; defaults come from the settings.",
    "orchestrate.TaskResult": "Dataclass describing one task outcome: status (ok/failed/skipped), attempts, seconds, error, exit code.",
    "orchestrate.overall_exit_code": "Exit code of the first failed task, else 0.",
    "orchestrate._parser": "Argparse parser for the orchestrator CLI.",
    "cleanroom._read_json": "Read a JSON file, or return an empty dict if it does not exist.",
    "lock.RunLock": "Context manager that atomically creates the lock file and removes it on exit; a lock older than max_age_seconds counts as stale.",
    "lock.RunLock.__init__": "Store the lock path and the staleness limit (default 6 hours).",
    "lock._is_stale" if False else "lock.RunLock._is_stale": "True if the lock file is older than max_age_seconds (or vanished).",
    "lock.RunLock.__enter__": "Atomically create the lock file (O_EXCL); replace one stale lock; otherwise raise LockError.",
    "lock.RunLock.__exit__": "Remove the lock file if this instance created it.",
    "config.Settings": "Frozen dataclass of string settings: input_csv, incoming_dir, state_dir, output_dir, quarantine_dir, sql_dir, log_level.",
    "logging_setup._StderrHandler": "Logging handler that looks up sys.stderr at emit time, so a stream swapped by a test never goes stale.",
    "logging_setup._StderrHandler.emit": "Format the record and write it to the current sys.stderr.",
    "batches.write_batches": "Write each chunk as <prefix>_NNNN.csv in the output directory and return the paths.",
    "batches.main": "CLI: generate rows and write them as N batch files.",
    "scale_test.main": "Time each stage on N generated rows in a temp folder, print a markdown table, never touch data/.",
}

STATIC_INVENTORY = """
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
"""

TEST_DESC = {
    "conftest": "Session-scoped Spark fixture; auto-marks every test that uses it as slow.",
    "test_generate_data": "Generator: config validation, row counts, formatting, duplicates, ties, defects, CSV round-trip, CLI.",
    "test_extra_defects": "Opt-in defect kinds: all 11 appear when enabled, none by default, counts unchanged, quarantined with their own codes.",
    "test_batches_ledger": "Batch splitting, SHA-256 ledger, atomic save, new/skip/conflict classification.",
    "test_ingest": "Schema, header check, FAILFAST read, empty-field and timestamp-parsing behavior pinned.",
    "test_validate": "Every rule code, multiple defects per row, null-safety, split and reconciliation on the seed-42 file.",
    "test_dedup": "Latest-version winner, ties, chains, order independence, reconciliation latest + superseded == valid.",
    "test_transform": "Typing, exact decimals, no-silent-null safeguard, quarantine flattening.",
    "test_publish": "Manifest, Parquet types, quarantine CSV, idempotence, reconcile-before-write.",
    "test_kpis": "KPI SQL against an independent Python oracle, edge cases, superseded-status flips.",
    "test_integration": "End to end run, 13 quality checks, corrupt-one-thing-per-check tests, CLI exit codes.",
    "test_incremental": "Late arrivals, quarantine append, crash recovery, no-op re-run, incremental == full load.",
    "test_engineering": "Schema single source, config precedence, logging, exit codes, lock, entry points.",
    "test_orchestrate": "Retries and backoff, skip-after-failure, run log, sample DAG and workflow drift guards.",
    "test_cleanroom": "Clean-room run reproduces seed-42 numbers; wrong expectation fails; non-empty workdir refused.",
}


def parse(path):
    return ast.parse(path.read_text(encoding="utf-8-sig"))


def sig(node):
    ret = f" -> {ast.unparse(node.returns)}" if node.returns else ""
    return f"({ast.unparse(node.args)}){ret}"


def project_imports(tree):
    deps = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module:
            if n.module.startswith("finance_pipeline."):
                deps.add(n.module.split(".")[1])
            elif n.module == "finance_pipeline":
                deps.update(a.name for a in n.names)
        elif isinstance(n, ast.Import):
            deps.update(a.name.split(".")[1] for a in n.names if a.name.startswith("finance_pipeline."))
    return deps


used = set()


def text_for(mod, qual, node):
    key = f"{mod}.{qual}"
    if key in DESC:
        used.add(key)
        return DESC[key]
    doc = ast.get_docstring(node)
    return " ".join(doc.split()) if doc else "(no description)"


def constants(tree):
    names = []
    for n in tree.body:
        targets = n.targets if isinstance(n, ast.Assign) else [n.target] if isinstance(n, ast.AnnAssign) else []
        names += [t.id for t in targets if isinstance(t, ast.Name) and t.id.isupper()]
    return names


def module_section(path, importers, level="###"):
    mod = path.stem
    tree = parse(path)
    out = [f"{level} `{path.relative_to(ROOT).as_posix()}`", "", MODULE.get(mod, ast.get_docstring(tree) or ""), ""]
    deps = sorted(d for d in project_imports(tree) if d != mod)
    out.append("- **Imports from the project:** " + (", ".join(f"`{d}`" for d in deps) or "none"))
    if importers.get(mod):
        out.append("- **Used by:** " + ", ".join(f"`{d}`" for d in sorted(importers[mod])))
    consts = constants(tree)
    if consts:
        out.append("- **Constants:** " + ", ".join(f"`{c}`" for c in consts))
    out.append("")
    count = 0
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out += [f"- `{n.name}{sig(n)}`", f"  {text_for(mod, n.name, n)}"]
            count += 1
        elif isinstance(n, ast.ClassDef):
            out += [f"- **class `{n.name}`**", f"  {text_for(mod, n.name, n)}"]
            count += 1
            for m in n.body:
                if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    out += [f"  - `{n.name}.{m.name}{sig(m)}`", f"    {text_for(mod, n.name + '.' + m.name, m)}"]
                    count += 1
    if not count:
        out.append("No functions: constants only." if consts else "No functions.")
    out.append("")
    return out, count


def main():
    src_files = [p for p in sorted(SRC.glob("*.py")) if p.name != "__init__.py"]
    importers = {}
    for p in src_files:
        for d in project_imports(parse(p)):
            importers.setdefault(d, set()).add(p.stem)

    lines = [
        "# Project reference",
        "",
        "Every file in the project and every function in the code. The function lists, signatures, imports",
        "and test counts below are generated from the source by `scripts/gen_reference.py`; run",
        "`python scripts/gen_reference.py` again after changing code. See `ARCHITECTURE.md` for diagrams.",
        STATIC_INVENTORY,
        "## 2. Source modules and functions",
        "",
        "Private helpers start with an underscore. Signatures are shown as written in the code.",
        "",
    ]
    total = 0
    for p in src_files:
        sec, n = module_section(p, importers)
        lines += sec
        total += n

    lines += ["## 3. Scripts and orchestration", ""]
    for p in [ROOT / "scripts" / "scale_test.py", ROOT / "orchestration" / "finance_pipeline_dag.py"]:
        if p.is_file():
            sec, n = module_section(p, {})
            lines += sec
            total += n

    lines += ["## 4. Tests", "",
              "Counts are test functions; `@pytest.mark.parametrize` expands them into more cases at run time.", "",
              "| File | Test functions | What it covers |", "|---|---|---|"]
    all_tests = 0
    for p in sorted((ROOT / "tests").glob("*.py")):
        if p.name == "__init__.py":
            continue
        tree = parse(p)
        n = sum(1 for x in ast.walk(tree) if isinstance(x, ast.FunctionDef) and x.name.startswith("test_"))
        all_tests += n
        lines.append(f"| `tests/{p.name}` | {n} | {TEST_DESC.get(p.stem, '')} |")
    lines += ["", f"Total test functions: {all_tests}.", ""]

    out = ROOT / "docs" / "REFERENCE.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    unused = sorted(set(DESC) - used)
    print(f"wrote {out} ({len(src_files)} source modules, {total} functions/classes, {all_tests} test functions)")
    if unused:
        print("DESC entries that matched nothing (name changed?):", ", ".join(unused))


if __name__ == "__main__":
    main()
