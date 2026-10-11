# Clean demo: fresh batches -> incremental run -> KPIs. Deletes data\incoming, state, output, quarantine.
$ErrorActionPreference = "Stop"
Remove-Item -Recurse -Force data\incoming, data\state, data\output, data\quarantine -ErrorAction SilentlyContinue
python -m finance_pipeline.batches --rows 1000 --seed 42 --batches 3
python -m finance_pipeline.incremental
if ($LASTEXITCODE -ne 0) { throw "pipeline failed with exit code $LASTEXITCODE" }
Get-Content data\output\kpis\totals.csv
