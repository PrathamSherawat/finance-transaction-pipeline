"""SAMPLE Airflow DAG: shows how the pipeline maps onto a scheduler.

Not run in CI and not a project dependency (Airflow is not in requirements.txt).
Run on a machine where the project is installed (pip install -e .).
Note: BashOperator retries on ANY non-zero exit. The pipeline's own orchestrator retries only
lock errors (exit 4); a scheduler setup should treat exit codes 1, 2 and 3 as non-retryable.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/finance-transaction-pipeline"

with DAG(
    dag_id="finance_pipeline_daily",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # the pipeline also takes a run.lock, this is a second guard
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    # Demo only: in production, batch files arrive in data/incoming from upstream.
    generate_batches = BashOperator(
        task_id="generate_batches",
        bash_command=f"cd {PROJECT_DIR} && python -m finance_pipeline.batches --rows 1000 --seed 42 --batches 3",
    )
    # Merges new batches, runs the 13 quality checks, then writes KPIs. Exit codes: 0/1/2/3/4.
    incremental = BashOperator(
        task_id="incremental",
        bash_command=f"cd {PROJECT_DIR} && fin-incremental",
    )
    generate_batches >> incremental
