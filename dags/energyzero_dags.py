from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="energyzero_etl",
    default_args=default_args,
    description="Extract EnergyZero prices, transform to Parquet, validate contract",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # prevents overlapping runs
    dagrun_timeout=timedelta(minutes=30),
    tags=["energyzero", "etl"],
) as dag:
    # Use timestamp-based filenames so multiple manual triggers do not overwrite each other
    run_tag = "{{ run_id | replace(':','') | replace('+','') | replace('.','') }}"
    raw_path = f"/opt/airflow/data/raw/energy_{run_tag}.json"
    parquet_path = f"/opt/airflow/data/processed/energy_{run_tag}.parquet"

extract = BashOperator(
    task_id="extract_energyzero_json",
    bash_command=f"python /opt/airflow/scripts/extract_energyzero.py --days 7 --output {raw_path}",
)

transform = BashOperator(
    task_id="transform_json_to_parquet",
    bash_command=f"python /opt/airflow/scripts/transform_pandas.py --input {raw_path} --output {parquet_path} --vat-rate 0.21",
)

validate = BashOperator(
    task_id="validate_parquet_contract",
    bash_command=f"python /opt/airflow/scripts/validate_parquet.py --input {parquet_path}",
)

extract >> transform >> validate # type: ignore
