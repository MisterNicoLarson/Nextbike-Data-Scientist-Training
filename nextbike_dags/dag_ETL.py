import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"

sys.path.insert(0, str(SRC_PATH))

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.ETL_Nextbike.extract import extract_json_from_api_nextbike
from src.ETL_Nextbike.transform import transform_nextbike_raw
from src.ETL_Nextbike.load import load_nextbike_processed
from src.DB_Nextbike.postgresql_db import init_db  

default_args = {
    "owner": "sia",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="nextbike_etl",
    schedule="*/5 * * * *",
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    default_args= default_args,
    catchup=False,
    tags=["etl","nextbike"]
) as dag:

    init_db_task = PythonOperator(
        task_id="init_db",
        python_callable=init_db,
    )

    extract = PythonOperator(
    task_id="extract",
    python_callable=extract_json_from_api_nextbike
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_nextbike_raw,
        op_kwargs={"raw_data": "{{ ti.xcom_pull(task_ids='extract') }}"},
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_nextbike_processed,
        op_kwargs={"processed_file": "{{ ti.xcom_pull(task_ids='transform') }}"},
    )

    init_db_task >> extract >> transform_task >> load