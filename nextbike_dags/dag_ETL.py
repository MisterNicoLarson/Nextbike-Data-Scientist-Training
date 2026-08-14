import sys
from pathlib import Path
import json

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"

sys.path.insert(0, str(SRC_PATH))

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from ETL_Nextbike.extract import extract_json_from_api_nextbike
from ETL_Nextbike.transform import transform_nextbike_raw
from ETL_Nextbike.load import load_nextbike_processed
from DB_Nextbike.postgresql_db import init_db

default_args = {
    "owner": "nicolas",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

def transform_wrapper(**context):
    raw_file = context["ti"].xcom_pull(task_ids="extract")

    with open(raw_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    return transform_nextbike_raw(raw_data)

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

    ##### transform_task ne passe plus raw_data via un template Jinja op_kwargs #####
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_wrapper,
    )
    ##### fin correction #####

    load = PythonOperator(
        task_id="load",
        python_callable=load_nextbike_processed,
        op_kwargs={"processed_file": "{{ ti.xcom_pull(task_ids='transform') }}"},
    )

    init_db_task >> extract >> transform_task >> load