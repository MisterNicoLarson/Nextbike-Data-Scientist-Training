import logging
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)

GRU_SERVICE_URL = "http://ml-gru:8001"

default_args = {
    "owner": "sia",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

"""
    Triggers a full retraining of the GRU model via the ml-gru service.

    Returns:
        None
"""
def trigger_training():
    response = requests.post(f"{GRU_SERVICE_URL}/train", timeout=1800)
    response.raise_for_status()
    logger.info("GRU training result: %s", response.json())

with DAG(
    dag_id="nextbike_gru_train",
    schedule="0 3 * * *",
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    catchup=False,
    tags=["ml", "nextbike", "gru", "training"],
) as dag:

    train_task = PythonOperator(
        task_id="train_gru",
        python_callable=trigger_training,
    )