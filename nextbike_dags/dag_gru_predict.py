import logging
import sys
from datetime import datetime, timedelta, UTC
from pathlib import Path

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import text

AIRFLOW_SRC = Path("/opt/airflow/src")
sys.path.insert(0, str(AIRFLOW_SRC))

from DB_Nextbike.postgresql_db import engine
from DB_Nextbike.models_db import TaskAlertML


logger = logging.getLogger(__name__)

GRU_SERVICE_URL = "http://ml-gru:8001"
MODEL_NAME = "gru"

default_args = {
    "owner": "nicolas",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def get_active_station_ids():
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT station_id
                FROM station
                WHERE is_active = true
                """
            )
        ).fetchall()

    return [row[0] for row in rows]


def run_gru_predictions():
    station_ids = get_active_station_ids()
    computed_at = datetime.now(UTC)
    inserted = 0

    with engine.begin() as conn:
        for station_id in station_ids:
            try:
                response = requests.get(
                    f"{GRU_SERVICE_URL}/predict/{station_id}",
                    timeout=30,
                )
                response.raise_for_status()
                result = response.json()

            except requests.exceptions.RequestException:
                logger.exception(
                    "GRU service call failed for station_id=%s",
                    station_id,
                )
                continue

            if result.get("minutes_remaining") is None:
                continue

            conn.execute(
                TaskAlertML.__table__.insert().values(
                    computed_at=computed_at,
                    station_id=station_id,
                    model=MODEL_NAME,
                    estimated_empty_at=result.get("estimated_empty_at"),
                    minutes_remaining=result.get("minutes_remaining"),
                    slope=None,
                    r2=None,
                    message=result.get("message"),
                )
            )

            inserted += 1

    logger.info("Inserted %d GRU predictions", inserted)
    return inserted


with DAG(
    dag_id="nextbike_gru_predict",
    schedule="*/15 * * * *",
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    catchup=False,
    tags=["ml", "nextbike", "gru", "prediction"],
) as dag:

    predict_task = PythonOperator(
        task_id="predict_gru",
        python_callable=run_gru_predictions,
    )