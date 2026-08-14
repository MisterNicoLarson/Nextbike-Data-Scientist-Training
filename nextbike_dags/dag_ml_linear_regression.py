import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"

sys.path.insert(0, str(SRC_PATH))

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.ML_Linear_Regression_Nextibke.linear_regression import run_ml_prediction

default_args = {
    "owner": "nicolas",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="nextbike_ml_linear_regression",
    schedule="*/15 * * * *",
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    catchup=False,
    tags=["ml", "nextbike", "linear-regression"],
) as dag:

    predict_task = PythonOperator(
        task_id="predict_depletion",
        python_callable=run_ml_prediction,
    )