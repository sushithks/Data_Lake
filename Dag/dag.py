from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
