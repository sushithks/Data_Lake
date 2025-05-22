from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from Main import input_data, output_data, create_spark_session, process_log_data

from Main import process_song_data

spark = create_spark_session()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'songs_de',
    default_args=default_args,
    description='Songs Data analysis',
    schedule_interval=timedelta(days=1)
)


dag_start = DummyOperator(
    task_id='dag_start',
    dag=dag)


song_data = PythonOperator(
    task_id='song_data',
    python_callable=process_song_data(),
    op_kwargs={'spark':spark, 'input_data':input_data, 'output_data': output_data},
    provide_context=True,
    dag=dag
)

log_data = PythonOperator(
    task_id='song_data',
    python_callable=process_log_data(),
    op_kwargs={'spark':spark, 'input_data':input_data, 'output_data': output_data},
    provide_context=True,
    dag=dag
)

dag_end = DummyOperator(
    task_id='dag_end',
    dag=dag)

dag_start.set_downstream(song_data)
song_data.set_downstream(log_data)
log_data.set_downstream(dag_end)