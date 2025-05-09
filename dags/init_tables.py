
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.init_tables import init_tables
from tasks.insert_district_data import merge_all_data

with DAG(
    dag_id="init_tables",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    insert_task = PythonOperator(
        task_id="init_tables",
        python_callable=init_tables
    )
    merge_task = PythonOperator(
        task_id='merge_station_data',
        python_callable=merge_all_data,
    )
