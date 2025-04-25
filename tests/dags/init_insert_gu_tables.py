
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from init_insert import insert_init_data

with DAG(
    dag_id="init_insert_gu_tables",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    insert_task = PythonOperator(
        task_id="create_and_insert_gu_tables",
        python_callable=insert_init_data
    )
