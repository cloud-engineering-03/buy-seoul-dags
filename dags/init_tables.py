
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.init_tables import init_tables
from tasks.insert_district_data import insert_district_data
from tasks.insert_station_data import insert_station_data

with DAG(
    dag_id="init_tables",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    init_task = PythonOperator(
        task_id="init_tables",
        python_callable=init_tables
    )

    insert_district_task = PythonOperator(
        task_id="insert_district_data",
        python_callable=insert_district_data
    )

    insert_station_task = PythonOperator(
        task_id="insert_station_data",
        python_callable=insert_station_data
    )

    init_task >> insert_district_task >> insert_station_task
