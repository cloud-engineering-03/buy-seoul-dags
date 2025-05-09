from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.etl_tasks import fetch_raw_data, insert_data


with DAG(
    "seoul_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    t1 = PythonOperator(task_id="fetch_raw_data",
                        python_callable=fetch_raw_data)
    t3 = PythonOperator(task_id="insert_data",
                        python_callable=insert_data)
    t1 >> t3
