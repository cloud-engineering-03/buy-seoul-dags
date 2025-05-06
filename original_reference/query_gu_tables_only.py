
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from query_gu import query_gu_table

with DAG(
    dag_id="query_gu_tables_only",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    query_task = PythonOperator(
        task_id="query_gu_tables",
        python_callable=query_gu_table
    )
