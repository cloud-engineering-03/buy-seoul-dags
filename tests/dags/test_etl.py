from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from my_tasks import fetch_raw_data, validate_data, insert_data, prune_old_data, t5_check_table, table_exist
with DAG("seoul_etl_pipeline", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    t0 = PythonOperator(task_id="table_exist", python_callable=table_exist)
    t1 = PythonOperator(task_id="fetch_raw_data", python_callable=fetch_raw_data)
    t2 = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t3 = PythonOperator(task_id="insert_data", python_callable=insert_data)
    t4 = PythonOperator(task_id="prune_old_data", python_callable=prune_old_data)
    t5 = PythonOperator(task_id="t5_check_table", python_callable=t5_check_table)
    t0 >> t1 >> t3 >> t4
