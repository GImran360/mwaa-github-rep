from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_requests():
    import requests
    print("✅ Requests version:", requests._version_)

with DAG(
    dag_id="test_requests_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id="check_requests",
        python_callable=test_requests
    )
