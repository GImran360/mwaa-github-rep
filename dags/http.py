from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="mwaa_connectivity_test",
    start_date=datetime(2025, 9, 4),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:

    test_fakestore_connect = BashOperator(
        task_id='test_fakestore_connect',
        bash_command="curl -I https://fakestoreapi.com/products",
    )
