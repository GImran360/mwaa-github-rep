from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def test_pandas():
    df = pd.DataFrame({"col": [1,2,3]})
    print(df)


with DAG(
    dag_id="test_pandas_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    PythonOperator(
        task_id="check_pandas",
        python_callable=test_pandas
    )
