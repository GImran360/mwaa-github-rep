from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a simple Python function to run
def hello_world():
    print("✅ Hello from Airflow!")
    
    print("hey I am new developer for Data pipeline")

# Create the DAG
with DAG(
    dag_id="test_hello_world_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Only run manually
    catchup=False
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world
    )

    hello_task
