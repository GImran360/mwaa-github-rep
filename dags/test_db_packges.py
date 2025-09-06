from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_db_packages():
    results = {}
    
    # Test PyMySQL
    try:
        import pymysql
        results['pymysql'] = "PyMySQL imported successfully!"
    except ImportError as e:
        results['pymysql'] = f"PyMySQL import failed: {e}"

    # Test psycopg2 (PostgreSQL)
    try:
        import psycopg2
        results['psycopg2'] = "psycopg2 imported successfully!"
    except ImportError as e:
        results['psycopg2'] = f"psycopg2 import failed: {e}"

    # Print results to Airflow logs
    for pkg, msg in results.items():
        print(f"{pkg}: {msg}")

with DAG(
    dag_id='test_db_packages',
    start_date=datetime(2025, 9, 6),
    schedule_interval=None,
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id='check_db_packages',
        python_callable=test_db_packages
    )
