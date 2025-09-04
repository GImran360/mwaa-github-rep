from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_requests_and_connectivity():
    import requests
    import logging

    logger = logging.getLogger("airflow.task")

    # ✅ Check requests version
    logger.info(f"Requests version: {requests.__version__}")

    # Fakestore API endpoints
    endpoints = {
        "products": "https://fakestoreapi.com/products",
        "carts": "https://fakestoreapi.com/carts",
        "users": "https://fakestoreapi.com/users",
    }

    # Test connectivity
    for name, url in endpoints.items():
        try:
            response = requests.get(url, timeout=10)
            logger.info(f"{name} endpoint response status: {response.status_code}")
            # Optionally, log first record
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    logger.info(f"First {name} record: {data[0]}")
        except Exception as e:
            logger.error(f"Failed to connect to {name}: {e}")

# --------------------------
# DAG DEFINITION
# --------------------------
with DAG(
    dag_id="test_requests_connectivity_dag",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["test", "requests", "api"]
) as dag:

    test_task = PythonOperator(
        task_id="test_requests_connectivity",
        python_callable=test_requests_and_connectivity
    )
