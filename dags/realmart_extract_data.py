from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests
import json
import logging
import boto3

# --------------------------
# CONFIG
# --------------------------
ENDPOINTS = {
    "products": "https://fakestoreapi.com/products",
    "carts": "https://fakestoreapi.com/carts",
    "users": "https://fakestoreapi.com/users",
}

BUCKET_NAME = "realmart-backbone"
RAW_PREFIX = "raw_data/to_processed"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 9, 1),
    "catchup": False
}

# --------------------------
# FUNCTION TO FETCH & UPLOAD
# --------------------------
def fetch_and_upload(dataset_name, api_url, **kwargs):
    try:
        logger.info(f"Fetching {dataset_name} from {api_url}")
        response = requests.get(api_url, headers={"User-Agent": "Airflow DAG", "Accept": "application/json"})
        if response.status_code != 200:
            logger.warning(f"Request error for {dataset_name}: {response.status_code} {response.reason}")
            return  # gracefully handle error without failing DAG

        data = response.json()
        if not isinstance(data, list):
            logger.warning(f"{dataset_name} API did not return a list")
            return

        now = datetime.utcnow()
        file_name = f"{dataset_name}_{now.strftime('%Y%m%d_%H%M%S')}.json"
        s3_key = f"{RAW_PREFIX}/{dataset_name}/{file_name}"

        s3 = boto3.client("s3")
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=json.dumps(data, indent=2), ContentType="application/json")
        logger.info(f"Uploaded {dataset_name} data to s3://{BUCKET_NAME}/{s3_key}")

    except Exception as e:
        logger.error(f"Error processing {dataset_name}: {e}")

# --------------------------
# DAG DEFINITION
# --------------------------
with DAG(
    dag_id="fakestore_ingestion_requests_dag",
    default_args=default_args,
    description="Ingest products, carts, and users from Fakestore API into S3 using requests",
    schedule_interval="0 1 * * *",
    tags=["fakestore", "s3", "ingestion"],
) as dag:

    tasks = []
    for dataset_name, api_url in ENDPOINTS.items():
        task = PythonOperator(
            task_id=f"ingest_{dataset_name}",
            python_callable=fetch_and_upload,
            op_args=[dataset_name, api_url]
        )
        tasks.append(task)

    # Trigger next DAG automatically after all datasets are ingested
    trigger_glue_dag = TriggerDagRunOperator(
        task_id="trigger_glue_dag",
        trigger_dag_id="fakestore_glue_processing_dag",  # replace with your second DAG id
        wait_for_completion=False
    )

    # Set dependencies
    tasks >> trigger_glue_dag
