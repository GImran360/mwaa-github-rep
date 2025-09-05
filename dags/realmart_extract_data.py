from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests
import json
import logging
import boto3
import time
import random

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
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "start_date": datetime(2025, 9, 1),
    "catchup": False
}

# --------------------------
# FUNCTION TO FETCH & UPLOAD
# --------------------------
def fetch_and_upload(dataset_name, api_url, **kwargs):
    # Updated User-Agent to a more generic string that is less likely to be blocked.
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
        "Accept": "application/json",
        "Connection": "keep-alive"
    }

    for attempt in range(5):
        try:
            logger.info(f"[{dataset_name}] Attempt {attempt+1}: Fetching from {api_url}")
            response = requests.get(api_url, headers=headers, timeout=30)

            # Handle Forbidden explicitly
            if response.status_code == 403:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"[{dataset_name}] 403 Forbidden. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            data = response.json()
            if not isinstance(data, list):
                raise ValueError(f"[{dataset_name}] API did not return a list")

            now = datetime.utcnow()
            file_name = f"{dataset_name}_{now.strftime('%Y%m%d_%H%M%S')}.json"
            s3_key = f"{RAW_PREFIX}/{dataset_name}/{file_name}"

            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(data, indent=2),
                ContentType="application/json"
            )
            logger.info(f"[{dataset_name}] ✅ Uploaded {len(data)} records to s3://{BUCKET_NAME}/{s3_key}")
            return

        except Exception as e:
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            logger.error(f"[{dataset_name}] Error: {e}. Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)

    # If all retries fail, save error response to S3 for debugging
    error_key = f"{RAW_PREFIX}/errors/{dataset_name}_error_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=error_key,
        Body=json.dumps({"dataset": dataset_name, "error": "403 Forbidden or API failure"}, indent=2),
        ContentType="application/json"
    )
    logger.error(f"[{dataset_name}] ❌ Failed after retries. Error saved at s3://{BUCKET_NAME}/{error_key}")
    raise Exception(f"{dataset_name} ingestion failed permanently after retries")

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

    trigger_glue_dag = TriggerDagRunOperator(
        task_id="trigger_glue_dag",
        trigger_dag_id="fakestore_glue_processing_dag",
        wait_for_completion=False
    )

    tasks >> trigger_glue_dag
