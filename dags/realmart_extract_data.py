from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import json
import logging
import boto3
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import os
import requests

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
    # Setup Chrome options for headless mode
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Path to the ChromeDriver executable
    # Assumes chromedriver is in the path or the current directory
    service = Service()
    
    driver = None
    data = None

    for attempt in range(5):
        try:
            logger.info(f"[{dataset_name}] Attempt {attempt+1}: Fetching from {api_url} using Selenium")
            
            # Initialize a new driver for each attempt to ensure a clean state
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.get(api_url)
            
            # The API returns a simple JSON, which is a part of the body.
            # We can get the text content directly.
            page_source = driver.page_source
            
            # The data is inside a <pre> tag. We need to extract it.
            pre_tag = driver.find_element_by_tag_name("pre")
            data_text = pre_tag.text

            # Parse the text as JSON
            data = json.loads(data_text)
            
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
            return # Success, exit the function

        except Exception as e:
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            logger.error(f"[{dataset_name}] Error: {e}. Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)
            
        finally:
            if driver:
                driver.quit() # Always close the driver to free up resources

    # If all retries fail, save error response to S3 for debugging
    error_key = f"{RAW_PREFIX}/errors/{dataset_name}_error_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=error_key,
        Body=json.dumps({"dataset": dataset_name, "error": "Permanent API failure with Selenium"}, indent=2),
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
    description="Ingest products, carts, and users from Fakestore API into S3 using Selenium",
    schedule_interval="0 1 * * *",
    tags=["fakestore", "s3", "ingestion", "selenium"],
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
