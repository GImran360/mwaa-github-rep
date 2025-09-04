from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
import time

# --------------------------
# CONFIGURATION
# --------------------------
ENDPOINTS = {
    "products": "https://fakestoreapi.com/products",
    "carts": "https://fakestoreapi.com/carts",
    "users": "https://fakestoreapi.com/users",
}

BUCKET_NAME = "realmart-backbone"
RAW_PREFIX = "raw_data/to_processed"
MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds

# --------------------------
# LOGGING SETUP
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger()

# --------------------------
# FUNCTION TO FETCH DATA & UPLOAD TO S3
# --------------------------
def fetch_and_upload(dataset_name, api_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/117.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"[Attempt {attempt}] Fetching {dataset_name} from {api_url}")
            response = requests.get(api_url, headers=headers, timeout=30)
            
            if response.status_code == 403:
                raise Exception(f"403 Forbidden - likely blocked by API")
            elif response.status_code != 200:
                raise Exception(f"API returned status {response.status_code}")

            data = response.json()
            if not isinstance(data, list):
                raise ValueError(f"{dataset_name} API did not return a list")

            logger.info(f"Fetched {len(data)} records for {dataset_name}")

            # Convert to CSV
            df = pd.json_normalize(data)
            csv_buffer = df.to_csv(index=False)

            # Upload to S3
            now = datetime.utcnow()
            file_name = f"{dataset_name}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
            s3_key = f"{RAW_PREFIX}/{dataset_name}/{file_name}"

            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=csv_buffer,
                ContentType="text/csv"
            )
            logger.info(f"Uploaded {dataset_name} CSV to s3://{BUCKET_NAME}/{s3_key}")
            break  # success, exit retry loop

        except (requests.RequestException, ClientError, NoCredentialsError, EndpointConnectionError, Exception) as e:
            logger.warning(f"Request error for {dataset_name}: {e}")
            if attempt < MAX_RETRIES:
                sleep_time = RETRY_DELAY * attempt
                logger.info(f"Retrying after {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Max retries reached for {dataset_name}. Skipping.")
                break

# --------------------------
# DEFAULT ARGS
# --------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,  # retries are handled inside the function
    "start_date": datetime(2025, 9, 1),
    "catchup": False
}

# --------------------------
# DAG DEFINITION
# --------------------------
with DAG(
    dag_id="fakestore_ingestion_s3_csv_dag",
    default_args=default_args,
    description="Fetch Fakestore API JSON, convert to CSV, and upload to S3 with robust retry",
    schedule_interval="0 1 * * *",
    tags=["fakestore", "s3", "ingestion"],
) as dag:

    for dataset_name, api_url in ENDPOINTS.items():
        PythonOperator(
            task_id=f"ingest_{dataset_name}",
            python_callable=fetch_and_upload,
            op_args=[dataset_name, api_url],
        )
