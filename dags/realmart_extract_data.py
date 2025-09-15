from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import urllib3
import logging
import boto3
import random
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

# --------------------------
# LOGGING SETUP
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger()
http = urllib3.PoolManager()

# --------------------------
# FETCH AND UPLOAD FUNCTION
# --------------------------
def fetch_and_upload(dataset_name, api_url):
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json",
            "Referer": "https://fakestoreapi.com/"
        }

        logger.info(f"Fetching {dataset_name} from {api_url} with headers {headers['User-Agent']}")
        response = http.request("GET", api_url, headers=headers, timeout=30)

        # Retry on 403
        if response.status == 403:
            logger.warning(f"403 Forbidden for {dataset_name}. Retrying with new User-Agent...")
            time.sleep(3)
            headers["User-Agent"] = random.choice(USER_AGENTS)
            response = http.request("GET", api_url, headers=headers, timeout=30)

        if response.status != 200:
            raise Exception(f"API returned status {response.status} for {dataset_name}")

        data = json.loads(response.data.decode("utf-8"))

        if not isinstance(data, list):
            raise ValueError(f"{dataset_name} API did not return a list")

        logger.info(f"Fetched {len(data)} {dataset_name} records")

        # Upload to S3
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

        logger.info(f"Uploaded {len(data)} {dataset_name} records to s3://{BUCKET_NAME}/{s3_key}")

    except (ClientError, NoCredentialsError, EndpointConnectionError, Exception) as e:
        logger.error(f"Error processing {dataset_name}: {e}")
        raise

# --------------------------
# DAG DEFINITION
# --------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_fakestore_datasets",
    default_args=default_args,
    description="Fetch multiple datasets from FakeStore API and upload to S3",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["api", "s3", "fakestore"],
) as dag:

    tasks = []
    for dataset_name, api_url in ENDPOINTS.items():
        task = PythonOperator(
            task_id=f"fetch_{dataset_name}",
            python_callable=lambda ds_name=dataset_name, url=api_url: fetch_and_upload(ds_name, url),
        )
        tasks.append(task)

    # Optionally chain tasks or run in parallel
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
