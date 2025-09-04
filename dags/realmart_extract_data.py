from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import urllib3
import logging
import boto3

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

# --------------------------
# LOGGING SETUP
# --------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger()

http = urllib3.PoolManager()

# Browser-like headers to avoid Cloudflare 403
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/117.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# Optional Fakestore API login (only needed if you want auth token)
USERNAME = "your_username"
PASSWORD = "your_password"


def get_auth_token():
    auth_url = "https://fakestoreapi.com/auth/login"
    auth_data = json.dumps({"username": USERNAME, "password": PASSWORD})

    logger.info("Authenticating to Fakestore API")
    auth_resp = http.request(
        "POST",
        auth_url,
        body=auth_data,
        headers={**HEADERS, "Content-Type": "application/json"}
    )

    if auth_resp.status != 200:
        raise Exception(f"Authentication failed with status {auth_resp.status}")

    token = json.loads(auth_resp.data.decode("utf-8"))["token"]
    logger.info("Authentication successful, token obtained")
    return token


# --------------------------
# FETCH DATA FROM API
# --------------------------
def fetch_and_upload(dataset_name, api_url):
    try:
        logger.info(f"Fetching {dataset_name} from {api_url}")

        # Optional: add Authorization header if using token
        # token = get_auth_token()
        # headers = {**HEADERS, "Authorization": f"Bearer {token}"}
        headers = HEADERS

        response = http.request("GET", api_url, headers=headers)
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

    except Exception as e:
        logger.error(f"Error processing {dataset_name}: {e}")
        raise


# --------------------------
# DEFAULT ARGS
# --------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 9, 1),
    "catchup": False
}

# --------------------------
# DAG DEFINITION
# --------------------------
with DAG(
    dag_id="fakestore_ingestion_dag",
    default_args=default_args,
    description="Ingest products, carts, and users from Fakestore API into S3",
    schedule_interval="0 1 * * *",  # run daily at 1 AM
    tags=["fakestore", "s3", "ingestion"],
) as dag:

    tasks = []
    for dataset_name, api_url in ENDPOINTS.items():
        task = PythonOperator(
            task_id=f"ingest_{dataset_name}",
            python_callable=fetch_and_upload,
            op_args=[dataset_name, api_url],
        )
        tasks.append(task)

    # Parallel execution
    tasks
