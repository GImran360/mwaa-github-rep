from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import urllib3
import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

# --------------------------
# CONFIGURATION
# --------------------------
USERNAME = "String"  # Replace with your FakeStore username
PASSWORD = "String"  # Replace with your FakeStore password

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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger()

http = urllib3.PoolManager()

# --------------------------
# FETCH DATA FROM API FUNCTION
# --------------------------
def fetch_and_upload(dataset_name, api_url, username=USERNAME, password=PASSWORD):
    try:
        logger.info(f"Authenticating to FakeStore API for {dataset_name}")
        # Step 1: Authenticate
        auth_payload = json.dumps({"username": username, "password": password})
        auth_response = http.request(
            "GET",
            "https://fakestoreapi.com/auth/login",
            body=auth_payload,
            headers={"Content-Type": "application/json"}
        )

        if auth_response.status != 200:
            raise Exception(f"Authentication failed with status {auth_response.status}")

        token = json.loads(auth_response.data.decode("utf-8"))["token"]

        # Step 2: GET data with token
        logger.info(f"Fetching {dataset_name} from {api_url}")
        response = http.request(
            "GET",
            api_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (compatible; Airflow DAG)"
            }
        )

        if response.status != 200:
            raise Exception(f"API returned status {response.status} for {dataset_name}")

        data = json.loads(response.data.decode("utf-8"))

        if not isinstance(data, list):
            raise ValueError(f"{dataset_name} API did not return a list")

        logger.info(f"Fetched {len(data)} {dataset_name} records")

        # Step 3: Upload to S3
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
# DEFAULT ARGS
# --------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 9, 1),
    "catchup": False,
}

# --------------------------
# DAG DEFINITION
# --------------------------
with DAG(
    dag_id="fakestore_ingestion_dag",
    default_args=default_args,
    description="Ingest products, carts, and users from FakeStore API into S3",
    schedule_interval=None,  # Manual trigger
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

    # Parallel execution (no dependencies)
    tasks
