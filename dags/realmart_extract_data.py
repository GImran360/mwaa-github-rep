import csv
import io
import json
import urllib3
import logging
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError


# --------------------------
# CONFIGURATION (fixed values)
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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger()

http = urllib3.PoolManager()

# --------------------------
# FETCH DATA FROM API
# --------------------------
def fetch_and_upload(dataset_name, api_url):
    try:
        logger.info(f"Fetching {dataset_name} from {api_url}")
        response = http.request(
            "GET",
            api_url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; Airflow DAG)",
                "Accept": "application/json"
            }
        )
        if response.status != 200:
            raise Exception(f"API returned status {response.status} for {dataset_name}")

        data = json.loads(response.data.decode("utf-8"))

        if not isinstance(data, list):
            raise ValueError(f"{dataset_name} API did not return a list")

        logger.info(f"Fetched {len(data)} {dataset_name} records")

        # --------------------------
        # Convert JSON → CSV
        # --------------------------
        try:
            if len(data) == 0:
                raise ValueError(f"No data to save for {dataset_name}")

            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            csv_data = output.getvalue()
        except Exception as e:
            logger.error(f"Failed to convert {dataset_name} JSON to CSV: {e}")
            raise

        # --------------------------
        # Upload to S3
        # --------------------------
        now = datetime.utcnow()
        file_name = f"{dataset_name}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        s3_key = f"{RAW_PREFIX}/{dataset_name}/{file_name}"

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=csv_data,
            ContentType="text/csv"
        )
        logger.info(f"Uploaded {len(data)} {dataset_name} records to s3://{BUCKET_NAME}/{s3_key}")
        return s3_key

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

    tasks
