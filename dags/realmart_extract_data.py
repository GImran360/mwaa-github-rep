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

        # Step 1: Authenticate
        auth_url = "https://fakestoreapi.com/auth/login"
        auth_payload = json.dumps({
            "username": 'String',
            "password": 'String'
        })
        auth_response = http.request(
            "POST",
            auth_url,
            body=auth_payload,
            headers={"Content-Type": "application/json"}
        )

        if auth_response.status != 200:
            raise Exception(f"Auth failed with status {auth_response.status}")

        token = json.loads(auth_response.data.decode("utf-8"))["token"]

        # Step 2: GET data with token
        response = http.request(
            "GET",
            api_url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; Airflow DAG)",
                "Accept": "application/json",
                "Authorization": f"Bearer {token}"
            }
        )

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
