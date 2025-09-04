import urllib3
import json
import logging
import boto3
from datetime import datetime

logger = logging.getLogger()
http = urllib3.PoolManager()

BUCKET_NAME = "realmart-backbone"
RAW_PREFIX = "raw_data/to_processed"

def fetch_and_upload(dataset_name, api_url, username, password):
    try:
        logger.info(f"Fetching {dataset_name} from {api_url}")

        # Auth payload as required by API
        auth_payload = {
            "username": "String",
            "password": "String"
        }

        response = http.request(
            "POST",  # POST because we are sending JSON auth
            api_url,
            body=json.dumps(auth_payload),
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; Airflow DAG)",
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
        )

        if response.status != 200:
            raise Exception(f"API returned status {response.status} for {dataset_name}")

        data = json.loads(response.data.decode("utf-8"))
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
