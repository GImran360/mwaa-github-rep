from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import json
import logging

# --------------------------
# CONFIGURATION
# --------------------------
BUCKET_NAME = "realmart-backbone"
RAW_PREFIX = "raw_data/to_processed"
DATASETS = {
    "products": "https://fakestoreapi.com/products",
    "carts": "https://fakestoreapi.com/carts",
    "users": "https://fakestoreapi.com/users",
}

# --------------------------
# LOGGING SETUP
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger()

# --------------------------
# FUNCTION TO UPLOAD CSV TO S3
# --------------------------
def upload_csv_to_s3(file_path, dataset_name):
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        # Convert list of dicts to DataFrame
        df = pd.json_normalize(data)

        now = datetime.utcnow()
        file_name = f"{dataset_name}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        s3_key = f"{RAW_PREFIX}/{dataset_name}/{file_name}"

        # Save CSV to memory buffer
        csv_buffer = df.to_csv(index=False)

        # Upload to S3
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=csv_buffer,
            ContentType="text/csv"
        )
        logger.info(f"Uploaded {dataset_name} CSV to s3://{BUCKET_NAME}/{s3_key}")

    except Exception as e:
        logger.error(f"Failed to upload {dataset_name} CSV: {e}")
        raise

# --------------------------
# DEFAULT ARGS
# --------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 9, 1),
    "catchup": False
}

# --------------------------
# DAG DEFINITION
# --------------------------
with DAG(
    dag_id="fakestore_ingestion_s3_csv_dag",
    default_args=default_args,
    description="Fetch Fakestore JSON via curl and upload CSV to S3",
    schedule_interval="0 1 * * *",
    tags=["fakestore", "s3", "ingestion"],
) as dag:

    for dataset_name, url in DATASETS.items():
        # Download JSON
        download_task = BashOperator(
            task_id=f"download_{dataset_name}",
            bash_command=f"curl -L {url} -o /tmp/{dataset_name}.json"
        )

        # Upload CSV
        upload_task = PythonOperator(
            task_id=f"upload_{dataset_name}_csv_s3",
            python_callable=upload_csv_to_s3,
            op_args=[f"/tmp/{dataset_name}.json", dataset_name],
        )

        download_task >> upload_task
