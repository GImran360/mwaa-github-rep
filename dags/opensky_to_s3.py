import json
import requests
import boto3
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# S3 config

BUCKET_NAME = "airplane-sensors-data"
S3_PREFIX = "raw_data/to_processed/"

# Function to fetch OpenSky data
def fetch_opensky_data():
    url = "https://opensky-network.org/api/states/all"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

# Function to save data to S3

def save_to_s3():
    data = fetch_opensky_data()
    s3 = boto3.client("s3")

    # Create unique filename with timestamp
    file_name = f"opensky_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    s3_key = f"{S3_PREFIX}{file_name}"

    # Upload JSON string
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    print(f"✅ Uploaded file to s3://{BUCKET_NAME}/{s3_key}")

# Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# DAG definition
with DAG(
    dag_id="opensky_to_s3_dag",
    default_args=default_args,
    description="Fetch airplane data from OpenSky API and store in S3",
    schedule_interval="*/10 * * * *",  # runs every 10 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=["opensky", "s3", "etl"],
) as dag:

    fetch_and_save = PythonOperator(
        task_id="fetch_and_save_opensky",
        python_callable=save_to_s3
    )

    fetch_and_save
