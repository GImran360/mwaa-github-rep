import os
import json
import csv
import requests
import boto3
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

# Initialize boto3 S3 client
s3_client = boto3.client('s3')

# ✅ Just the bucket name (not s3://)
BUCKET_NAME = "airplane-sensors-data"

dag = DAG(
    dag_id="download_rocket_launches_csv",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
    catchup=False,
)

# 1️⃣ Download launches JSON → Convert → Save CSV → Upload CSV to S3
def _download_and_upload_csv():
    local_json = "/tmp/launches.json"
    local_csv = "/tmp/launches.csv"

    # Download JSON
    url = "https://ll.thespacedevs.com/2.0.0/launch/upcoming"
    response = requests.get(url, timeout=15)
    response.raise_for_status()

    with open(local_json, "w") as f:
        f.write(response.text)

    launches = response.json()["results"]

    # Convert JSON → CSV
    fieldnames = ["id", "name", "window_start", "window_end", "image"]
    with open(local_csv, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for launch in launches:
            writer.writerow({
                "id": launch.get("id"),
                "name": launch.get("name"),
                "window_start": launch.get("window_start"),
                "window_end": launch.get("window_end"),
                "image": launch.get("image"),
            })

    # Upload CSV only
    s3_client.upload_file(
        Filename=local_csv,
        Bucket=BUCKET_NAME,
        Key="rocket_launches/launches.csv",
    )

    print(f"Uploaded launches.csv to s3://{BUCKET_NAME}/rocket_launches/launches.csv")

upload_csv = PythonOperator(
    task_id="download_and_upload_csv",
    python_callable=_download_and_upload_csv,
    dag=dag,
)
