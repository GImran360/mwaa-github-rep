import json
import airflow
import requests
import requests.exceptions as requests_exceptions
import boto3
import os 
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Initialize boto3 S3 client
s3_client = boto3.client('s3')

BUCKET_NAME = 'rocket-launches-airflow'

dag = DAG(
    dag_id='download_rocket_launches',
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None
)

# 1️⃣ Download launches.json locally
download_launches = BashOperator(
    task_id='download_launches',
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)

# 2️⃣ Upload launches.json to S3
def _upload_launches_to_s3():
    s3_client.upload_file(
        Filename='/tmp/launches.json',
        Bucket=BUCKET_NAME,
        Key='download_rocket_launches/launches.json'
    )
    print("Uploaded launches.json to S3.")

upload_launches = PythonOperator(
    task_id='upload_launches',
    python_callable=_upload_launches_to_s3,
    dag=dag
)

# 3️⃣ Download images & upload to S3
def _get_pictures():
    # Removed: pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    # Assumes /tmp/images/ exists!
    local_path = "/tmp/images/pic.jpg"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]

        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                local_path = f"/tmp/images/{image_filename}"
                s3_key = f"get_download_pictures/{image_filename}"

                with open(local_path, "wb") as f_out:
                    f_out.write(response.content)

                s3_client.upload_file(
                    Filename=local_path,
                    Bucket=BUCKET_NAME,
                    Key=s3_key
                )

                print(f"Downloaded and uploaded {image_url} to s3://{BUCKET_NAME}/{s3_key}")

            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}")

get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

# 4️⃣ Notify
notify = BashOperator(
    task_id="notify",
    bash_command='echo "Check S3 bucket: rocket-launches-airflow for images."',
    dag=dag
)

# DAG order
download_launches >> upload_launches >> get_pictures >> notify
