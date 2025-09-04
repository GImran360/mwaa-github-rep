from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import boto3
import datetime
import logging
from botocore.exceptions import ClientError

# --------------------------
# CONFIGURATION
# --------------------------
SCRIPTS = {
    "glue_user_extract": "s3://aws-glue-assets-258208867389-ap-southeast-2/scripts/Real-mart-user_data_extract.py",
    "glue_product_data": "s3://aws-glue-assets-258208867389-ap-southeast-2/scripts/Realmart-Store.py",
    "glue_cart_clean": "s3://aws-glue-assets-258208867389-ap-southeast-2/scripts/Realmart-cart_clean_data_Store.py"
}

REGION_NAME = "ap-southeast-2"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def run_glue_job_from_s3(task_name, s3_script_path, **kwargs):
    """
    Creates (if not exists) and starts a Glue job based on S3 script location.
    """
    client = boto3.client("glue", region_name=REGION_NAME)

    # Generate a Glue job name dynamically
    glue_job_name = task_name

    # Check if job exists
    try:
        client.get_job(JobName=glue_job_name)
        logger.info(f"Glue job {glue_job_name} exists, starting it...")
    except client.exceptions.EntityNotFoundException:
        logger.info(f"Glue job {glue_job_name} does not exist, creating it...")
        # Create job dynamically
        client.create_job(
            Name=glue_job_name,
            Role="AWSGlueServiceRole",  # Replace with your Glue IAM role
            Command={
                "Name": "glueetl",
                "ScriptLocation": s3_script_path,
                "PythonVersion": "3"
            },
            MaxRetries=0,
            GlueVersion="3.0",
            NumberOfWorkers=2,
            WorkerType="Standard"
        )
        logger.info(f"Created Glue job {glue_job_name}")

    # Start Glue job
    response = client.start_job_run(JobName=glue_job_name)
    logger.info(f"Started Glue job {glue_job_name}, JobRunId: {response['JobRunId']}")
    return response['JobRunId']


with DAG(
    dag_id="fakestore_glue_processing_dag",
    default_args=default_args,
    description="Run Glue jobs from S3 scripts",
    schedule_interval=None,
    start_date=days_ago(0),
    catchup=False
) as dag:

    tasks = []
    for task_name, s3_script in SCRIPTS.items():
        t = PythonOperator(
            task_id=task_name,
            python_callable=run_glue_job_from_s3,
            op_args=[task_name, s3_script]
        )
        tasks.append(t)

    # Optional linear flow
    for i in range(len(tasks)-1):
        tasks[i] >> tasks[i+1]

