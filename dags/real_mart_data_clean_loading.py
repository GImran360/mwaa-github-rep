from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
import datetime

# Define holidays
HOLIDAYS = [
    datetime.date(2025, 1, 1),
    datetime.date(2025, 12, 25),
    datetime.date(2025, 5, 23),
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'email': ['your-email@example.com']
}

def check_holiday(**kwargs):
    today = datetime.date.today()
    if today in HOLIDAYS:
        return 'skip_task'
    return 'wait_for_mart_extract'

with DAG(
    dag_id="fakestore_ingestion_dag",
    default_args=default_args,
    description="Ingest RealmArt data into S3 using Glue",
    schedule_interval="0 1 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["fakestore", "s3", "ingestion"]
) as dag:

    # Branching based on holiday
    holiday_check = BranchPythonOperator(
        task_id='holiday_check',
        python_callable=check_holiday
    )

    skip_task = EmptyOperator(
        task_id='skip_task'
    )

    # Wait for mart_extract DAG
    wait_for_mart_extract = ExternalTaskSensor(
        task_id='wait_for_mart_extract',
        external_dag_id='mart_extract',
        external_task_id=None,
        poke_interval=60,
        timeout=3600,
        mode='poke'
    )

    # Step 1: User data extraction
    glue_user_extract = AwsGlueJobOperator(
        task_id='glue_user_extract',
        job_name='RealMart-user_data_extract',
        iam_role_name='realmart-iam-role',
        region_name='ap-southeast-2',
        script_location='s3://aws-glue-assets-258208867389-ap-southeast-2/scripts/Real-mart-user_data_extract.py'
    )

    # Step 2: Product data processing
    glue_product_data = AwsGlueJobOperator(
        task_id='glue_product_data',
        job_name='Realmart-product_data_store',
        iam_role_name='realmart-iam-role',
        region_name='ap-southeast-2',
        script_location='s3://aws-glue-assets-258208867389-ap-southeast-2/scripts/Realmart-Store.py'
    )

    # Step 3: Cart clean data processing
    glue_cart_clean = AwsGlueJobOperator(
        task_id='glue_cart_clean',
        job_name='Realmart-cart_clean_data_Store',
        iam_role_name='realmart-iam-role',
        region_name='ap-southeast-2',
        script_location='s3://aws-glue-assets-258208867389-ap-southeast-2/scripts/Realmart-cart_clean_data_Store.py'
    )

    # Email on success
    email_success = EmailOperator(
        task_id='email_success',
        to='gandooriimran360@gmail.com',
        subject='Fakestore Ingestion DAG Success',
        html_content='Fakestore ingestion DAG has completed successfully!',
    )

    # DAG flow
    holiday_check >> [skip_task, wait_for_mart_extract]
    wait_for_mart_extract >> glue_user_extract >> glue_product_data >> glue_cart_clean >> email_success
    skip_task >> email_success
