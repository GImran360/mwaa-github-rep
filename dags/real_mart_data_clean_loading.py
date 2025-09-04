
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
import datetime
import boto3

# --------------------------
# HOLIDAYS
# --------------------------
HOLIDAYS = [
    datetime.date(2025, 1, 1),
    datetime.date(2025, 12, 25),
    datetime.date(2025, 5, 23),
]

# --------------------------
# DEFAULT ARGS
# --------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'email': ['gandooriimran360@gmail.com']
}

# --------------------------
# FUNCTIONS
# --------------------------
def check_holiday(**kwargs):
    today = datetime.date.today()
    if today in HOLIDAYS:
        return 'skip_task'
    return 'wait_for_fakestore_ingestion'

def run_glue_job(job_name, region_name='ap-southeast-2', **kwargs):
    client = boto3.client('glue', region_name=region_name)
    response = client.start_job_run(JobName=job_name)
    print(f"Started Glue job {job_name}: {response}")
    return response['JobRunId']

# --------------------------
# DAG DEFINITION
# --------------------------
with DAG(
    dag_id="fakestore_glue_processing_dag",
    default_args=default_args,
    description="Trigger Glue jobs after fakestore_ingestion_requests_dag",
    schedule_interval=None,  # Triggered by sensor
    start_date=days_ago(1),
    catchup=False,
    tags=["fakestore", "glue", "s3"]
) as dag:

    # Branch based on holiday
    holiday_check = BranchPythonOperator(
        task_id='holiday_check',
        python_callable=check_holiday
    )

    skip_task = EmptyOperator(task_id='skip_task')

    # Wait for first DAG to complete
    wait_for_fakestore_ingestion = ExternalTaskSensor(
        task_id='wait_for_fakestore_ingestion',
        external_dag_id='fakestore_ingestion_requests_dag',
        external_task_id=None,  # Wait for entire DAG
        poke_interval=60,
        timeout=3600,
        mode='poke'
    )

    # Glue jobs sequentially
    glue_user_extract = PythonOperator(
        task_id='glue_user_extract',
        python_callable=run_glue_job,
        op_args=['RealMart-user_data_extract']
    )

    glue_product_data = PythonOperator(
        task_id='glue_product_data',
        python_callable=run_glue_job,
        op_args=['Realmart-product_data_store']
    )

    glue_cart_clean = PythonOperator(
        task_id='glue_cart_clean',
        python_callable=run_glue_job,
        op_args=['Realmart-cart_clean_data_Store']
    )

    # Email notification
    email_success = EmailOperator(
        task_id='email_success',
        to='gandooriimran360@gmail.com',
        subject='Fakestore Glue DAG Success',
        html_content='Fakestore Glue processing DAG has completed successfully!',
    )

    # DAG flow
    holiday_check >> [skip_task, wait_for_fakestore_ingestion]
    wait_for_fakestore_ingestion >> glue_user_extract >> glue_product_data >> glue_cart_clean >> email_success
    skip_task >> email_success
