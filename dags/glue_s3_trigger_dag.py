from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# DAG definition
with DAG(
    dag_id="glue_s3_trigger_ncr_dag",
    default_args=default_args,
    description="Trigger Glue job when new file arrives in S3",
    schedule_interval=None,   # manual or external trigger (sensor will wait)
    start_date=days_ago(1),
    catchup=False,
    tags=["glue", "s3", "etl"],
) as dag:

    # 1. Wait for a new file in S3
    wait_for_s3_file = S3KeySensor(
        task_id="wait_for_new_file",
        bucket_key="data/*",   # look inside data/ folder
        bucket_name="my-s3backbone",
        wildcard_match=True,
        aws_conn_id="aws_default",  # Airflow AWS connection
        poke_interval=60,  # check every 60 seconds
        timeout=60*60      # timeout after 1 hour
    )

    # 2. Run the Glue Job
    run_glue_job = GlueJobOperator(
        task_id="run_glue_ncr_ride_bookings",
        job_name="ncr_ride_bookings_analytics",  # Glue job name
        script_location="s3://aws-glue-assets-258208867389-ap-southeast-2/scripts/ncr_ride_bookings_analytics.py",
        iam_role_name="arn:aws:iam::258208867389:role/imrang-all-iam-role",
        region_name="ap-southeast-2",
        #num_of_dpus=2,
        create_job_kwargs={
            "GlueVersion": "3.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2
        }
    )

    # Dependency
    wait_for_s3_file >> run_glue_job
