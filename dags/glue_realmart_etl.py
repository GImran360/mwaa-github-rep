from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

# Define the DAG
with DAG(
    dag_id="glue_realmart_etl_dag",
    default_args=default_args,
    description="Run AWS Glue ETL Job: realmart_etl_clean_analytics",
    schedule_interval=None,   # set cron (e.g., "0 2 * * *") if you want auto-schedule
    start_date=days_ago(1),
    catchup=False,
    tags=["glue", "etl", "realmart"],
) as dag:

    # Task: Run Glue Job
    run_glue_job = GlueJobOperator(
        task_id="run_realmart_glue_job",
        job_name="realmart_etl_clean_analytics",  # Glue job name in AWS
        script_location="s3://aws-glue-assets-258208867389-ap-southeast-2/scripts/realmart_etl_clean_analytics.py",
        iam_role_name="arn:aws:iam::258208867389:role/imrang-all-iam-role",
        region_name="ap-southeast-2",  # Sydney region
        num_of_dpus=2,
        create_job_kwargs={
            "GlueVersion": "3.0",   # or "4.0" depending on your Glue job version
            "WorkerType": "G.1X",   # change if you need bigger workers
            "NumberOfWorkers": 2
        }
    )

    run_glue_job
