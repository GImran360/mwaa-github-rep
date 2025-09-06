from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='s3_trigger_glue_airplane_job',
    default_args=default_args,
    description='Trigger Glue job when new file arrives in S3',
    schedule_interval=None,
    start_date=datetime(2025, 9, 6),
    catchup=False,
    max_active_runs=1,
    tags=['glue', 's3', 'airplane']
)

# S3 Sensor
wait_for_new_file = S3KeySensor(
    task_id='wait_for_new_file',
    bucket_key='raw_data/to_processed/*',
    bucket_name='airplane-sensors-data',
    wildcard_match=True,
    poke_interval=60,
    timeout=60 * 60,
    dag=dag
)

# Glue Job Operator
run_glue_job = GlueJobOperator(
    task_id='run_airplane_glue_job',
    job_name='airplane_raw_to_processed_analytics_job',
    script_location='s3://aws-glue-assets-258208867389-ap-southeast-2/scripts/airplane_raw_to_processed_analytics_job.py',
    iam_role_name='arn:aws:iam::258208867389:role/imrang-all-iam-role',
    region_name='ap-southeast-2',
    num_of_dpus=10,
    dag=dag
)

wait_for_new_file >> run_glue_job
