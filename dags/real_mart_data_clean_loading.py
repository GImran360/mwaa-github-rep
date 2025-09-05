from airflow import DAG 
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

default_args={
    "owener":"airflow",
    "depends_on_past":False,
    "retries":1,
}
with DAG(
    dag_id="realmart_glue_job",
    default_args=default_args,
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
) as dag:
    run_glue_job=GlueJobOperator(
        task_id="carts_run_glue_job",
        job_name="glue_user_extract",
        script_location="s3://aws-glue-assets-258208867389-ap-southeast-2/scripts/Real-mart-user_data_extract.py",
        region_name="ap-southeast-2",
        num_of_dpus=2,
    )
    run_glue_job
    
