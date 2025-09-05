from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator



default_args = {
    "owner": "imrang",  # 🔧 Fixed typo from "owener"
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 3),
}

with DAG(
    dag_id="spotify_trigger_external",
    default_args=default_args,
    description="DAG to trigger Lambda and check S3 upload",
    schedule_interval=timedelta(days=1),  # 🔧 Fixed typo from "schedue_interval"
    catchup=False,
) as dag:

    trigger_extract_lambda = LambdaInvokeFunctionOperator(
        task_id="trigger_extract_lambda",
        function_name="spotify_api_data_extract",  # 🔁 Make sure this Lambda name is correct
        aws_conn_id="aws_s3_spotify",              # ✅ Make sure this Airflow connection exists
        region_name="us-east-1",                   # ✅ Use the region where your Lambda is deployed
    )

    


    check_s3_upload=S3KeySensor(
        task_id="check_s3_upload",
        bucket_name="spotify-etl-project-imrang",  # ✅ Just the bucket name
        bucket_key="raw_data/to_processed/*",       # ✅ Key path (not full s3://...)
        wildcard_match=True,
        aws_conn_id="aws_s3_spotify",              # ✅ Correct param name is aws_conn_id
        timeout=60 * 60,        # 1 hour max wait time
        poke_interval=60,       # Check every 60 seconds
        mode='poke',            # Optional: can use 'reschedule' for more efficient usage
        dag=dag,
    
    
    )

    #trigger_transform_load=LambdaInvokeFunctionOperator(
     #   task_id="trigger_transform_load",
      #  function_name="spotify_transformation_load_function",
       # aws_conn_id="aws_s3_spotify",
        #region_name="us-east-1",
    #)
    trigger_glue_job=GlueJobOperator(
        task_id="trigger_glue_job",
        job_name="spotify_transformation_job",
        script_location="s3://aws-glue-assets-844787308866-us-east-1/scripts/spotify_transformation_job.py",
        aws_conn_id="aws_s3_spotify",
        region_name="us-east-1",
        iam_role_name="spotify_glue_iam_role",
        s3_bucket="aws-glue-assets-844787308866-us-east-1"

    )
    

#trigger_extract_lambda >> check_s3_upload >>trigger_transform_load

trigger_extract_lambda >> check_s3_upload >>trigger_glue_job

