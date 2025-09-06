from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import pymysql
import boto3
import csv
from io import StringIO
import logging


def export_mysql_table_to_s3(**kwargs):
    """
    Export a MySQL table to S3 as CSV.
    Table name, S3 bucket, and key come from Airflow Variables.
    """

    # 🔹 Get variables
    table_name = Variable.get("table_name")
    s3_bucket = Variable.get("s3_bucket_mysql")
    s3_key = Variable.get("s3_key")

    # 🔹 Get MySQL connection
    mysql_conn = BaseHook.get_connection("mysql_conn")
    conn = pymysql.connect(
        host=mysql_conn.host,
        user=mysql_conn.login,
        password=mysql_conn.password,
        database=mysql_conn.schema,
        port=mysql_conn.port or 3306,
    )

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]

        # 🔹 Write CSV to memory
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(headers)
        writer.writerows(rows)

        # 🔹 Upload to S3
        s3 = boto3.client("s3")
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

        logging.info(f"✅ Exported {len(rows)} rows from {table_name} to s3://{s3_bucket}/{s3_key}")

    finally:
        conn.close()


# -----------------------------
# DAG Definition
# -----------------------------
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@company.com"],  # change this
    "retries": 1,
}

with DAG(
    dag_id="mysql_to_s3_with_vars",
    default_args=default_args,
    description="Export MySQL tables to S3 using Airflow Variables + Connections",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["mysql", "s3"],
) as dag:

    export_table = PythonOperator(
        task_id="export_mysql_table",
        python_callable=export_mysql_table_to_s3,
        provide_context=True,
    )

    export_table
