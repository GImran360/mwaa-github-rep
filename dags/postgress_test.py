from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# DAG definition
with DAG(
    dag_id="postgres_client_demo",
    start_date=days_ago(1),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["postgres", "demo"],
) as dag:

    # Task 1: Create table
    create_table = PostgresOperator(
        task_id="create_client_table",
        postgres_conn_id="my_postgres_conn",  # Your Airflow Connection ID
        sql="""
        CREATE TABLE IF NOT EXISTS client (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Task 2: Insert dummy records
    insert_records = PostgresOperator(
        task_id="insert_dummy_clients",
        postgres_conn_id="my_postgres_conn",
        sql="""
        INSERT INTO client (name, email) VALUES
        ('Alice', 'alice@example.com'),
        ('Bob', 'bob@example.com'),
        ('Charlie', 'charlie@example.com')
        ON CONFLICT DO NOTHING;
        """,
    )

    # Task 3: Select records
    select_records = PostgresOperator(
        task_id="select_clients",
        postgres_conn_id="my_postgres_conn",
        sql="SELECT * FROM client;",
    )

    # DAG task order
    create_table >> insert_records >> select_records
