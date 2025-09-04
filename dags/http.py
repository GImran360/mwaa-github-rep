from airflow.operators.bash import BashOperator

test_connection = BashOperator(
    task_id='test_fakestore_connect',
    bash_command="curl -I https://fakestoreapi.com/products",
    dag=dag
)
