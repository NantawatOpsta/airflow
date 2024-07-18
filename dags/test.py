from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


default_args = {
    'owner': 'LDB',
}


def echo_datetime():
    current_datetime = datetime.now()
    # This will print the current datetime to the Airflow logs
    print(f"The current datetime is: {current_datetime}")


with DAG(
    'test',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    default_args=default_args,
) as dag:
    task_echo_datetime = PythonOperator(
        task_id='echo_datetime',
        python_callable=echo_datetime
    )
