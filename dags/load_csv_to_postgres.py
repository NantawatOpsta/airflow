import csv

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin


def create_new_table(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_db')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS POSTGRES_TABLE")
    cursor.execute(
        "CREATE TABLE POSTGRES_TABLE (id SERIAL PRIMARY KEY, name VARCHAR(50), create_time TIMESTAMP)")
    connection.commit()
    cursor.close()
    connection.close()


def load_csv_to_postgres(**kwargs):
    logger = LoggingMixin().log
    postgres_hook = PostgresHook(postgres_conn_id="postgres_db")
    csv_file_path = "/tmp/airflow_files/user.csv"

    logger.info("Starting to load CSV data into PostgreSQL")

    with open(csv_file_path, "r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip the header row

        for row in csv_reader:
            id, name, create_time = row[0], row[1], row[2]
            postgres_hook.run(
                "INSERT INTO postgres_table (id, name, create_time) VALUES (%s, %s, %s) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, create_time = EXCLUDED.create_time",
                parameters=(id, name, create_time)
            )
            logger.info(
                f"Inserted/Updated row with id: {id}, name: {name}, create_time: {create_time}")


default_args = {
    'owner': 'LDB',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    'load_csv_to_postgres',
    default_args=default_args,
    description='A DAG to load CSV data into a PostgreSQL table',
    schedule_interval=timedelta(days=1),
    tags=['file'],
) as dag:

    create_postgres_table = PythonOperator(
        task_id='create_new_postgres_table',
        python_callable=create_new_table,
    )

    load_csv_to_postgres_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres,
    )

    create_postgres_table >> load_csv_to_postgres_task
