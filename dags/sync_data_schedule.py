from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from dateutil.parser import parse
from datetime import timedelta


def sync_data():
    oracle_hook = OracleHook(oracle_conn_id='oracle_db')
    postgres_hook = PostgresHook(postgres_conn_id='postgres_db')

    # Fetch the last sync time, or use a default if it's the first run
    last_sync_time = Variable.get(
        "last_sync_time", default_var=str(days_ago(1)))

    # Use parse to handle datetime string with timezone information
    last_sync_time_dt = parse(last_sync_time)

    # Format last_sync_time to exclude fractional seconds
    last_sync_time_formatted = last_sync_time_dt.strftime("%Y-%m-%d %H:%M:%S")

    # Modify the SQL query to fetch only records newer than the last sync time
    sql = f"SELECT create_time, name FROM ORACLE_TABLE WHERE create_time > TO_TIMESTAMP('{
        last_sync_time_formatted}', 'YYYY-MM-DD HH24:MI:SS')"
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    if rows:
        # Define the target fields in your PostgreSQL table, excluding the 'id' field
        # Adjust these to match your table's schema, excluding the 'id' field
        target_fields = ['create_time', 'name']

        # Insert data into PostgreSQL, excluding the 'id' field
        postgres_hook.insert_rows(
            table="postgres_table", rows=rows, target_fields=target_fields)

        # Update the last sync time to the current time after a successful sync
        Variable.set("last_sync_time", str(datetime.now()))


default_args = {
    'owner': 'LDB',
    'depends_on_past': False,
    # Example: 10 minutes in the past
    'start_date': datetime.now() - timedelta(minutes=10),
    'retries': 0,
    'catchup': False,
    # mail when the task fails
}

with DAG('sync_data_dag', default_args=default_args, schedule_interval='*/1 * * * *') as dag:
    sync_task = PythonOperator(
        task_id='sync_data_task',
        python_callable=sync_data,
    )

    sync_task
