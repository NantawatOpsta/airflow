from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook

# Configure DAG parameters
default_args = {
    'owner': 'LDB',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 18),
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='query_all_from_oracle_to_postgres',
    default_args=default_args,
    schedule_interval=None  # Run manually or set a schedule
) as dag:

    def transfer_data_to_postgres(**kwargs):
        oracle_hook = OracleHook(oracle_conn_id='oracle_db')
        postgres_hook = PostgresHook(postgres_conn_id='postgres_db')

        # Assuming ORACLE_TABLE and POSTGRES_TABLE have the same schema
        sql = "SELECT create_time, name FROM ORACLE_TABLE"
        connection = oracle_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()

        # Define the target fields in your PostgreSQL table, excluding the 'id' field
        target_fields = ['create_time', 'name']  # Adjust these to match your table's schema, excluding the 'id' field

        # Insert data into PostgreSQL, excluding the 'id' field
        postgres_hook.insert_rows(table="postgres_table", rows=rows, target_fields=target_fields)
    
    # Transfer data to PostgreSQL
    transfer_data = PythonOperator(
        task_id='transfer_data_to_postgres',
        python_callable=transfer_data_to_postgres,
    )

    transfer_data