from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'LDB',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 18),
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='insert_data_to_oracle',
    default_args=default_args,
    schedule_interval=None
) as dag:

    # insert data into oracle table
    def insert_data_into_table(**kwargs):
        oracle_hook = OracleHook(oracle_conn_id='oracle_db')
        connection = oracle_hook.get_conn()
        cursor = connection.cursor()
        
        # Get the current date and time
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Use the current date and time for the create_time field
        cursor.execute("""
        INSERT INTO ORACLE_TABLE (id, name, create_time) 
        VALUES (1, 'John Doe', TO_TIMESTAMP(:current_time, 'YYYY-MM-DD HH24:MI:SS'))""",
        [current_time])
        
        connection.commit()
        cursor.close()
        connection.close()
    
    insert_data = PythonOperator(
        task_id='insert_data_into_oracle_table',
        python_callable=insert_data_into_table,
    )

    insert_data