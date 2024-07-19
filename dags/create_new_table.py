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
    dag_id='create_new_table',
    default_args=default_args,
    schedule_interval=None
) as dag:

    # delete oracle table if exists and create new table
    def create_new_table(**kwargs):
        oracle_hook = OracleHook(oracle_conn_id='oracle_db')
        connection = oracle_hook.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE ORACLE_TABLE';
        EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
                RAISE;
            END IF;
        END;
        """)
        cursor.execute(
            "CREATE TABLE ORACLE_TABLE (id NUMBER, name VARCHAR(50), create_time DATE)")
        connection.commit()
        cursor.close()
        connection.close()

    create_oracle_table = PythonOperator(
        task_id='create_new_oracle_table',
        python_callable=create_new_table,
    )

    # delete postgres table if exists and create new table
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

    create_postgres_table = PythonOperator(
        task_id='create_new_postgres_table',
        python_callable=create_new_table,
    )

    create_postgres_table
