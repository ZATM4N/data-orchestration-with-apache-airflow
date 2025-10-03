import datetime
import logging
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

def _extract_customer_data():
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    customers_df = pg_hook.get_df(
        "SELECT * FROM customers;"
    )
    logging.info(customers_df)
    return customers_df

def _transform_data():
    #customers_df = pd.read_csv(...)
    # Format the datetime from "12 May 1990" to "1990-05-12"
    customers_df["birthdate"] = pd.to_datetime(df["birthdate"], dayfirst=True)    
    
    logging.info(customers_df)
    return customers_df

with DAG(
    dag_id="pipeline",
    start_date=datetime.datetime(2025, 10, 1),
    schedule=None,
):
    start = EmptyOperator(task_id="start")

    extract_customer_data = PythonOperator(
        task_id="extract_customer_data",
        python_callable=_extract_customer_data,
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=_transform_data,
    )

    end = EmptyOperator(task_id="end")

    start >> list_customers >> end