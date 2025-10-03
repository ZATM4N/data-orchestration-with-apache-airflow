import datetime
import logging
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

def _list_customers():
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    customers_df = hook.get_df(
        "SELECT * FROM customers;"
    )
    logging.info(customers_df)
    return customers_df

with DAG(
    dag_id="pipeline",
    start_date=datetime.datetime(2025, 10, 1),
    schedule=None,
):
    start = EmptyOperator(task_id="start")

    list_customers = PythonOperator(
        task_id="list_customers",
        python_callable=_list_customers,
    )

    end = EmptyOperator(task_id="end")

    start >> list_customers >> end