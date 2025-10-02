# connection_and_hook.py
import datetime
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _list_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    tables = hook.get_records(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
    )
    logging.info(tables)
    return tables


with DAG(
    dag_id="connection_and_hook",
    start_date=datetime.datetime(2025, 10, 1),
    schedule=None,
):
    start = EmptyOperator(task_id="start")

    list_tables = PythonOperator(
        task_id="list_tables",
        python_callable=_list_tables,
    )

    end = EmptyOperator(task_id="end")

    start >> list_tables >> end