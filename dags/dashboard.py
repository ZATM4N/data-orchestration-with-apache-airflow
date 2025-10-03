from airflow.sdk import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    "dashboard",
    start_date=timezone.datetime(2025, 1, 1),
    schedule=None,
):
    start = EmptyOperator(task_id="start")
    extract_api = EmptyOperator(task_id="extract_api")
    extract_postgres = EmptyOperator(task_id="extract_postgres")
    extract_google_analytics = EmptyOperator(task_id="extract_google_analytics")
    tranform_and_clean_data = EmptyOperator(task_id="tranform_and_clean_data")
    load_to_data_warehouse = EmptyOperator(task_id="load_to_data_warehouse")
    generate_reports = EmptyOperator(task_id="generate_reports")
    end = EmptyOperator(task_id="end")

    start >> [extract_api, extract_postgres, extract_google_analytics] >> tranform_and_clean_data >> load_to_data_warehouse >> generate_reports >> end