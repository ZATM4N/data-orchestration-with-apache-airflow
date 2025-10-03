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

def _transform_data(ti):
    # customers_df = pd.read_csv(...)
    # Format the datetime from "12 May 1990" to "1990-05-12"

    # ดึงค่าจาก XCom ของ task ก่อนหน้า
    customers_df = ti.xcom_pull(task_ids='extract_customer_data')
    customers_df["birthdate"] = pd.to_datetime(customers_df["birthdate"], dayfirst=True)   
    logging.info(customers_df)

    # กำหนดที่อยู่และชื่อไฟล์ที่จะบันทึก
    output_path = "/tmp/customers.parquet"

    # บันทึก DataFrame เป็นไฟล์ Parquet
    logging.info(f"Saving transformed data to {output_path}")
    customers_df.to_parquet(output_path, index=False)

    # คืนค่า Path ของไฟล์ออกไป
    return output_path

def _load_data_to_landing(ti):
    output_path = ti.xcom_pull(task_ids='transform_data')
    
    s3_hook = S3Hook(aws_conn_id="my_aws_connection")
    s3_key = "Golf/2025-10-03/customers.parquet"
    s3_bucket="pea-watt"
    s3_hook.load_file(
        filename=output_path,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )

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

    load_data_to_landing = PythonOperator(
        task_id="load_data_to_landing",
        python_callable=_load_data_to_landing,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_customer_data >> transform_data >> load_data_to_landing >> end