import datetime
import logging
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import DAG

def _extract_customer_data():
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    customers_df = pg_hook.get_df(
        "SELECT * FROM customers;"
    )
    logging.info(customers_df)
    return customers_df

def _extract_order_data():
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    order_df = pg_hook.get_df(
        "SELECT * FROM orders;"
    )
    logging.info(order_df)

    # กำหนดที่อยู่และชื่อไฟล์ที่จะบันทึก
    order_output_path = "/tmp/orders.parquet"

    logging.info(f"Saving extracted data to {order_output_path}")
    order_df.to_parquet(order_output_path, index=False)

    return order_df

def _transform_customer_data(ti):
    # customers_df = pd.read_csv(...)
    # Format the datetime from "12 May 1990" to "1990-05-12"

    # ดึงค่าจาก XCom ของ task ก่อนหน้า
    customers_df = ti.xcom_pull(task_ids='extract_customer_data')
    customers_df["birthdate"] = pd.to_datetime(customers_df["birthdate"], dayfirst=True)   
    logging.info(customers_df)

    # กำหนดที่อยู่และชื่อไฟล์ที่จะบันทึก
    customer_output_path = "/tmp/customers.parquet"

    # บันทึก DataFrame เป็นไฟล์ Parquet
    logging.info(f"Saving transformed data to {customer_output_path}")
    customers_df.to_parquet(customer_output_path, index=False)

    # คืนค่า Path ของไฟล์ออกไป
    return customer_output_path

def _load_data_to_landing(ti):
    customer_output_path = ti.xcom_pull(task_ids='transform_customer_data')
    
    s3_hook = S3Hook(aws_conn_id="my_aws_connection")
    s3_key = "Golf/2025-10-03/customers.parquet"
    s3_bucket="pea-watt"
    logging.info(f"Uploading {customer_output_path} to S3 bucket {s3_bucket} with key '{s3_key}'")

    s3_hook.load_file(
        filename=customer_output_path,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )
    logging.info("File upload successful.")

    order_output_path = ti.xcom_pull(task_ids='extract_order_data')
    
    s3_hook = S3Hook(aws_conn_id="my_aws_connection")
    s3_key = "Golf/2025-10-03/orders.parquet"
    s3_bucket="pea-watt"
    logging.info(f"Uploading {order_output_path} to S3 bucket {s3_bucket} with key '{s3_key}'")

    s3_hook.load_file(
        filename=order_output_path,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )
    logging.info("File upload successful.")

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

    extract_order_data = PythonOperator(
        task_id="extract_order_data",
        python_callable=_extract_order_data,
    )

    transform_customer_data = PythonOperator(
        task_id="transform_customer_data",
        python_callable=_transform_customer_data,
    )

    load_data_to_landing = PythonOperator(
        task_id="load_data_to_landing",
        python_callable=_load_data_to_landing,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_customer_data >> transform_customer_data >> load_data_to_landing >> end
    extract_order_data >> load_data_to_landing