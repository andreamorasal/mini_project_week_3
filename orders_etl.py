from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

from datetime import datetime, timedelta
import pandas as pd
import logging


# -----------------------------
# File location
# -----------------------------

RAW_FILE = "/opt/airflow/data/raw/orders.csv"


# -----------------------------
# DAG settings
# -----------------------------

default_args = {
    "owner": "analytics_team",

    # Retry configuration
    "retries": 2,
    "retry_delay": timedelta(minutes=5),

    # Failure email alerts
    "email_on_failure": True,
    "email": [
        "MYEMAIL@gmail.com"
    ],
}


# -----------------------------
# ETL Functions
# -----------------------------

def extract_orders():

    logging.info("Starting orders extraction")

    df = pd.read_csv(RAW_FILE)

    logging.info(
        f"Extracted {len(df)} orders"
    )



def validate_orders():

    logging.info(
        "Starting orders validation"
    )

    df = pd.read_csv(RAW_FILE)


    # Check if file is empty

    if df.empty:
        raise ValueError(
            "Orders file is empty"
        )


    # Required columns

    required_columns = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price"
    ]


    for column in required_columns:

        if column not in df.columns:
            raise ValueError(
                f"Missing column: {column}"
            )


    # Check missing values

    missing_values = df.isnull().sum()

    logging.info(
        f"Missing values:\n{missing_values}"
    )


    logging.info(
        "Validation successful"
    )



def load_staging():

    logging.info(
        "Loading data into staging_orders table"
    )


    df = pd.read_csv(RAW_FILE)


    # In a real project:
    # df.to_sql("staging_orders", connection)


    logging.info(
        f"Loaded {len(df)} rows into staging_orders"
    )



def transform_orders():

    logging.info(
        "Starting transformation step"
    )


    df = pd.read_csv(RAW_FILE)


    # Create calculated field

    df["total_sales"] = (
        df["quantity"] *
        df["unit_price"]
    )


    logging.info(
        "Transformation completed successfully"
    )



# -----------------------------
# DAG Definition
# -----------------------------

with DAG(

    dag_id="orders_etl_pipeline",

    description="Daily orders ETL pipeline",

    start_date=datetime(2026, 1, 1),

    # Run every day at 2 AM
    schedule="0 2 * * *",

    catchup=False,

    default_args=default_args,

    tags=[
        "etl",
        "orders"
    ]

) as dag:


    # -----------------------------
    # Sensor
    # -----------------------------

    check_orders_file = FileSensor(

        task_id="check_orders_file",

        filepath=RAW_FILE,

        poke_interval=60,

        timeout=600

    )


    # -----------------------------
    # Extract
    # -----------------------------

    extract_task = PythonOperator(

        task_id="extract_orders",

        python_callable=extract_orders

    )


    # -----------------------------
    # Validation
    # -----------------------------

    validate_task = PythonOperator(

        task_id="validate_orders",

        python_callable=validate_orders

    )


    # -----------------------------
    # Load staging
    # -----------------------------

    staging_task = PythonOperator(

        task_id="load_staging_orders",

        python_callable=load_staging

    )


    # -----------------------------
    # Transformation
    # -----------------------------

    transform_task = PythonOperator(

        task_id="transform_orders",

        python_callable=transform_orders

    )


    # -----------------------------
    # Dependencies
    # -----------------------------

    (
        check_orders_file
        >> extract_task
        >> validate_task
        >> staging_task
        >> transform_task
    )
