from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = "project-fcfb8f7b-9392-450a-b27"

with DAG(
    dag_id="check_and_load_csv",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcs", "bigquery"],
) as dag:

    check_file = GCSObjectExistenceSensor(
        task_id="check_file",
        bucket="health-data-subhendu-2026",
        object="global_health_data.csv",
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    load_csv = GCSToBigQueryOperator(
        task_id="load_csv",
        bucket="health-data-subhendu-2026",
        source_objects=["global_health_data.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.staging_dataset.global_data",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    check_file >> load_csv
