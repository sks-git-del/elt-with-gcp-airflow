from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

project_id = 'project-fcfb8f7b-9392-450a-b27'
dataset_id = 'staging_dataset'
transform_dataset_id = 'transform_dataset'

source_table = f'{project_id}.{dataset_id}.global_data'
countries = ['USA', 'India', 'Germany', 'Japan', 'France', 'Canada', 'Italy']

with DAG(
    dag_id='load_and_transform',
    default_args=default_args,
    description='Load and transform data',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:

    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='health-data-subhendu-2026',
        object='global_health_data.csv',
        timeout=300,
        poke_interval=30,
        mode='poke',
    )

    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='health-data-subhendu-2026',
        source_objects=['global_health_data.csv'],
        destination_project_dataset_table=source_table,
        source_format='CSV',
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
    )

    for country in countries:
        BigQueryInsertJobOperator(
            task_id=f'create_table_{country.lower()}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{project_id}.{transform_dataset_id}.{country.lower()}_table` AS
                        SELECT *
                        FROM `{source_table}`
                        WHERE country = '{country}'
                    """,
                    "useLegacySql": False,
                }
            },
        ).set_upstream(load_csv_to_bigquery)

    check_file_exists >> load_csv_to_bigquery
