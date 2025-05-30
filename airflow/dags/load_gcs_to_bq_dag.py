from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta
from pendulum import timezone, datetime

local_tz = timezone("Asia/Jakarta")

default_args = {
    'owner': 'Dimas Adi Hartomo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 26, 2, 0, 0, tz=local_tz)
}

files_to_load = [
    # DAILY DATA
    {
        "gcs_path": "capstone3_dimasadihartomo/csv/{{ ds }}/*",
        "table": "taxi_data_staging",
        "format": "CSV"
    },
    {
        "gcs_path": "capstone3_dimasadihartomo/json/{{ ds }}/*",
        "table": "taxi_data_staging",
        "format": "NEWLINE_DELIMITED_JSON"
    },
    # MASTER DATA (always same file name)
    {
        "gcs_path": "capstone3_dimasadihartomo/master/taxi_zone_lookup.csv",
        "table": "taxi_zone",
        "format": "CSV"
    },

    {
        "gcs_path": "capstone3_dimasadihartomo/master/payment_type.csv",
        "table": "payment_type",
        "format": "CSV"
    }
]

taxi_data_staging_schema = [
    {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "lpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "store_and_fwd_flag", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "RatecodeID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "passenger_count", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "ehail_fee", "type": "STRING", "mode": "NULLABLE"},
    {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "payment_type", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "trip_type", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "congestion_surcharge", "type": "STRING", "mode": "NULLABLE"}
]

with DAG(
    dag_id='load_gcs_to_bq_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['gcs', 'load', 'daily', 'bigquery'],
    description="DAG to load CSV and JSON files from GCS to Bigquery"
) as dag:
    
    start = EmptyOperator(task_id="start_load")
    end = EmptyOperator(task_id="end_load")
    
    for file_cfg in files_to_load:

        is_taxi_data = "{{ ds }}" in file_cfg["gcs_path"]

        # Auto choose WRITE_APPEND for daily, WRITE_TRUNCATE for master
        write_disposition = "WRITE_APPEND" if is_taxi_data else "WRITE_TRUNCATE"

        # Determine if CSV needs header skip
        skip_rows = 1 if file_cfg["format"] == "CSV" else 0

        task_id = f"load_{file_cfg['table']}_{file_cfg['format'].lower()}"

        load_task = GCSToBigQueryOperator(
            task_id=task_id,
            bucket='jdeol003-bucket',
            source_objects=[file_cfg["gcs_path"]],
            destination_project_dataset_table=f"purwadika.jcdeol3_capstone3_dimasadihartomo.{file_cfg['table']}",
            source_format=file_cfg["format"],
            write_disposition=write_disposition,
            skip_leading_rows=skip_rows,
            schema_fields=taxi_data_staging_schema if file_cfg["table"] == "taxi_data_staging" else None,
            time_partitioning={"type": "DAY", "field": "lpep_pickup_datetime"} if file_cfg["table"] == "taxi_data_staging" else None,
            autodetect=not is_taxi_data,
        )

        start >> load_task >> end
    
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="transform_dbt_dag", 
        wait_for_completion=False,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    end >> trigger_dbt