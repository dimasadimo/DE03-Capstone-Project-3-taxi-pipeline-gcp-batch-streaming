from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta
from pendulum import timezone, datetime
import sys
sys.path.insert(0, "/opt/airflow")
from gcs.upload_to_gcs import upload_all_files_for_today

local_tz = timezone("Asia/Jakarta")

default_args = {
    'owner': 'Dimas Adi Hartomo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 26, 2, 0, 0, tz=local_tz)
}

with DAG(
    dag_id='gcs_extract_dag',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['gcs', 'extract', 'daily'],
    description="DAG to extract CSV and JSON files to GCS"
) as dag:

    extract_task = PythonOperator(
        task_id='extract_file_to_gcs',
        python_callable=upload_all_files_for_today,
        op_kwargs={
            'bucket_name': 'jdeol003-bucket',
        }
    )

    trigger_load = TriggerDagRunOperator(
        task_id='trigger_load_dag',
        trigger_dag_id='load_gcs_to_bq_dag',  # DAG to trigger
        wait_for_completion=False,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed']
    )

    extract_task >> trigger_load