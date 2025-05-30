from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from pendulum import timezone, datetime

local_tz = timezone("Asia/Jakarta")

default_args = {
    'owner': 'Dimas Adi Hartomo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 26, 2, 0, 0, tz=local_tz)
}

with DAG(
    dag_id="transform_dbt_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['dbt', 'transform', 'daily', 'bigquery'],
    description="DAG to Transform DBT Bigquery"
) as dag:

    transform_dbt = BashOperator(
        task_id="transform_dbt",
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --models mart.taxi_data_mart
        """
    )

    transform_dbt