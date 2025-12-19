from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='taxi_clean_pipeline',
    default_args=default_args,
    description='NYC Taxi Data Cleaning Pipeline',
    schedule_interval='@monthly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['spark', 'taxi', 'monthly'],
)

spark_clean = SparkSubmitOperator(
    task_id='run_spark_clean',
    application='/opt/airflow/scripts/clean_taxi.py',
    application_args=[
        '--input', '/opt/airflow/data/input/yellow_tripdata_2025-10.parquet',
        '--output', '/opt/airflow/data/output/cleaned',
        '--stats', '/opt/airflow/data/stats.json',
    ],
    conn_id='spark_default',
    dag=dag,
)

def push_stats_to_xcom(**context):
    stats_path = '/opt/airflow/data/stats.json'
    with open(stats_path, 'r') as f:
        stats = json.load(f)

    ti = context['ti']
    ti.xcom_push(key='records_read', value=stats['records_read'])
    ti.xcom_push(key='records_written', value=stats['records_written'])
    ti.xcom_push(key='job_status', value=stats['status'])

push_stats = PythonOperator(
    task_id='push_stats_to_xcom',
    python_callable=push_stats_to_xcom,
    provide_context=True,
    dag=dag,
)

spark_clean >> push_stats