"""Lab 04: first real DAG — three tasks, a dependency fan-in, daily schedule."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,                                # try 3 times total
    "retry_delay": timedelta(minutes=1),
}

def summarize(**context):
    ds = context["ds"]                           # logical date, e.g. "2026-07-01"
    print(f"[urbangear_hello] pretending to summarize orders for {ds}")

with DAG(
    dag_id="urbangear_hello",
    description="Lab 04 teaching DAG - no real work",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,                               # do NOT backfill history (yet)
    default_args=default_args,
    tags=["lab04", "urbangear"],
) as dag:

    check_env = BashOperator(
        task_id="check_minio",
        bash_command="curl -sf http://minio:9000/minio/health/live && echo MINIO_OK",
    )
    print_date = BashOperator(
        task_id="print_logical_date",
        bash_command="echo 'processing data interval starting {{ ds }}'",
    )
    summary = PythonOperator(
        task_id="summarize", 
        python_callable=summarize
    )
    clean_orders_preview = BashOperator(
        task_id="clean_orders_preview",
        bash_command="python /opt/airflow/jobs/clean_orders.py {{ ds }}", # 2026-07-03
    )

    [check_env, print_date] >> summary           # both must succeed first
    summary >> clean_orders_preview