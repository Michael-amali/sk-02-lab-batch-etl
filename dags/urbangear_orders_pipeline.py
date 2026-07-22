"""UrbanGear nightly orders pipeline: wait -> clean (Spark) -> verify.
Every task is a pure function of {{ ds }} => idempotent, backfillable."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

def _on_failure(context):
    ti = context["task_instance"]
    # In production this posts to Slack/PagerDuty. Locally we log loudly.
    print(f"[ALERT] task={ti.task_id} dag={ti.dag_id} ds={context['ds']} FAILED "
          f"after {ti.try_number - 1} attempts. Log: {ti.log_url}")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,      # 1 min, then 2 min
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="urbangear_orders_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="0 5 * * *",        # 05:00 UTC — file lands ~03:00, buffer for lateness
    catchup=False,               # we'll backfill explicitly in Step 4.4
    max_active_runs=1,           # backfill dates one at a time (laptop-friendly)
    default_args=default_args,
    tags=["urbangear", "lab05"],
) as dag:

    wait_for_file = S3KeySensor(
        task_id="wait_for_orders_file",
        aws_conn_id="minio_s3",
        bucket_name="urbangear-raw",
        bucket_key="raw/orders/date={{ ds }}/*",
        wildcard_match=True,
        mode="reschedule",           # free the worker slot between checks
        poke_interval=60,            # check every minute
        timeout=60 * 60 * 2,         # give up (fail loudly) after 2 hours
        retries=0,                   # a sensor timeout is real news — don't retry it
    )


    clean_orders = BashOperator(
        task_id="clean_orders",
        bash_command="python /opt/airflow/jobs/clean_orders.py {{ ds }}",
    )

    def _verify_output(**context):
        """Trust nothing: confirm the partition exists and has rows."""
        import sys; sys.path.insert(0, "/opt/airflow/jobs")
        from spark_common import get_spark
        ds = context["ds"]
        spark = get_spark(f"verify-{ds}")
        n = spark.read.parquet(
            f"s3a://urbangear-processed/processed/orders/date={ds}/").count()
        spark.stop()
        if n == 0:
            raise ValueError(f"output partition for {ds} is EMPTY")
        print(f"[verify] date={ds} clean_rows={n} OK")

    verify_output = PythonOperator(
        task_id="verify_output",
        python_callable=_verify_output,
    )

    wait_for_file >> clean_orders >> verify_output