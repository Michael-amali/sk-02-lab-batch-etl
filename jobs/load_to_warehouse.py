"""Load Spark-cleaned MinIO parquet into warehouse.raw.sales for dbt.
Usage: python load_to_warehouse.py <YYYY-MM-DD>
"""
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/opt/airflow/jobs")
from pyspark.sql import functions as F
from spark_common import get_spark


def main(run_date: str) -> None:
    spark = get_spark(f"load-warehouse-{run_date}")
    src = f"s3a://urbangear-processed/processed/orders/date={run_date}/"
    processed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # Shape MinIO orders into the raw.sales contract that dbt staging expects.
    # store_id is not on UrbanGear orders — assign S01–S05 deterministically.
    sales = (
        spark.read.parquet(src)
        .select(
            "order_id",
            F.concat(
                F.lit("S"),
                F.lpad(((F.abs(F.hash("order_id")) % 5) + 1).cast("string"), 2, "0"),
            ).alias("store_id"),
            "product_id",
            "quantity",
            "unit_price",
            F.col("order_ts").cast("string").alias("order_ts"),
        )
        .withColumn("processed_at", F.lit(processed_at))
    )

    rows = [row.asDict(recursive=True) for row in sales.collect()]
    spark.stop()

    if not rows:
        raise SystemExit(f"NO ROWS to load for {run_date}")

    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["WAREHOUSE_HOST"],
        port=os.environ["WAREHOUSE_PORT"],
        dbname=os.environ["WAREHOUSE_DB"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
    )
    expected_cols = {
        "order_id", "store_id", "product_id", "quantity",
        "unit_price", "order_ts", "processed_at",
    }
    create_sql = """
        CREATE TABLE raw.sales (
            order_id     text,
            store_id     text,
            product_id   text,
            quantity     integer,
            unit_price   double precision,
            order_ts     text,
            processed_at timestamp
        )
    """

    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = 'sales'
                """
            )
            existing_cols = {r[0] for r in cur.fetchall()}
            # Older lab tables (sale_id/sale_date/…) break dbt staging — recreate.
            if existing_cols != expected_cols:
                cur.execute("DROP TABLE IF EXISTS raw.sales CASCADE")
                cur.execute(create_sql)

            # Idempotent for the run date: drop that day's rows, then insert.
            cur.execute(
                "DELETE FROM raw.sales WHERE date(order_ts::timestamp) = %s::date",
                (run_date,),
            )
            cur.executemany(
                """
                INSERT INTO raw.sales
                    (order_id, store_id, product_id, quantity, unit_price, order_ts, processed_at)
                VALUES
                    (%(order_id)s, %(store_id)s, %(product_id)s, %(quantity)s,
                     %(unit_price)s, %(order_ts)s, %(processed_at)s)
                """,
                rows,
            )
        conn.commit()
    finally:
        conn.close()

    print(f"[load_to_warehouse] date={run_date} loaded={len(rows):,} rows into raw.sales")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: load_to_warehouse.py <YYYY-MM-DD>")
    main(sys.argv[1])
