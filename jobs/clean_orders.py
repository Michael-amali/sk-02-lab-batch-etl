"""Lab 03: validate raw UrbanGear orders for one date; write Parquet + quarantine.
Usage: python clean_orders.py <YYYY-MM-DD>
"""
import sys
sys.path.insert(0, "/opt/airflow/jobs")
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)
from spark_common import get_spark

REJECT_THRESHOLD = 0.05   # fail the job if more than 5% of rows are bad

SCHEMA = StructType([
    StructField("order_id",       StringType(),  False),
    StructField("order_ts",       StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("product_name",   StringType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("unit_price",     DoubleType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("customer_email", StringType(),  True),
])

def main(run_date: str) -> None:
    spark = get_spark(f"clean-orders-{run_date}")
    src = f"s3a://urbangear-raw/raw/orders/date={run_date}/"
    raw = (spark.read.option("header", "true")
                .option("mode", "PERMISSIVE")     # malformed cells become null, row survives to be judged
                .schema(SCHEMA)
                .csv(src))
    
    checked = (raw
        .withColumn("reject_reason", F.concat_ws("; ",
            F.when(F.col("order_id").isNull(), F.lit("missing order_id")),
            F.when(F.col("amount").isNull() | (F.col("amount") <= 0),
                   F.lit("bad amount")),
            F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0),
                   F.lit("bad quantity")),
            F.when(~F.col("customer_email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
                   F.lit("invalid email")),
        )))

    good = checked.filter(F.col("reject_reason") == "").drop("reject_reason")
    bad  = checked.filter(F.col("reject_reason") != "")

    total = raw.count()
    good_deduped = good.dropDuplicates(["order_id"])
    n_good, n_bad = good_deduped.count(), bad.count()
    n_dupes = total - n_good - n_bad

    print(f"[clean_orders] date={run_date} total={total} "
          f"clean={n_good} rejected={n_bad} duplicates_removed={n_dupes}")

    if total == 0:
        raise SystemExit(f"NO INPUT for {run_date} — refusing to write empty output")
    if n_bad / total > REJECT_THRESHOLD:
        raise SystemExit(f"REJECT RATE {n_bad/total:.1%} exceeds "
                         f"{REJECT_THRESHOLD:.0%} — systemic issue, failing loudly")
    

    dst_good = f"s3a://urbangear-processed/processed/orders/date={run_date}/"
    dst_bad  = f"s3a://urbangear-quarantine/quarantine/orders/date={run_date}/"

    good_deduped.write.mode("overwrite").parquet(dst_good)
    if n_bad > 0:
        bad.write.mode("overwrite").parquet(dst_bad)

    print(f"[clean_orders] wrote {dst_good} and "
          f"{dst_bad if n_bad else '(no rejects)'}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: clean_orders.py <YYYY-MM-DD>")
    main(sys.argv[1])