"""Lab 02: aggregation job submitted to the real cluster so we can watch the UI."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab02-agg").getOrCreate()   # confs come from spark-submit
daily = (spark.read.option("header", "true").option("inferSchema", "true")
         .csv("s3a://urbangear-raw/raw/orders/")
         .groupBy("product_name")
         .agg(F.sum("amount").alias("revenue")))
daily.show()
input("Open http://localhost:8081 -> Running Applications -> lab02-agg -> Application Detail UI. Press Enter to finish.")
spark.stop()