"""Shared SparkSession factory for all SK-03 jobs. Import, don't copy-paste."""
import os
from pyspark.sql import SparkSession

def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # The two JARs that teach Spark to speak S3 (downloaded once, then cached)
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        # Point the s3a connector at MinIO using the env vars from docker-compose
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )