"""Lab 01: create UrbanGear's data-lake buckets. Safe to re-run (idempotent)."""
import boto3
import os
from botocore.exceptions import ClientError

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["MINIO_ENDPOINT"],      # container-to-container hostname
    aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
)

BUCKETS = ["urbangear-raw", "urbangear-processed", "urbangear-quarantine"]

for bucket in BUCKETS:
    try:
        s3.head_bucket(Bucket=bucket)          # cheap "does it exist?" check
        print(f"exists : {bucket}")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"created: {bucket}")