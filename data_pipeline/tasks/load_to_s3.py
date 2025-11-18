from datetime import date
from io import BytesIO
from prefect import task
from prefect_aws import AwsCredentials, S3Bucket

import pandas as pd


@task(name="Load data into a s3 bucket")
def load_data_to_s3(data: pd.DataFrame, flow_name: str, category="") -> str:
    parquet_buffer = BytesIO()
    data.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    today = date.today()
    path = f"{flow_name}_data/ingestion_date={today.isoformat()}/{flow_name}"
    if category:
        path += f"_{category}"
    path += f"_{today.isoformat()}.parquet"
    return stage_data_to_s3(parquet_buffer, path)


# ------------------------------
# UTIL METHODS
# ------------------------------
def stage_data_to_s3(file: BytesIO, s3_path: str) -> str:
    # aws_credentials = AwsCredentials.load("aws-block")
    s3_bucket = S3Bucket.load("s3-bucket-block")

    s3_bucket_path = s3_bucket.upload_from_file_object(file, to_path=s3_path)
    print(s3_bucket_path)
    return f"s3://{s3_bucket.bucket_name}/{s3_path}"
