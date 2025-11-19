from datetime import date
from io import BytesIO
from typing import Optional
from prefect import task
from prefect_aws import S3Bucket

import pandas as pd


@task(name="Load data into a s3 bucket")
def load_data_to_s3(data: pd.DataFrame, flow_name: str, category="") -> Optional[str]:
    if data.empty:
        print("There isn't any data frame is empty so the task will be canceled.")
        return None

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
def stage_data_to_s3(file: BytesIO, s3_path: str) -> Optional[str]:
    try: 
        s3_bucket = S3Bucket.load("s3-bucket-block")
        s3_bucket_path = s3_bucket.upload_from_file_object(file, to_path=s3_path)

        return f"s3://{s3_bucket.bucket_name}/{s3_path}"
    except Exception as e:
        print(f"Unhandled Error: {e}")
        return None
