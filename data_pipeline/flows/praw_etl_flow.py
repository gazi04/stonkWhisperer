from typing import List
from prefect import flow

from tasks.extraction import extract_praw_data
from tasks.transformation import transform_praw_data
from tasks.loading import load_praw_data
from tasks.load_to_s3 import load_data_to_s3
from tasks.trigger_databricks_job import trigger_databrick_job

@flow(name="PRAW ETL Pipeline", log_prints=True)
async def praw_etl_flow(subreddit_name: str, subreddit_flair: List) -> int:
    """
    Dedicated ETL pipeline for PRAW (Reddit).
    """
    print(f"*** Running PRAW ETL for subreddit: {subreddit_name} ***")

    raw_data = extract_praw_data(subreddit=subreddit_name, flairs=subreddit_flair)
    transformed_data = transform_praw_data(raw_data)
    path = load_data_to_s3(transformed_data, "posts", subreddit_name)

    if path:
        await trigger_databrick_job("get_posts_from_s3", path)

    return len(raw_data)
