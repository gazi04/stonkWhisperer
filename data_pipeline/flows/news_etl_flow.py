from prefect import flow

from tasks.extraction import extract_news_data
from tasks.transformation import transform_news_data
from tasks.loading import load_news_data
from tasks.load_to_s3 import load_data_to_s3
from tasks.trigger_databricks_job import trigger_databrick_job

@flow(name="NewsAPI ETL Pipeline", log_prints=True)
async def news_etl_flow(query: str, category: str) -> int:
    """
    Dedicated ETL pipeline for NewsAPI.
    """
    print(f"*** Running News ETL for query: {query} ***")

    raw_data = extract_news_data(query)
    transformed_data = transform_news_data(raw_data)
    path = load_data_to_s3(transformed_data, "news", category)

    if path: 
        await trigger_databrick_job("get_news_from_s3", path)
    
    return len(raw_data)
