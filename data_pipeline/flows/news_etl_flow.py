from prefect import flow
from tasks.extraction import extract_news_data
# Import placeholder functions for T and L
from tasks.transformation import transform_news_data
from tasks.loading import load_news_data

@flow(name="NewsAPI ETL Pipeline", log_prints=True)
def news_etl_flow(query: str, api_key: str):
    """
    Dedicated ETL pipeline for NewsAPI.
    """
    print(f"*** Running News ETL for query: {query} ***")
    
    # 1. E-xtraction
    raw_data = extract_news_data(query=query, api_key=api_key)
    
    # 2. T-ransformation (Placeholder)
    # transformed_data = transform_news_data(raw_data)
    
    # 3. L-oading (Placeholder)
    # load_news_data(transformed_data)
    
    return len(raw_data) # Return count for aggregation
