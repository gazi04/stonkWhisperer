from prefect import flow
from tasks.extraction import extract_praw_data
# Import placeholder functions for T and L
from tasks.transformation import transform_praw_data
from tasks.loading import load_praw_data

@flow(name="PRAW ETL Pipeline", log_prints=True)
def praw_etl_flow(subreddit: str):
    """
    Dedicated ETL pipeline for PRAW (Reddit).
    """
    print(f"*** Running PRAW ETL for subreddit: {subreddit} ***")
    
    # 1. E-xtraction
    raw_data = extract_praw_data(subreddit=subreddit)
    
    # 2. T-ransformation (Placeholder)
    # transformed_data = transform_praw_data(raw_data)
    
    # 3. L-oading (Placeholder)
    # load_praw_data(transformed_data)

    return len(raw_data)
