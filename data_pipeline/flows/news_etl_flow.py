from datetime import datetime, timedelta
from prefect import flow

from tasks.extraction import extract_news_data
from tasks.transformation import transform_news_data
from tasks.loading import load_news_data

@flow(name="NewsAPI ETL Pipeline", log_prints=True)
def news_etl_flow(query: str, category: str):
    """
    Dedicated ETL pipeline for NewsAPI.
    """
    print(f"*** Running News ETL for query: {query} ***")

    # raw_data = []
    # current_day = datetime(2025, 10, 7)
    # end_date = datetime(2025, 10, 15)
    #
    # while current_day.date() < end_date.date():
    #     day_start = current_day
    #     day_end = day_start + timedelta(days=1)
    #     raw_data.extend(extract_news_data(query, day_start, day_end))
    #     current_day = day_end

    raw_data = extract_news_data(query, datetime(2025, 10, 16), datetime(2025, 10, 17))

    transformed_data = transform_news_data(raw_data)
    
    load_news_data(transformed_data, category)
    
    return len(raw_data)
