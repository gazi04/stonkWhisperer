from prefect import flow
from tasks.extraction import extract_news_data
from tasks.transformation import transform_news_data
from tasks.loading import dispatch_load_news_data


@flow(name="NewsAPI ETL Pipeline", log_prints=True)
def news_etl_flow():
    """
    Dedicated ETL pipeline for NewsAPI.
    """

    # raw_data = []
    # current_day = datetime(2025, 10, 2)
    # end_date = datetime.now()
    #
    # while current_day.date() < end_date.date():
    #     day_start = current_day
    #     day_end = day_start + timedelta(days=1)
    #     raw_data.extend(extract_news_data(query, day_start, day_end))
    #     current_day = day_end

    raw_data = extract_news_data(query, datetime(2025, 10, 2), datetime(2025, 10, 3))

    # 2. T-ransformation 
    transformed_data = transform_news_data(raw_data)
    
    # 3. L-oading 
    # todo: ♻️ need to automate such that we pass on the category 
    # that is used to extract the data from the api
    dispatch_load_news_data(transformed_data, "core_financial")
    
    return len(raw_data) # Return count for aggregation
