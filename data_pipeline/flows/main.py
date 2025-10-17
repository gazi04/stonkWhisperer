# from prefect import flow
#
# from tasks.extraction import fetch_api_data
#
# @flow
# def bla():
#     fetch_api_data()
#     return "hello world"
#
# if __name__ == "__main__":
#     print(bla())

from prefect import flow
from flows.news_etl_flow import news_etl_flow
from flows.praw_etl_flow import praw_etl_flow
from flows.alpaca_etl_flow import alpaca_etl_flow

# Note: Using sub-flows is usually better than importing tasks directly here
# unless you need to merge data mid-run.

@flow(name="Daily Data Ingestion Master Flow", log_prints=True)
def main_ingestion_flow():
    """
    Master flow that orchestrates the concurrent execution of
    NewsAPI, PRAW, and Alpaca-Py ETL pipelines.
    """
    print("--- Starting Master Ingestion Flow (Running sub-flows concurrently) ---")
    
    # --- 1. Call Sub-Flows Concurrently ---
    # When Prefect calls another flow (the sub-flow), it treats it like a single,
    # large orchestratable task. By default, these sub-flow calls run in parallel.

    # Note: Replace 'FAKE_API_KEY' with a method to securely retrieve your actual API key
    news_future = news_etl_flow(query="technology", api_key="FAKE_API_KEY", wait_for=None)
    # praw_future = praw_etl_flow(subreddit="dataengineering", wait_for=None)
    # alpaca_future = alpaca_etl_flow(symbol="TSLA", wait_for=None)
    
    # --- 2. Aggregate Results (Wait for all futures to complete) ---
    
    # .result() waits for the sub-flow to complete and retrieves its return value.
    news_count = news_future.result()
    # praw_count = praw_future.result()
    # alpaca_count = alpaca_future.result()
    
    # total_records = news_count + praw_count + alpaca_count
    
    # --- 3. Final Logging / Reporting Task ---
    print("\n--- All Sub-Flows Completed ---")
    print(f"NewsAPI ETL ingested {news_count} records.")
    # print(f"PRAW ETL ingested {praw_count} records.")
    # print(f"Alpaca ETL ingested {alpaca_count} records.")
    # print(f"Total records ingested across all sources: {total_records}")

if __name__ == "__main__":
    main_ingestion_flow()

