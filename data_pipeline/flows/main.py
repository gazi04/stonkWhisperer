from prefect import flow
from prefect_aws.s3 import asyncio
from core.constants import NEWS_CATEGORIES, SUBREDDITS, STOCK_TICKERS
from flows.news_etl_flow import news_etl_flow
from flows.praw_etl_flow import praw_etl_flow
from flows.alpaca_etl_flow import alpaca_etl_flow


@flow(name="Daily Data Ingestion Master Flow", log_prints=True)
async def main_ingestion_flow():
    """
    Master flow that orchestrates the concurrent execution of
    NewsAPI, PRAW, and Alpaca-Py ETL pipelines.
    """
    print("--- Starting Master Ingestion Flow (Running sub-flows concurrently) ---")

    news_count = 0
    praw_count = 0

    for category_name, query_string in NEWS_CATEGORIES.items():
        news_count += await news_etl_flow(query_string, category_name)

    for subreddit in SUBREDDITS:
        praw_count += await praw_etl_flow(subreddit["name"], subreddit["flairs"], wait_for=None)

    alpaca_result = await alpaca_etl_flow(symbols=STOCK_TICKERS, wait_for=None)
    alpaca_count = alpaca_result
    
    total_records = news_count + praw_count + alpaca_count
    
    print("\n--- All Sub-Flows Completed ---")
    print(f"NewsAPI ETL ingested {news_count} records.")
    print(f"PRAW ETL ingested {praw_count} records.")
    print(f"Alpaca ETL ingested {alpaca_count} records.")
    print(f"Total records ingested across all sources: {total_records}")

if __name__ == "__main__":
    asyncio.run(main_ingestion_flow())

