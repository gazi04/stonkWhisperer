from typing import List
from prefect import flow
from tasks.extraction import extract_alpaca_data
from tasks.transformation import transform_alpaca_data
from tasks.loading import load_alpaca_data
from tasks.load_to_s3 import load_data_to_s3
from tasks.trigger_databricks_job import trigger_databrick_job

@flow(name="Alpaca ETL Pipeline", log_prints=True)
async def alpaca_etl_flow(symbols: List[str]) -> int:
    """
    Dedicated ETL pipeline for Alpaca-Py (Market Data).
    """
    print(f"*** Running Alpaca ETL for symbol: {symbols} ***")
    
    raw_data = extract_alpaca_data(symbols)
    transformed_data, ticker_list = transform_alpaca_data(raw_data)
    path = load_data_to_s3(transformed_data, "stocks")

    if path:
        await trigger_databrick_job("get_stocks_from_s3", path)

    return len(raw_data)
