from typing import List
from prefect import flow
from tasks.extraction import extract_alpaca_data
from tasks.transformation import transform_alpaca_data
from tasks.loading import load_alpaca_data

@flow(name="Alpaca ETL Pipeline", log_prints=True)
def alpaca_etl_flow(symbols: List[str]):
    """
    Dedicated ETL pipeline for Alpaca-Py (Market Data).
    """
    print(f"*** Running Alpaca ETL for symbol: {symbols} ***")
    
    # 1. E-xtraction
    raw_data = extract_alpaca_data(symbols)
    
    # 2. T-ransformation (Placeholder)
    transformed_data, ticker_list = transform_alpaca_data(raw_data)
    
    # 3. L-oading (Placeholder)
    load_alpaca_data(transformed_data, ticker_list)

    return len(raw_data)
