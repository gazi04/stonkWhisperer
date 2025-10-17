from prefect import flow
from tasks.extraction import extract_alpaca_data
# Import placeholder functions for T and L
from tasks.transformation import transform_alpaca_data
from tasks.loading import load_alpaca_data

@flow(name="Alpaca ETL Pipeline", log_prints=True)
def alpaca_etl_flow(symbol: str):
    """
    Dedicated ETL pipeline for Alpaca-Py (Market Data).
    """
    print(f"*** Running Alpaca ETL for symbol: {symbol} ***")
    
    # 1. E-xtraction
    raw_data = extract_alpaca_data(symbol=symbol)
    
    # 2. T-ransformation (Placeholder)
    # transformed_data = transform_alpaca_data(raw_data)
    
    # 3. L-oading (Placeholder)
    # load_alpaca_data(transformed_data)

    return len(raw_data)
