"""
Imported models are not used, in a traditional way, either way for SqlAlchemy they need to be imported to create the corresponding tables foreach model, for suppresing the lint error in ruff is used 'noqa'
"""

from sqlalchemy import DDL
from sqlalchemy.exc import ProgrammingError

from core.database import Base, engine
from models import Article, RedditPost, Company, StockBar

def ensure_timescale_setup(engine):
    """
    Executes necessary TimescaleDB setup commands, including enabling the extension
    and converting the stock_bars table to a hypertable.
    """
    print("-> Checking/Setting up TimescaleDB extension and hypertable...")
    
    try:
        with engine.connect() as connection:
            connection.execute(DDL("CREATE EXTENSION IF NOT EXISTS timescaledb"))
            connection.commit()
            print("-> TimescaleDB extension confirmed/enabled.")
    except ProgrammingError as e:
        print(f"WARNING: Could not enable TimescaleDB extension. Is the TimescaleDB image in use? Error: {e}")
        return 
    except Exception as e:
        print(f"An unexpected error occurred during Timescale extension setup: {e}")
        return

    hypertable_sql = DDL("SELECT create_hypertable('stock_bars', 'timestamp', if_not_exists => TRUE)")
    
    try:
        with engine.connect() as connection:
            connection.execute(hypertable_sql)
            connection.commit()
            print("-> 'stock_bars' table successfully converted to a TimescaleDB hypertable.")
    except ProgrammingError as e:
        print(f"WARNING: Could not convert 'stock_bars' to hypertable. You may need to run initial migration/creation first. Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during hypertable creation: {e}")


def create_tables():
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Standard tables created successfully.")
    ensure_timescale_setup(engine)


if __name__ == "__main__":
    create_tables()

