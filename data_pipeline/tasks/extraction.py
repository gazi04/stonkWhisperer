# from prefect import task
# from core.config_loader import settings
#
# import httpx
#
# # @task
# def fetch_api_data():
#     response = httpx.get("https://api.nytimes.com/svc/archive/v1/2024/1.json?api-key=" + settings.ny_times_api_key)
#
#     print(response.status_code)
#
#     print(response.json())
#
# if __name__ == "__main__":
#     fetch_api_data()

from prefect import task
from newsapi import NewsApiClient

from core.config_loader import settings
import requests
# Assume the SDKs (newsapi-python, praw, alpaca-py) are installed

@task(name="Extract NewsAPI Data")
def extract_news_data(query: str, api_key: str) -> list[dict]:
    """
    Connects to the NewsAPI SDK to fetch articles based on a query.
    Note: For simplicity, we use requests here, but in production, you'd use the SDK.
    """
    print(f"-> Starting NewsAPI extraction for query: {query}")
    api = NewsApiClient(api_key=settings.news_api_key)
    
    news = api.get_everything(language="en")
    # Placeholder for NewsAPI SDK interaction
    # Example: newsapi = NewsApiClient(api_key=api_key)
    # response = newsapi.get_everything(q=query, language='en')
    
    # Mock API call for demonstration:
    response = requests.get(f"https://some-news-mock-api.com/articles?q={query}")
    response.raise_for_status()
    
    # In a real scenario, this would return the list of article objects
    return [{"source": "NewsAPI", "title": f"Article {i}"} for i in range(10)]

@task(name="Extract PRAW Data")
def extract_praw_data(subreddit: str) -> list[dict]:
    """
    Connects to Reddit via PRAW to fetch posts from a specified subreddit.
    """
    print(f"-> Starting PRAW extraction from subreddit: {subreddit}")
    
    # Placeholder for PRAW SDK interaction
    # Example: reddit = praw.Reddit('...')
    # subreddit = reddit.subreddit(subreddit)
    # return [post for post in subreddit.hot(limit=20)]

    return [{"source": "PRAW", "post": f"Post {i}"} for i in range(5)]

@task(name="Extract Alpaca Data")
def extract_alpaca_data(symbol: str) -> list[dict]:
    """
    Connects to Alpaca-Py SDK to fetch market data for a stock symbol.
    """
    print(f"-> Starting Alpaca-Py extraction for symbol: {symbol}")
    
    # Placeholder for Alpaca-Py SDK interaction
    # Example: trading_client = TradingClient(ALPACA_KEY_ID, ALPACA_SECRET_KEY)
    # data = trading_client.get_latest_bar(symbol)

    return [{"source": "Alpaca", "price": 150.50, "symbol": symbol}]

