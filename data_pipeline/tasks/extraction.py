from concurrent.futures import ThreadPoolExecutor
from newsapi import NewsApiClient
from newsapi.newsapi_exception import NewsAPIException
from prefect import task
from typing import Optional

from core.config_loader import settings

import trafilatura

@task(name="Extract NewsAPI Data")
def extract_news_data(query: str) -> list[dict]:
    """
    Connects to the NewsAPI SDK to fetch articles based on a query.
    Note: For simplicity, we use requests here, but in production, you'd use the SDK.
    """
    print(f"-> Starting NewsAPI extraction for query: {query}")

    try:
        api = NewsApiClient(api_key=settings.news_api_key)
        data = api.get_everything(q=query, language="en")
        articles = data["articles"]
        
        if not articles:
            print("No articles found.")
            return []

        print(f"-> Starting concurrent content fetch for {len(articles)} articles.")
        
        article_urls = [article["url"] for article in articles]
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            full_contents = list(executor.map(get_full_article, article_urls))
            
        for article, content in zip(articles, full_contents):
            article["content"] = content

        print(f"<- Finished fetching full content for {len(articles)} articles.")
        return articles
    except NewsAPIException as ex:
        raise ex


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

def get_full_article(url: str) -> Optional[str]:
    downloaded = trafilatura.fetch_url(url)
    return trafilatura.extract(downloaded)
