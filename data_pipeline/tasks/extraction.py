from celery import group
from newsapi import NewsApiClient
from newsapi.newsapi_exception import NewsAPIException
from prefect import task
from typing import Optional

from celery_app import app
from core.config_loader import settings

import praw
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
        data = api.get_everything(q=query, language="en", from_param="2025-10-20", to="2025-10-21")
        articles = data["articles"]
        
        if not articles:
            print("No articles found.")
            return []

        print(f"-> Starting concurrent content fetch for {len(articles)} articles.")
        
        print(f"-> Dispatching {len(articles)} article content fetching tasks to Celery.")
        
        task_signatures = [get_full_article.s(article["url"]) for article in articles]
        
        task_group = group(task_signatures)
        result = task_group.apply_async()

        print("-> Waiting for Celery workers to return full content (Max 300s timeout)...")
        try:
            full_contents = result.get(timeout=300) 
        except Exception as e:
            print(f"Error retrieving Celery results (Timeout or Task Failure): {e}")
            # If the group fails or times out, return None for all contents to continue flow
            full_contents = [None] * len(articles)

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

    reddit = praw.Reddit(
        client_id=settings.reddit_client_id,
        client_secret=settings.reddit_client_secret,
        username=settings.reddit_username,
        password=settings.reddit_password,
        user_agent=settings.reddit_user_agent,
    )

    subreddit = reddit.subreddit(subreddit)
    new_posts = subreddit.new(limit=100)

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

@app.task(name="fetch_article_content")
def get_full_article(url: str) -> Optional[str]:
    """
    Celery task to fetch and extract content from a single URL.
    This will run on the dedicated celery-worker container.
    """
    # print(f"    -> [Worker] Fetching content for: {url[:50]}...")
    downloaded = trafilatura.fetch_url(url)
    return trafilatura.extract(downloaded)
