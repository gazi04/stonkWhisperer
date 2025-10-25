from celery import group
from newsapi import NewsApiClient
from newsapi.newsapi_exception import NewsAPIException
from praw.exceptions import APIException, ClientException
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
def extract_praw_data(subreddit: str, flairs: list[str]) -> list[dict]:
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

    subreddit_obj = reddit.subreddit(subreddit)
    post_list: list = []
    
    try: 
        if flairs:
            data = subreddit_obj.search(
                query=prepare_reddit_query(flairs),
                sort="new",
                limit=100
            ) 
        else:
            data = subreddit_obj.new(limit=100)
    except APIException as api_e:
        print(f"PRAW API call failed during fetch. Reason: {api_e}")
        raise RuntimeError(f"PRAW Extraction Failed(API Error): {api_e}")
    except ClientException as client_e:
        print(f"PRAW Client connection issue. Reason: {client_e}")
        raise RuntimeError(f"PRAW Extraction Failed(Client Error): {client_e}")
    except Exception as e:
        print(f"PRAW extraction failed for r/{subreddit}. Reason: {e}")
        raise RuntimeError(f"PRAW Extraction Failed(Unhandled Error): {e}")

    try:
        for post in data:
            post_list.append({
                "reddit_id": post.id,
                "subreddit": post.subreddit.display_name,
                "author": post.author.name if post.author else "[deleted]",
                "title": post.title,
                "selftext": post.selftext, 
                "score": post.score,
                "num_comments": post.num_comments,
                "is_text_post": post.is_self,
                "url": post.url,
                "link_flair_text": post.link_flair_text or "",
                "upvote_ratio": post.upvote_ratio,
                "permalink": post.permalink,
                "published_at": post.created_utc,
            })
    except Exception as e:
        print(f"PRAW failed during mapping the fetch data inot a list. Reason: {e}")
        raise RuntimeError(f"PRAW Mapping Extraction Failed: {e}")

    return post_list

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

# ------------------------------
# CELERY TASKS
# ------------------------------
@app.task(name="fetch_article_content")
def get_full_article(url: str) -> Optional[str]:
    """
    Celery task to fetch and extract content from a single URL.
    This will run on the dedicated celery-worker container.
    """
    # print(f"    -> [Worker] Fetching content for: {url[:50]}...")
    downloaded = trafilatura.fetch_url(url)
    return trafilatura.extract(downloaded)

# ------------------------------
# UTIL FUNCTIONS
# ------------------------------
def prepare_reddit_query(flairs: list[str]) -> str:
    if len(flairs) == 0: return ""

    query = ""
    for index in range(len(flairs)):
        if index == len(flairs) - 1:
            return query + f"flair:\"{flairs[index]}\""
        query += f"flair:\"{flairs[index]}\" OR "

    return query
