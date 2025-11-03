from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from celery import group
from datetime import datetime
from newsapi import NewsApiClient
from newsapi.newsapi_exception import NewsAPIException
from praw.exceptions import APIException, ClientException
from prefect import task
from requests import RequestException
from trafilatura import fetch_url, extract, extract_metadata
from typing import Any, Dict, List, Optional

from celery_app import app
from core.config_loader import settings

import asyncio
import httpx
import praw
import numpy as np

# ------------------------------
# PREFECT TASKS
# ------------------------------
@task(name="Extract NewsAPI Data")
def extract_news_data(query: str, start_date: datetime, end_date: datetime) -> list[dict]:
    print(f"-> Starting NewsAPI extraction for query: {query}")

    try:
        api = NewsApiClient(api_key=settings.news_api_key)
        data = api.get_everything(
            q=query, language="en", from_param=from_date_str, to=to_date_str
        )
    except NewsAPIException as e:
        print(f"Error occured in the NewsAPI client: {e}")
        print(f"Retrying the task.")
        raise e

    articles: List[Dict[str, Any]] = data["articles"]
    article_chunks = np.array_split(articles, 4)

    if not articles:
        print("No articles found.")
        return []


    print(
        f"-> Dispatching {len(articles)} article content fetching tasks to Celery."
    )
    task_signatures = [
        fetch_and_extract_content.s(chunk.tolist()) 
        for chunk in article_chunks
    ]

    task_group = group(task_signatures)
    result = task_group.apply_async()

    print("-> Waiting for Celery workers to return 4 batches of full content...")

    try:
        batch_results = result.get(timeout=60)
        final_articles = [article for batch in batch_results for article in batch]
        print(f"<- Finished extracting the articles.")
        return final_articles
    except Exception as e:
        print(f"Error retrieving Celery results (Timeout or Task Failure): {e}")
        print(f"Retrying the task.")
        raise e

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
                query=prepare_reddit_query(flairs), sort="new", limit=100
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

    task_signatures = []
    try:
        for post in data:
            post_list.append(
                {
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
                }
            )

            if not post.is_self:
                task_signatures.append(fetch_article_task.s(post.url))
            else:
                task_signatures.append(fetch_article_task.s(None))

    except Exception as e:
        print(f"PRAW failed during mapping the fetch data inot a list. Reason: {e}")
        raise RuntimeError(f"PRAW Mapping Extraction Failed: {e}")

    print(
        f"-> Dispatching {len(task_signatures)} article content fetching tasks to Celery."
    )

    task_group = group(task_signatures)
    result = task_group.apply_async()

    print(
        "-> Waiting for Celery workers to return full article data (Max 300s timeout)..."
    )
    full_article_data = []
    try:
        full_article_data = result.get(timeout=300)
    except Exception as e:
        print(f"Error retrieving Celery results (Timeout or Task Failure): {e}")
        full_article_data = [DEFAULT_ARTICLE_DATA] * len(post_list)

    for post, article_data in zip(post_list, full_article_data):
        # If fetch failed or timed out, add default empty structure
        post.update(article_data) if isinstance(article_data, dict) else post.update(
            DEFAULT_ARTICLE_DATA
        )

    print(f"<- Finished fetching content for {len(post_list)} posts/articles.")
    return post_list


@task(name="Extract Alpaca Data")
def extract_alpaca_data(symbol_list: List[str] = ["AAPL", "TSLA", "NVDA"]) -> List[Dict]:
    print(f"-> Starting Alpaca-Py extraction for symbol: AAPL")

    alpaca_client = StockHistoricalDataClient(settings.alpaca_api_key, settings.alpaca_secret_key) 
    request = StockBarsRequest(
        symbol_or_symbols=symbol_list,
        timeframe=TimeFrame.Minute,
        start=datetime(2025, 10, 1),
        end=datetime(2025, 10, 2)
    )
    data = alpaca_client.get_stock_bars(request)
    all_bars = []

    for bar in data.dict().values():
        all_bars.extend(bar)

    return all_bars


# ------------------------------
# CELERY TASKS
# ------------------------------
@app.task(
    name="fetch_and_extract_content",
    bind=True,
    autoretry_for=(RequestException,),
    retry_kwargs={'max_retries': 3, 'countdown': 30},
    soft_time_limit=300,
    time_limit=330,
)
def fetch_and_extract_content(self, articles_batch: List[Dict])  -> List[Dict]:
    print(f"  -> [Worker] Starting async batch fetch for {len(articles_batch)} articles.")
    
    async def main():
        async with httpx.AsyncClient() as client:
            tasks = [fetch_one_url(client, article) for article in articles_batch]
            processed_articles = await asyncio.gather(*tasks)
            return processed_articles

    results = asyncio.run(main())
    print(f"  -> [Worker] Finished async batch for {len(results)} articles.")
    return results

    downloaded = None

    try:
        downloaded = fetch_url(url)
    except RequestException as e:
        print(f"ERROR: Failed to download URL '{url}'. Network/Connection Issue: {e}")
        return None
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during fetch_url: {e}")
        return None

    if downloaded is None:
        print(f"WARNING: No content downloaded for URL '{url}'.")
        return None

    try:
        article = extract(downloaded)
        meta = extract_metadata(downloaded)

        if article is None or meta is None:
            failed_item = (
                "content" if article is None else "metadata" if meta is None else "data"
            )
            print(
                f"WARNING: Trafilatura failed to extract {failed_item} from the downloaded data for '{url}'."
            )
            return None

        return {
            "article_headline": meta.title,
            "article_author": meta.author,
            "article_publisher": meta.sitename,
            "article_content": article,
            "article_published_at": meta.date,
            "article_category": meta.categories
        }
    except Exception as e:
        print(f"ERROR: An exception occurred during content extraction: {e}")
        return None


# ------------------------------
# UTIL FUNCTIONS
# ------------------------------
def prepare_reddit_query(flairs: list[str]) -> str:
    flair_queries = [f'flair:"{flair}"' for flair in flairs]
    return " OR ".join(flair_queries)

async def fetch_one_url(client: httpx.AsyncClient, article: Dict[str, Any]) -> Dict[str, Any]:
    url = article.get("url")
    if not url:
        article["content"] = None
        return article
    
    try:
        response = await client.get(url, timeout=15.0, follow_redirects=True)
        content = extract(response.text)
        article["content"] = content
    except Exception as e:
        print(f"  -> [Worker] Async fetch failed for {url}: {e}")
        article["content"] = None
    
    return article

    url = article.get("url")
    if not url:
        article["content"] = None
