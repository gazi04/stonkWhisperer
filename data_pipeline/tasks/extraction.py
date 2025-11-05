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
from trafilatura import extract, extract_metadata
from typing import Any, Dict, List, Optional

from celery_app import app
from core.config_loader import settings
from core.constants import DATA_FETCH_LIMIT_PER_FLOW, DEFAULT_ARTICLE_DATA

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
                query=prepare_reddit_query(flairs), sort="new", limit=DATA_FETCH_LIMIT_PER_FLOW
            )
        else:
            data = subreddit_obj.new(limit=DATA_FETCH_LIMIT_PER_FLOW)
    except APIException as api_e:
        print(f"PRAW API call failed during fetch. Reason: {api_e}")
        raise RuntimeError(f"PRAW Extraction Failed(API Error): {api_e}")
    except ClientException as client_e:
        print(f"PRAW Client connection issue. Reason: {client_e}")
        raise RuntimeError(f"PRAW Extraction Failed(Client Error): {client_e}")
    except Exception as e:
        print(f"PRAW extraction failed for r/{subreddit}. Reason: {e}")
        raise RuntimeError(f"PRAW Extraction Failed(Unhandled Error): {e}")

    print(f"-> Dispatching {len(post_list)} article fetching tasks to Celery.")
    url_list = []
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

            url_list.append(post.url if not post.is_self else None)

    except Exception as e:
        print(f"PRAW failed during mapping the fetch data inot a list. Reason: {e}")
        raise RuntimeError(f"PRAW Mapping Extraction Failed: {e}")

    url_chunks = np.array_split(url_list, 4)

    task_signatures = [
        fetch_article_task.s(chunk.tolist()) for chunk in url_chunks
    ]

    task_group = group(task_signatures)
    result = task_group.apply_async()

    print("-> Waiting for Celery workers to return 4 batches of full content...")

    try:
        batch_results = result.get(timeout=60)
        final_articles = [article for batch in batch_results for article in batch]

        for reddit_post, article in zip(post_list, final_articles):
            reddit_post.update(article)

        print(f"<- Finished extracting the reddit posts with their linked article.")
        return post_list
    except Exception as e:
        print(f"Error retrieving Celery results (Timeout or Task Failure): {e}")
        print(f"Retrying the task.")
        raise e


@task(name="Extract Alpaca Data")
def extract_alpaca_data(symbol_list: List[str]) -> List[Dict]:
    total_symbols = len(symbol_list)
    print(f"-> Starting parallel Alpaca-Py extraction for {total_symbols} symbols.")

    symbol_chunks = np.array_split(symbol_list, 4)
    
    task_signatures = [
        fetch_stock_bars.s(chunk.tolist()) for chunk in symbol_chunks if chunk.size > 0
    ]
    
    task_group = group(task_signatures)
    result = task_group.apply_async()
    
    print(f"-> Waiting for {len(task_signatures)} Celery workers to return batches...")

    try:
        batch_results = result.get(timeout=300) 
        stock_data = [stock_bar for batch in batch_results for stock_bar in batch]
        print(f"<- Finished extracting {len(stock_data)} stock bars.")
        return stock_data
    except Exception as e:
        print(f"Error retrieving Celery results (Timeout or Task Failure): {e}")
        return []


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

@app.task(
    name="fetch_article",
    bind=True,
    autoretry_for=(RequestException,),
    retry_kwargs={'max_retries': 3, 'countdown': 30},
    soft_time_limit=300,
    time_limit=330,
)
def fetch_article_task(self, urls: List[Optional[str]]) -> List[Dict[str, Any]]:
    print(f"  -> [Worker] Starting async batch fetch for {len(urls)} articles.")

    async def main():
        async with httpx.AsyncClient() as client:
            tasks = [fetch_and_parse_url(client, url) for url in urls]
            processed_articles = await asyncio.gather(*tasks)
            return processed_articles

    results = asyncio.run(main())
    print(f"  -> [Worker] Finished async batch for {len(results)} articles.")
    return results

@app.task(
    name="fetch_stock_bars",
    bind=True,
    autoretry_for=(RequestException,),
    retry_kwargs={'max_retries': 3, 'countdown': 30},
    soft_time_limit=300,
    time_limit=330,
)
def fetch_stock_bars(symbols: List[str]):
    print(f"  -> [Worker] Starting fetching stock bars for {len(symbols)} symbols.")
    
    client = StockHistoricalDataClient(settings.alpaca_api_key, settings.alpaca_secret_key) 
    
    request = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Minute,
        start=datetime(2025, 10, 4), 
        end=datetime(2025, 10, 5)
    )

    data = client.get_stock_bars(request)
    print(f"  -> [Worker] Finished fetching raw data.")
    
    all_bars = []

    data_dict = data.dict()
    for bar_list in data_dict.values():
        all_bars.extend(bar_list)

    print(f"  -> [Worker] Parsed {len(all_bars)} bars.")
    return all_bars


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

async def fetch_and_parse_url(client: httpx.AsyncClient, url: str) -> Dict[str, Any]:
    if not url:
        return DEFAULT_ARTICLE_DATA

    try:
        response = await client.get(url, timeout=15.0, follow_redirects=True)
        response.raise_for_status() # Raise HTTP errors

        downloaded = response.text

        article = extract(downloaded)
        meta = extract_metadata(downloaded)

        if not article or not meta:
            return DEFAULT_ARTICLE_DATA

        return {
            "article_headline": meta.title,
            "article_author": meta.author,
            "article_publisher": meta.sitename,
            "article_content": article,
            "article_published_at": meta.date,
            "article_category": meta.categories,
        }
        
    except Exception as e:
        print(f"  -> [Worker] Async fetch failed for {url}: {e}")
        return DEFAULT_ARTICLE_DATA


