from celery import group
from prefect import task
from requests import RequestException
from typing import Any, Dict, List

from celery_app import app
from core.database import get_db
from models import Article, RedditPost, StockBar
from services import ArticleService, CompanyService, RedditService, StockBarService

import pandas as pd
import numpy as np


@task(name="Dispatch DB Load Task")
def load_news_data(data: pd.DataFrame, category: str) -> int:

    if data is None:
        print("There are 0 news records, the loading taks will be skipped.")
        return 0

    data_size = len(data)
    print(f"-> [Dispatcher] DataFrame received. Size: {data_size}")

    if data.empty:
        print("-> [Dispatcher] DataFrame is empty. Skipping DB load.")
        return 0

    print(
        f"-> [Dispatcher] Converting DataFrame of size {data_size} to list of records..."
    )

    records = data.to_dict("records")

    print(
        f"-> [Dispatcher] Dispatching {len(records)} records for DB insertion via Celery."
    )

    result = insert_articles_task.apply(args=[records, category])

    articles_inserted = result.get()

    print(
        f"-> [Dispatcher] Celery task completed. {articles_inserted} articles inserted."
    )

    return articles_inserted


@task(name="Dispatch Reddit Load Task")
def load_praw_data(data: pd.DataFrame) -> int:
    """
    Loads Reddit posts from a DataFrame into the database.
    """
    if data is None:
        print("There are 0 reddit post records, the loading taks will be skipped.")
        return 0

    data_size = len(data)
    print(f"-> [Reddit Dispatcher] DataFrame received. Size: {data_size}")

    if data.empty:
        print("-> [Reddit Dispatcher] DataFrame is empty. Skipping DB load.")
        return 0

    print(
        f"-> [Reddit Dispatcher] Converting DataFrame of size {data_size} to list of records..."
    )

    records = data.to_dict("records")

    print(
        f"-> [Reddit Dispatcher] Dispatching {len(records)} records for DB insertion via Celery."
    )

    result = insert_reddit_posts_task.apply(args=[records])

    posts_inserted = result.get()

    print(
        f"-> [Reddit Dispatcher] Celery task completed. {posts_inserted} Reddit posts inserted."
    )

    return posts_inserted


@task
def load_alpaca_data(data_frame: pd.DataFrame, tickers: List):
    data = data_frame.to_dict("records")

    print(f"-> Starting parallel loading of {len(data)} records.")

    ticker_cache: Dict[str, Any] = {}

    with get_db() as session:
        try:
            ticker_cache = CompanyService.get_ids_by_tickers(session, tickers)
        except Exception as e:
            print(f"")
            raise e

    found_count = len(ticker_cache)
    if found_count < len(tickers):
        missing_tickers = [t for t in tickers if t not in ticker_cache]
        print(
            f"WARNING: Could not find Company records for: {', '.join(missing_tickers)}. Associated bars will be skipped."
        )

    chunks = np.array_split(data, 4)

    task_signatures = [
        insert_stock_task.s(stocks.tolist(), ticker_cache) for stocks in chunks
    ]
    task_group = group(task_signatures)
    result = task_group.apply_async()

    try:
        batch_results = result.get(timeout=60)
        total_stock_length = sum(batch_results)
        print(f"<- Finished loading {total_stock_length} of stock data.")
        return True
    except Exception as e:
        print(f"Error retrieving Celery results (Timeout or Task Failure): {e}")
        print(f"Retrying the task.")
        raise e


# ----------------------------------------
# CELERY TASKS
# ----------------------------------------
@app.task(
    name="insert_articles_to_db",
    bind=True,
    autoretry_for=(RequestException,),
    retry_kwargs={"max_retries": 3, "countdown": 30},
    soft_time_limit=300,
    time_limit=330,
)
def insert_articles_task(self, records: List[Dict[str, Any]], category: str) -> int:
    articles_count_received = len(records)
    print(f"-> [Worker] Received {articles_count_received} records for insertion.")

    if articles_count_received == 0:
        print("-> [Worker] Skipping DB insertion, 0 records received.")
        return 0

    with get_db() as session:
        articles_count = 0

        incoming_article_urls = {r.get("url") for r in records if r.get("url")}
        existing_articles = ArticleService.get_existing_urls(
            session, incoming_article_urls
        )
        existing_articles_map = {article.url: article for article in existing_articles}

        articles = []
        for record in records:
            url = record.get("url")

            if url in existing_articles_map:
                continue

            article = Article(
                author=record.get("author"),
                title=record.get("title"),
                content=record.get("content"),
                title_cleaned=record.get("title_cleaned"),
                content_cleaned=record.get("content_cleaned"),
                sentiment_strategy=category,
                published_at=record.get("published_at"),
                source_name=record.get("source_name"),
                url=record.get("url"),
            )
            articles.append(article)

        articles_count = len(articles)
        print(
            f"-> [Worker] Skipped {len(records) - articles_count} articles, cause they already exists."
        )

        try:
            ArticleService.create_many(session, articles)
            session.commit()
            print(
                f"-> [Worker] Successfully loaded {articles_count} articles to database."
            )
            return articles_count

        except Exception as e:
            session.rollback()
            print(f"-> [Worker] Error loading data: {e}")
            # Re-raise the exception so Celery can handle retries
            raise e


@app.task(
    name="insert_reddit_posts_to_db",
    bind=True,
    autoretry_for=(RequestException,),
    retry_kwargs={"max_retries": 2, "countdown": 30},
    soft_time_limit=300,
    time_limit=330,
)
def insert_reddit_posts_task(self, records: List[Dict[str, Any]]) -> int:
    posts_count_received = len(records)
    print(f"-> [Reddit Worker] Received {posts_count_received} records for insertion.")

    if posts_count_received == 0:
        print("-> [Reddit Worker] Skipping DB insertion, 0 records received.")
        return 0

    with get_db() as session:
        incoming_reddit_ids = {
            r.get("reddit_id") for r in records if r.get("reddit_id")
        }

        existing_reddit_ids_query = RedditService.get_existing_posts(
            session, incoming_reddit_ids
        )

        existing_reddit_ids = set(existing_reddit_ids_query)
        print(
            f"-> [Reddit Worker] Found {len(existing_reddit_ids)} duplicate Reddit posts in DB."
        )

        incoming_article_urls = {
            r.get("article_url")
            for r in records
            if not r.get("is_text_post") and r.get("article_url")
        }

        existing_articles_map = {}
        if incoming_article_urls:
            existing_articles = ArticleService.get_existing_urls(
                session, incoming_article_urls
            )
            existing_articles_map = {
                article.url: article for article in existing_articles
            }

        print(
            f"-> [Reddit Worker] Found {len(existing_articles_map)} existing Articles in DB."
        )

        articles_to_insert = []
        posts_to_insert = []
        for record in records:
            reddit_id = record.get("reddit_id")

            if reddit_id in existing_reddit_ids:
                continue

            article_url = record.get("article_url")
            is_text_post = record.get("is_text_post", True)
            article_obj = None

            if not is_text_post and article_url:
                if article_url in existing_articles_map:
                    article_obj = existing_articles_map[article_url]
                else:
                    article_obj = Article(
                        author=record.get("article_author"),
                        title=record.get("article_headline"),
                        content=record.get("article_content"),
                        title_cleaned=record.get("article_headline_cleaned"),
                        content_cleaned=record.get("article_content_cleaned"),
                        sentiment_strategy=record.get("article_category"),
                        published_at=record.get("article_published_at"),
                        source_name=record.get("article_publisher"),
                        url=record.get("article_url"),
                    )
                    articles_to_insert.append(article_obj)
                    # Add to map/cache for subsequent posts in this batch linking to the same article
                    existing_articles_map[article_url] = article_obj

            reddit_post = RedditPost(
                reddit_id=reddit_id,
                subreddit=record.get("subreddit"),
                author=record.get("author"),
                title=record.get("title"),
                body_text=record.get("body_text"),
                score=record.get("score"),
                number_of_comments=record.get("number_of_comments"),
                is_text_post=is_text_post,
                subreddit_category=record.get("subreddit_category"),
                upvote_ratio=record.get("upvote_ratio"),
                published_at=record.get("published_at"),
                reddit_post_url=record.get("reddit_post_url"),
                article=article_obj,
            )

            posts_to_insert.append(reddit_post)

        total_objects_to_commit = len(articles_to_insert) + len(posts_to_insert)
        inserted_posts_count = len(posts_to_insert)

        try:
            if articles_to_insert:
                ArticleService.create_many(session, articles_to_insert)

            RedditService.create_many(session, posts_to_insert)
            session.commit()
            print(
                f"-> [Reddit Worker] Committed {total_objects_to_commit} total objects "
                f"({len(articles_to_insert)} Articles, {inserted_posts_count} Reddit Posts) to database."
            )
            return inserted_posts_count

        except Exception as e:
            session.rollback()
            print(
                f"-> [Reddit Worker] Error loading Reddit data. Transaction rolled back: {e}"
            )
            raise e


@app.task(
    name="insert_stock_to_db",
    bind=True,
    autoretry_for=(RequestException,),
    retry_kwargs={"max_retries": 2, "countdown": 30},
    soft_time_limit=300,
    time_limit=330,
)
def insert_stock_task(self, records: Dict, ticker_cache: Dict) -> int:
    stocks = []
    for record in records:
        ticker = record.get("ticker")

        company_id = ticker_cache.get(ticker)

        if not company_id:
            continue

        stocks.append(
            StockBar(
                company_id=company_id,
                timestamp=record.get("timestamp"),
                open_price=record.get("open"),
                high_price=record.get("high"),
                low_price=record.get("low"),
                close_price=record.get("close"),
                volume=record.get("volume"),
                trade_count=record.get("trade_count"),
                vwap=record.get("vwap"),
            )
        )

    with get_db() as session:
        try:
            StockBarService.create_many(session, stocks)
            session.commit()

            stock_length = len(stocks)
            print(f"Successfully loaded {stock_length} stocks to database.")
            return stock_length
        except Exception as e:
            session.rollback()
            print(f"Error loading stock bar data. Transaction rolled back: {e}")
            raise e
