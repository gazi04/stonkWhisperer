from prefect import task
from sqlalchemy import select
from typing import Any, Dict, List

from celery_app import app
from core.database import get_db
from models.article import Article

import pandas as pd


@task(name="Dispatch DB Load Task")
def dispatch_load_news_data(data: pd.DataFrame, category: str):
    data_size = len(data)
    print(f"-> [Dispatcher] DataFrame received. Size: {data_size}")

    if data.empty:
        print("-> [Dispatcher] DataFrame is empty. Skipping DB load.")
        return 0

    print(f"-> [Dispatcher] Converting DataFrame of size {data_size} to list of records...")
    
    records = data.to_dict("records")
    
    print(f"-> [Dispatcher] Dispatching {len(records)} records for DB insertion via Celery.")
    
    result = insert_articles_task.apply(args=[records, category])
    
    articles_inserted = result.get()
    
    print(f"-> [Dispatcher] Celery task completed. {articles_inserted} articles inserted.")
    
    return articles_inserted


@task
def load_praw_data(data):
    print(f"Loading {len(data)} PRAW records...")
    return True


@task
def load_alpaca_data(data):
    print(f"Loading {len(data)} Alpaca records...")
    return True


# ----------------------------------------
# CELERY TASKS
# ----------------------------------------
@app.task(name="insert_articles_to_db")
def insert_articles_task(records: List[Dict[str, Any]], category: str) -> int:
    articles_count_received = len(records)
    print(f"-> [Worker] Received {articles_count_received} records for insertion.")
    
    if articles_count_received == 0:
        print("-> [Worker] Skipping DB insertion, 0 records received.")
        return 0

    session = next(get_db())
    articles_count = 0

    try:
        articles = []
        for record in records:
            exists = (session
                      .execute(select(Article).where(Article.url == record.get("url")))
                      .scalars()
                      .first())

            if exists is not None:
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
        
        # Bulk insert
        session.add_all(articles)
        session.commit()
        
        print(f"-> [Worker] Successfully loaded {articles_count} articles to database.")
        return articles_count
        
    except Exception as e:
        session.rollback()
        print(f"-> [Worker] Error loading data: {e}")
        # Re-raise the exception so Celery can handle retries if configured
        raise e
    finally:
        session.close()

