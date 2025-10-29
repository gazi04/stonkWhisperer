from prefect import task
from sqlalchemy import select
from typing import Any, Dict, List
import uuid

from celery_app import app
from core.database import get_db
from models.article import Article
from models.reddit_post import RedditPost

import pandas as pd


@task(name="Dispatch DB Load Task")
def dispatch_load_news_data(data: pd.DataFrame, category: str):
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
def load_praw_data(data: pd.DataFrame):
    """
    Loads Reddit posts from a DataFrame into the database.
    """
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
            exists = (
                session.execute(select(Article).where(Article.url == record.get("url")))
                .scalars()
                .first()
            )

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


@app.task(name="insert_reddit_posts_to_db")
def insert_reddit_posts_task(records: List[Dict[str, Any]]) -> int:
    posts_count_received = len(records)
    print(f"-> [Reddit Worker] Received {posts_count_received} records for insertion.")

    if posts_count_received == 0:
        print("-> [Reddit Worker] Skipping DB insertion, 0 records received.")
        return 0

    session = next(get_db())
    posts_count = 0

    try:
        posts = []
        for record in records:
            # Check if post already exists using reddit_id
            reddit_post_exists = (
                session.execute(
                    select(RedditPost).where(
                        RedditPost.reddit_id == record.get("reddit_id")
                    )
                )
                .scalars()
                .first()
            )

            if reddit_post_exists is not None:
                print(
                    f"-> [Reddit Worker] Post with reddit_id {record.get('reddit_id')} already exists. Skipping."
                )
                continue

            reddit_post = RedditPost(
                reddit_id=record.get("reddit_id"),
                subreddit=record.get("subreddit"),
                author=record.get("author"),
                title=record.get("title"),
                body_text=record.get("body_text"),
                score=record.get("score"),
                number_of_comments=record.get("number_of_comments"),
                is_text_post=record.get("is_text_post"),
                subreddit_category=record.get("subreddit_category"),
                upvote_ratio=record.get("upvote_ratio"),
                published_at=record.get("published_at"),
                reddit_post_url=record.get("reddit_post_url"),
            )

            posts.append(reddit_post)

            if record.get("is_text_post"):
                continue

            article_exists = (
                session.execute(
                    select(Article).where(
                        Article.url == record.get("article_url")
                    )
                )
                .scalars()
                .first()
            )

            if article_exists:
                reddit_post.article = article_exists
                continue


            article = Article(
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
            reddit_post.article = article
            posts.append(article)

        posts_count = len(posts)

        # Bulk insert
        session.add_all(posts)
        session.commit()

        print(
            f"-> [Reddit Worker] Successfully loaded {posts_count} Reddit posts to database."
        )
        return posts_count

    except Exception as e:
        session.rollback()
        print(f"-> [Reddit Worker] Error loading Reddit data: {e}")
        # Re-raise the exception so Celery can handle retries if configured
        raise e
    finally:
        session.close()
