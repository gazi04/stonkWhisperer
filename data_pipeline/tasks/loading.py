from prefect import task
from core.database import get_db
from models.article import Article
import pandas as pd

@task
def load_news_data(data: pd.DataFrame, category: str):
    session = get_db() 

    try:
        records = data.to_dict("records")
        
        articles = []
        for record in records:
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
        
        # Bulk insert
        session.add_all(articles)
        session.commit()
        
        print(f"Successfully loaded {len(articles)} articles to database")
        return len(articles)
        
    except Exception as e:
        session.rollback()
        print(f"Error loading data: {e}")
        raise e
    finally:
        session.close()

@task
def load_praw_data(data):
    print(f"Loading {len(data)} PRAW records...")
    return True

@task
def load_alpaca_data(data):
    print(f"Loading {len(data)} Alpaca records...")
    return True
