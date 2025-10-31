from typing import Dict, List, Tuple
from pandas import DataFrame
from prefect import task

import pandas as pd
import re


# ----------------------------------------
# PREFECT TASKS
# ----------------------------------------
@task
def transform_news_data(data: dict):
    print("Transforming News data...")
    data_frame = DataFrame.from_dict(data)

    data_frame["source"] = data_frame["source"].str.get("name")
    print("Extract name from the source")

    data_frame = handle_missing_values(data_frame)
    print("Handled missing values ")

    data_frame = (
        data_frame.pipe(handle_missing_values)
        .rename(
            columns={
                "source": "source_name",
                "publishedAt": "published_at",
            }
        )
        .drop(
            columns=[
                col
                for col in ["urlToImage", "description"]
                if col in DataFrame.from_dict(data).columns
            ]
        )
    )
    print("Renamed and dropped columns.")

    initial_rows = len(data_frame)
    data_frame = data_frame.drop_duplicates(subset=["url"], keep="first")
    print(f"Removed {initial_rows - len(data_frame)} duplicate rows.")

    data_frame["title_cleaned"] = data_frame["title"].apply(clean_text_for_nlp)
    data_frame["content_cleaned"] = data_frame["content"].apply(clean_text_for_nlp)
    print("Created 'title_cleaned' and 'content_cleaned' for prediction model.")

    print("Transformation is completed.")

    data_analysis(data_frame)

    return data_frame


@task
def transform_praw_data(data: list[dict]):
    print("Transforming PRAW data...")
    data_frame = DataFrame(data)

    data_frame["published_at"] = pd.to_datetime(
        data_frame["published_at"], unit="s", utc=True
    )
    print("Standardized 'published_at' from Unix timestamp to UTC datetime.")

    data_frame = data_frame.rename(
        columns={
            "selftext": "body_text",
            "num_comments": "number_of_comments",
            "link_flair_text": "subreddit_category",
            "permalink": "reddit_post_url",
            "url": "article_url",
        }
    )
    print("Renamed columns")

    # Insterting into the database article_category as a list will raise and error
    # so we turning it into a string
    data_frame['article_category'] = data_frame['article_category'].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else x
    )

    initial_rows = len(data_frame)
    data_frame = data_frame.drop_duplicates(
        subset=["reddit_id", "reddit_post_url"], keep="first"
    )
    print(f"Removed {initial_rows - len(data_frame)} duplicate rows.")

    initial_rows = len(data_frame)
    data_frame = (
        data_frame.dropna(
            subset=[
                "reddit_id",
                "subreddit",
                "published_at",
            ],
            how="any"
        )
    )
    print(
        f"Removed {initial_rows - len(data_frame)} rows without a reddit_id or a subreddit or a published_at."
    )

    data_frame = data_frame.fillna(
        {
            "body_text": "No text",
            "content": "No text",
            "subreddit_category": "No category",
            "score": 0,
            "number_of_comments": 0,
            "upvote_ratio": 0.5,
            "article_author": "Unknown Author",
            "article_publisher": "Unknown Publisher",
            "article_headline": "No Title",
            "article_content": "No Content",
        }
    )
    data_frame["score"] = data_frame["score"].astype(int)
    data_frame["number_of_comments"] = data_frame["number_of_comments"].astype(int)

    print("Applying custom filter for non-text posts with null article data...")
    # ♻️ todo: better solution instead of removing the data
    # add a new column to the reddit_posts table that tells if the no text post is fetched or not
    # Removing the post which are not text posts and their article_published_at column is empty
    data_frame = data_frame[~((data_frame['is_text_post'] == False) & (data_frame['article_published_at'].isna() | data_frame['article_published_at'].eq('')))]
    
    data_frame["article_headline_cleaned"] = data_frame["article_headline"].apply(
        clean_text_for_nlp
    )
    data_frame["article_content_cleaned"] = data_frame["article_content"].apply(
        clean_text_for_nlp
    )
    print(
        "Created 'article_headline_cleaned' and 'article_content_cleaned' for prediction model."
    )

    data_analysis(data_frame)
    return data_frame


@task
def transform_alpaca_data(data) -> Tuple:
    print("Transforming Alpaca data...")
    data_frame = DataFrame(data)

    print("Ensure data types are correct")
    data_frame["timestamp"] = pd.to_datetime(data_frame["timestamp"])

    numerical_columns = ["open", "high", "low", "close", "volume", "trade_count", "vwap"]
    data_frame.loc[:, numerical_columns] = data_frame[numerical_columns].apply(
        pd.to_numeric,
        errors="coerce" # Turns invalid data into NaN which is handled below
    )

    data_frame["symbol"] = data_frame["symbol"].astype("category")

    print("Sorting data frame based on timestamp and symbol")
    data_frame = data_frame.sort_values(by=['symbol', 'timestamp'])

    price_cols = ['open', 'high', 'low', 'close', 'vwap']
    volume_cols = ['volume', 'trade_count']

    print("Handling emtpy values")
    data_frame[price_cols] = data_frame.groupby("symbol")[price_cols].ffill()
    data_frame[volume_cols] = data_frame[volume_cols].fillna(0)
    data_frame[price_cols] = data_frame.groupby("symbol")[price_cols].bfill()

    initial_rows = len(data_frame)
    data_frame = data_frame.dropna(subset=price_cols)
    final_rows = len(data_frame)

    if initial_rows > final_rows:
        print(f"-> [Transform] WARNING: Dropped {initial_rows - final_rows} rows with non-fixable null prices.")

    print(f"-> [Transform] Transformation complete. Returning {final_rows} clean bars.")

    data_frame = data_frame.rename(columns={"symbol": "ticker"})
    data_analysis(data_frame)

    unique_symbols_list = data_frame["ticker"].unique().tolist()
    dictionary_symbols = {symbole: symbole for symbole in unique_symbols_list}

    return (data_frame.to_dict("records"), dictionary_symbols)


# ----------------------------------------
# HELPER METHODS
# ----------------------------------------


# 1. FOR THE NEWS API TRANSFORMATION
def data_analysis(data: DataFrame):
    print("Head data")
    print(data.head())

    print("Info data")
    print(data.info())

    print("Check for duplicate rows")
    print(data.duplicated())

    print("Identify data types")
    cat_col = [col for col in data.columns if data[col].dtype == "object" or data[col].dtype == "category"]
    num_col = [col for col in data.columns if data[col].dtype != "object" and data[col].dtype != "category"]
    print("Categorical columns:", cat_col)
    print("Numerical columns:", num_col)

    print("Missing values as percentage")
    print(round((data.isnull().sum() / data.shape[0]) * 100, 2))


def handle_missing_values(dt: DataFrame) -> DataFrame:
    original_rows = len(dt)

    dt["author"] = dt["author"].fillna("No Author")
    dt["title"] = dt["title"].fillna("Untitled Article")
    dt["content"] = dt["content"].fillna(dt["description"].fillna(dt["title"]))

    dt_cleaned = dt.dropna(subset=["content"], how="any")

    print(
        f"Dropped {original_rows - len(dt_cleaned)} rows due to missing critical data."
    )
    return dt_cleaned


def clean_text_for_nlp(text: str) -> str:
    if pd.isna(text) or text is None:
        return ""

    text = str(text).lower()
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
    text = (
        re.sub(r"\[\+\d+ chars\]", "", text).strip()
    )  # Removes from 'content' remnants like "[+4561 chars]" or similar brackets
    text = re.sub(r"[^\w\s.!?]", "", text)  # Removes non-alphanumeric characters
    text = re.sub(r"\s+", " ", text).strip()

    return text


# 2. FOR THE REDDIT TRANSFORMATION

# 3. FOR THE ALPACA API TRANSFORMATION
