from pandas import DataFrame
from prefect import task

import pandas as pd
import re

@task
def transform_news_data(data: dict):
    print("Transforming News data...")
    data_frame = DataFrame.from_dict(data)

    data_frame["source"] = data_frame["source"].str.get("name")
    print("Extract name from the source")

    data_frame = handle_missing_values(data_frame)
    print("Handled missing values ")

    data_frame = (
        data_frame
        .pipe(handle_missing_values)
        .rename(columns={
            "source": "source_name",
            "publishedAt": "published_at"
        })
        .drop(columns=[col for col in ["urlToImage", "description"] if col in DataFrame.from_dict(data).columns])
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
def transform_praw_data(data):
    print("Transforming PRAW data...")
    return data


@task
def transform_alpaca_data(data):
    print("Transforming Alpaca data...")
    return data

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
    cat_col = [col for col in data.columns if data[col].dtype == "object"]
    num_col = [col for col in data.columns if data[col].dtype != "object"]
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

    print(f"Dropped {original_rows - len(dt_cleaned)} rows due to missing critical data.")
    return dt_cleaned

def clean_text_for_nlp(text: str) -> str:
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text).lower()
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
    text = re.sub(r"\[\+\d+ chars\]", "", text).strip() # Removes from 'content' remnants like "[+4561 chars]" or similar brackets
    text = re.sub(r"[^\w\s.!?]", "", text)              # Removes non-alphanumeric characters
    text = re.sub(r"\s+", " ", text).strip()

    return text

# 2. FOR THE REDDIT TRANSFORMATION

# 3. FOR THE ALPACA API TRANSFORMATION
