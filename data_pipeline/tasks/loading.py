from prefect import task

@task
def load_news_data(data):
    print(f"Loading {len(data)} News records...")
    return True

@task
def load_praw_data(data):
    print(f"Loading {len(data)} PRAW records...")
    return True

@task
def load_alpaca_data(data):
    print(f"Loading {len(data)} Alpaca records...")
    return True
