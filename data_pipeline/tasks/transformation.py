from prefect import task

@task
def transform_news_data(data):
    print("Transforming News data...")
    return data

@task
def transform_praw_data(data):
    print("Transforming PRAW data...")
    return data

@task
def transform_alpaca_data(data):
    print("Transforming Alpaca data...")
    return data
