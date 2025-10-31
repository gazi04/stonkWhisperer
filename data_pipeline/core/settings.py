from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file="../../.env")

    news_api_key: str

    alpaca_api_key: str
    alpaca_secret_key: str

    database_url: str

    celery_broker_url: str
    celery_result_backend: str

    reddit_client_id: str
    reddit_client_secret: str
    reddit_username: str
    reddit_password: str
    reddit_user_agent: str
