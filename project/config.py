import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Aiven Valkey
    valkey_host: str = "localhost"
    valkey_port: int = 6380
    valkey_password: str = ""
    valkey_ssl: bool = True

    # Aiven PostgreSQL
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_user: str = "avnadmin"
    pg_password: str = ""
    pg_database: str = "sentiment"

    # Kafka (Updated for SASL)
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_user: str = "avnadmin"        # Matches KAFKA_USER in .env
    kafka_password: str = ""           # Matches KAFKA_PASSWORD in .env
    kafka_topic_news: str = "news.raw"
    kafka_topic_sentiment: str = "news.sentiment"

    # App
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    # Pydantic v2 configuration style
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
