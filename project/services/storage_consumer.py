import json
import logging
import asyncio
import os
from datetime import datetime
from typing import Optional

import asyncpg
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from textblob import TextBlob

from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Match thresholds with sentiment_consumer.py
BULLISH_THRESHOLD = 0.05
BEARISH_THRESHOLD = -0.05


def sentiment_label(score: float) -> str:
    if score > BULLISH_THRESHOLD:
        return "bullish"
    elif score < BEARISH_THRESHOLD:
        return "bearish"
    return "neutral"


class StorageConsumer:
    def __init__(self):
        self.consumer: Optional[KafkaConsumer] = None
        self.pool: Optional[asyncpg.Pool] = None
        self._connect_kafka()

    def _connect_kafka(self):
        try:
            ca_path = os.path.join(os.getcwd(), "ca.pem")

            if not os.path.exists(ca_path):
                logger.error(f"❌ ca.pem NOT FOUND at {ca_path}. SSL handshake may fail.")

            self.consumer = KafkaConsumer(
                settings.kafka_topic_news,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                security_protocol="SASL_SSL",
                sasl_mechanism="SCRAM-SHA-256",
                sasl_plain_username=settings.kafka_user,
                sasl_plain_password=settings.kafka_password,
                ssl_cafile=ca_path,
                group_id='storage-consumer',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                request_timeout_ms=30001,
                session_timeout_ms=30000
            )
            logger.info(f"✅ Connected to Kafka topic {settings.kafka_topic_news} (SASL)")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            self.consumer = None

    async def _connect_postgres(self):
        try:
            self.pool = await asyncpg.create_pool(
                host=settings.pg_host,
                port=settings.pg_port,
                user=settings.pg_user,
                password=settings.pg_password,
                database=settings.pg_database,
                ssl='require',
                min_size=2,
                max_size=10
            )
            await self._init_tables()
            logger.info(f"✅ Connected to PostgreSQL at {settings.pg_host}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            self.pool = None

    async def _init_tables(self):
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    title TEXT NOT NULL,
                    source VARCHAR(100),
                    url TEXT,
                    published_at TIMESTAMP,
                    fetched_at TIMESTAMP NOT NULL,
                    sentiment_score REAL,
                    sentiment_label VARCHAR(20),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sentiment_history (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    score REAL NOT NULL,
                    label VARCHAR(20) NOT NULL,
                    article_count INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_articles_symbol ON articles(symbol)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sentiment_history_symbol ON sentiment_history(symbol)"
            )

    def analyze_sentiment(self, text: str) -> tuple[float, str]:
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        # Factor in subjectivity to boost opinionated headlines (matches sentiment_consumer)
        subjectivity = blob.sentiment.subjectivity
        adjusted = polarity * (1 + subjectivity * 0.3)
        adjusted = max(-1.0, min(1.0, adjusted))
        label = sentiment_label(adjusted)
        return round(adjusted, 3), label

    async def store_article(self, news: dict):
        if not self.pool:
            return
        score, label = self.analyze_sentiment(news.get('title', ''))

        def parse_ts(ts_str):
            try:
                return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            except Exception:
                return datetime.utcnow()

        pub_at = parse_ts(news['published_at']) if news.get('published_at') else None
        fetch_at = parse_ts(news.get('fetched_at', ''))

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO articles
                    (symbol, title, source, url, published_at, fetched_at, sentiment_score, sentiment_label)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                news.get('symbol'),
                news.get('title'),
                news.get('source'),
                news.get('url'),
                pub_at,
                fetch_at,
                score,
                label
            )
            logger.info(f"✅ Stored article for {news.get('symbol')} — {label} ({score})")

    async def run(self):
        await self._connect_postgres()

        if not self.consumer:
            logger.error("❌ Kafka consumer not initialized, exiting.")
            return

        logger.info("🚀 Starting storage consumer loop...")
        try:
            while True:
                msg_pack = self.consumer.poll(timeout_ms=1000)
                for tp, messages in msg_pack.items():
                    for message in messages:
                        await self.store_article(message.value)
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"❌ Loop error: {e}")
        finally:
            await self.close()

    async def close(self):
        if self.consumer:
            self.consumer.close()
        if self.pool:
            await self.pool.close()


if __name__ == "__main__":
    consumer = StorageConsumer()
    try:
        asyncio.run(consumer.run())
    except KeyboardInterrupt:
        logger.info("👋 Shutting down storage consumer...")