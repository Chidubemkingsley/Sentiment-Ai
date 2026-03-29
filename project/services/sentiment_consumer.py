import json
import time
import logging
import os
import requests
from datetime import datetime
from typing import Optional

import redis
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from textblob import TextBlob

from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TICKERS = ["AAPL", "TSLA", "BTC", "ETH", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "SPY"]

# Lowered thresholds so more headlines register as bullish/bearish
BULLISH_THRESHOLD = 0.05
BEARISH_THRESHOLD = -0.05
HISTORY_WINDOW = 20


def sentiment_label(score: float) -> str:
    if score > BULLISH_THRESHOLD:
        return "bullish"
    elif score < BEARISH_THRESHOLD:
        return "bearish"
    return "neutral"


class SentimentConsumer:
    def __init__(self):
        self.consumer: Optional[KafkaConsumer] = None
        self.valkey: Optional[redis.Redis] = None
        self._connect_kafka()
        self._connect_valkey()

    def _connect_kafka(self):
        try:
            ca_path = os.path.join(os.getcwd(), "ca.pem")
            if not os.path.exists(ca_path):
                logger.error(f"❌ ca.pem NOT FOUND at {ca_path}")

            self.consumer = KafkaConsumer(
                settings.kafka_topic_news,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                security_protocol="SASL_SSL",
                sasl_mechanism="SCRAM-SHA-256",
                sasl_plain_username=settings.kafka_user,
                sasl_plain_password=settings.kafka_password,
                ssl_cafile=ca_path,
                group_id='sentiment-consumer',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                request_timeout_ms=30001,
                session_timeout_ms=30000
            )
            logger.info(f"✅ Connected to Kafka topic {settings.kafka_topic_news} with SASL")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            self.consumer = None

    def _connect_valkey(self):
        try:
            self.valkey = redis.Redis(
                host=settings.valkey_host,
                port=settings.valkey_port,
                password=settings.valkey_password if settings.valkey_password else None,
                ssl=settings.valkey_ssl,
                decode_responses=True
            )
            self.valkey.ping()
            logger.info(f"✅ Connected to Valkey at {settings.valkey_host}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Valkey: {e}")
            self.valkey = None

    def analyze_sentiment(self, text: str) -> tuple[float, str]:
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        # Also factor in subjectivity — highly subjective text carries more signal
        subjectivity = blob.sentiment.subjectivity
        # Boost polarity slightly when text is subjective (opinion-driven headlines)
        adjusted = polarity * (1 + subjectivity * 0.3)
        adjusted = max(-1.0, min(1.0, adjusted))
        label = sentiment_label(adjusted)
        return round(adjusted, 3), label

    def update_sentiment(self, symbol: str, news: dict):
        if not self.valkey:
            return

        score, label = self.analyze_sentiment(news.get('title', ''))
        key = f"sentiment:{symbol}"

        existing = self.valkey.get(key)
        if existing:
            data = json.loads(existing)

            # Maintain a rolling window of recent scores
            history = data.get('history', [])
            history.append(score)
            if len(history) > HISTORY_WINDOW:
                history = history[-HISTORY_WINDOW:]

            # Score is the average of the recent window — NOT a lifetime average
            # This prevents old scores from permanently diluting the current signal
            new_score = round(sum(history) / len(history), 3)
            new_label = sentiment_label(new_score)

            articles = data.get('articles', 0) + 1
            data.update({
                'score': new_score,
                'label': new_label,
                'articles': articles,
                'bullCount': data.get('bullCount', 0) + (1 if label == 'bullish' else 0),
                'bearCount': data.get('bearCount', 0) + (1 if label == 'bearish' else 0),
                'latestTitle': news.get('title', ''),
                'history': history,
                'updated_at': datetime.utcnow().isoformat()
            })
        else:
            data = {
                'score': score,
                'label': label,
                'articles': 1,
                'bullCount': 1 if label == 'bullish' else 0,
                'bearCount': 1 if label == 'bearish' else 0,
                'latestTitle': news.get('title', ''),
                'history': [score],
                'updated_at': datetime.utcnow().isoformat()
            }

        self.valkey.set(key, json.dumps(data))
        logger.info(f"Updated {symbol}: score={data['score']} label={data['label']} articles={data['articles']}")

        # Broadcast change to FastAPI WebSocket clients
        try:
            requests.post(
                f"http://localhost:{settings.app_port}/internal/broadcast",
                json={"symbol": symbol, "data": data},
                timeout=0.2
            )
        except Exception as e:
            logger.debug(f"UI Broadcast failed: {e}")

    def run(self):
        if not self.consumer:
            logger.error("❌ Kafka consumer not initialized, exiting.")
            return
        logger.info("🚀 Starting sentiment consumer loop...")
        try:
            for message in self.consumer:
                news = message.value
                symbol = news.get('symbol')
                if symbol:
                    self.update_sentiment(symbol, news)
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            self.close()

    def close(self):
        if self.consumer:
            self.consumer.close()
        if self.valkey:
            self.valkey.close()


if __name__ == "__main__":
    try:
        import textblob
        textblob.download_corpora()
    except Exception:
        pass
    consumer = SentimentConsumer()
    consumer.run()