import json
import time
import logging
import random
import os
from datetime import datetime
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TICKERS = ["AAPL", "TSLA", "BTC", "ETH", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "SPY"]

MOCK_HEADLINES = {
    "AAPL": [
        "Apple Vision Pro 2 shatters sales expectations in blockbuster launch",
        "Services revenue crushes estimates as Apple ecosystem dominates",
        "iPhone 16 demand surges globally, supply struggles to keep up",
        "Apple AI features disappoint users, growth outlook dims badly",
        "App Store antitrust ruling devastates Apple revenue model",
        "Mac sales collapse as PC market deteriorates sharply",
    ],
    "TSLA": [
        "Tesla FSD breakthrough delights investors, autonomy era begins",
        "Cybertruck recall disaster rattles shareholders amid safety concerns",
        "Tesla Q3 deliveries smash records, bulls rejoice",
        "Elon Musk distraction fears hammer Tesla stock to new lows",
        "Tesla energy division surges, Powerwall demand explodes",
        "Price war devastates Tesla margins, profitability crumbles",
    ],
    "BTC": [
        "Bitcoin ETF inflows smash monthly record, institutional flood continues",
        "Crypto crash wipes billions in brutal overnight selloff",
        "Bitcoin surges past key resistance, halving euphoria builds",
        "Regulatory crackdown crushes crypto markets globally",
        "On-chain data reveals massive accumulation by long-term holders",
        "Bitcoin miners capitulate as difficulty crushes profitability",
    ],
    "ETH": [
        "Ethereum Layer 2 ecosystem explodes past $12B, adoption soars",
        "ETH staking collapse triggers mass exodus from validators",
        "Ethereum upgrade delivers massive scalability gains, devs thrilled",
        "Gas fees spike to unbearable levels, users flee to competitors",
        "DeFi renaissance drives Ethereum transaction volumes to record highs",
        "ETH supply deflation accelerates, scarcity narrative strengthens",
    ],
    "MSFT": [
        "Microsoft Copilot dominates enterprise AI adoption, revenue surges",
        "Azure cloud outage cripples global services, clients furious",
        "Microsoft gaming division hits record revenue on strong titles",
        "Teams usage declines sharply as enterprise cuts spending",
        "Azure growth crushes estimates, cloud dominance expands aggressively",
        "Microsoft layoffs signal deep trouble in core business units",
    ],
    "NVDA": [
        "Nvidia Blackwell demand crushes all forecasts, backlog explodes",
        "Data center slowdown hammers Nvidia outlook, shares tumble",
        "Nvidia H100 allocation wars drive prices through the roof",
        "Competition from AMD and Intel threatens Nvidia GPU dominance",
        "Nvidia data center revenue triples, AI boom shows no signs of stopping",
        "Export restrictions decimate Nvidia China revenue outlook",
    ],
    "GOOGL": [
        "Google Gemini Ultra stuns with breakthrough benchmarks, rivals shaken",
        "Search revenue plunges as AI competitors steal market share",
        "YouTube ad revenue surges to record as content creators flourish",
        "Google antitrust breakup fears rattle investors deeply",
        "Google Cloud growth accelerates, enterprise deals pile up",
        "Android dominance threatened as regulators tighten scrutiny globally",
    ],
    "AMZN": [
        "AWS surges as cloud dominance accelerates beyond expectations",
        "Amazon Prime cancellations spike sharply amid consumer spending squeeze",
        "Amazon advertising revenue explodes, becoming true profit powerhouse",
        "Warehouse automation failures cause costly fulfillment breakdowns",
        "Amazon health division shows explosive growth, investors cheer",
        "Retail margins collapse as competition intensifies across categories",
    ],
    "META": [
        "Meta AI hits 1 billion users in record time, monetization begins",
        "Threads ad collapse devastates revenue outlook, growth stalls badly",
        "Meta Reality Labs breakthrough makes VR mainstream at last",
        "Advertiser boycott hammers Meta revenue in brutal quarter",
        "Instagram Reels dominates short video, TikTok competition crushed",
        "Meta privacy scandal triggers massive regulatory fines globally",
    ],
    "SPY": [
        "S&P 500 soars on broad rally as economic data blows past estimates",
        "PCE shock triggers brutal broad market selloff, recession fears spike",
        "Fed pivot euphoria sends S&P 500 to stunning all-time highs",
        "Banking sector stress spreads fear across entire equity market",
        "Corporate earnings season delivers massive upside surprises broadly",
        "Yield curve inversion deepens, recession probability surges alarmingly",
    ],
}

SOURCES = ["Bloomberg", "Reuters", "CNBC", "The Block", "WSJ", "FT", "CoinDesk", "Barrons"]


class NewsProducer:
    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self._connect()

    def _connect(self):
        try:
            ca_path = os.path.join(os.getcwd(), "ca.pem")

            if not os.path.exists(ca_path):
                logger.error(f"❌ ca.pem NOT FOUND at {ca_path}. Please download it from Aiven Console.")
                self.producer = None
                return

            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                security_protocol="SASL_SSL",
                sasl_mechanism="SCRAM-SHA-256",
                sasl_plain_username=settings.kafka_user,
                sasl_plain_password=settings.kafka_password,
                ssl_cafile=ca_path,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                request_timeout_ms=30000
            )
            logger.info(f"✅ Connected to Kafka (SASL) at {settings.kafka_bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            self.producer = None

    def generate_mock_news(self, symbol: str) -> dict:
        headlines = MOCK_HEADLINES.get(symbol, ["Market update for " + symbol])
        headline = random.choice(headlines)
        return {
            "symbol": symbol,
            "title": headline,
            "source": random.choice(SOURCES),
            "url": f"https://example.com/{symbol.lower()}/{int(time.time())}",
            "published_at": datetime.utcnow().isoformat(),
            "fetched_at": datetime.utcnow().isoformat()
        }

    def produce_news(self, symbol: str):
        if not self.producer:
            logger.warning("Kafka not connected, skipping news production")
            return
        news = self.generate_mock_news(symbol)
        try:
            future = self.producer.send(
                settings.kafka_topic_news,
                value=news,
                key=symbol.encode('utf-8')
            )
            future.get(timeout=10)
            logger.info(f"Produced news for {symbol}: {news['title'][:60]}...")
        except Exception as e:
            logger.error(f"Failed to produce message: {e}")

    def run(self, interval: int = 30):
        logger.info("Starting news producer...")
        while True:
            for symbol in TICKERS:
                self.produce_news(symbol)
                time.sleep(2)
            logger.info(f"Completed round, sleeping for {interval}s")
            time.sleep(interval)

    def close(self):
        if self.producer:
            self.producer.close()


if __name__ == "__main__":
    producer = NewsProducer()
    try:
        producer.run()
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
        producer.close()