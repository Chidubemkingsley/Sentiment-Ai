import os
import json
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

import redis
import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Producer

from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global connections
valkey_client: Optional[redis.Redis] = None
pg_pool: Optional[asyncpg.Pool] = None
kafka_producer: Optional[Producer] = None

TICKERS = ["AAPL", "TSLA", "BTC", "ETH", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "SPY"]

# Must match thresholds in sentiment_consumer.py and storage_consumer.py
BULLISH_THRESHOLD = 0.05
BEARISH_THRESHOLD = -0.05


def mood_label(score: float) -> str:
    if score > BULLISH_THRESHOLD:
        return "bullish"
    elif score < BEARISH_THRESHOLD:
        return "bearish"
    return "neutral"


async def init_connections():
    global valkey_client, pg_pool, kafka_producer

    # 1. Valkey
    try:
        valkey_client = redis.Redis(
            host=settings.valkey_host,
            port=settings.valkey_port,
            password=settings.valkey_password if settings.valkey_password else None,
            ssl=settings.valkey_ssl,
            decode_responses=True
        )
        valkey_client.ping()
        logger.info("✅ Connected to Valkey")
    except Exception as e:
        logger.error(f"❌ Valkey connection failed: {e}")

    # 2. PostgreSQL
    try:
        pg_pool = await asyncpg.create_pool(
            host=settings.pg_host,
            port=settings.pg_port,
            user=settings.pg_user,
            password=settings.pg_password,
            database=settings.pg_database,
            ssl='require',
            min_size=1,
            max_size=10
        )
        logger.info("✅ Connected to PostgreSQL")
    except Exception as e:
        logger.error(f"❌ PostgreSQL CONNECTION FAILED: {e}")
        pg_pool = None

    # 3. Kafka
    try:
        kafka_conf = {
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'SCRAM-SHA-256',
            'sasl.username': settings.kafka_user,
            'sasl.password': settings.kafka_password,
            'ssl.ca.location': os.path.join(os.path.dirname(__file__), 'ca.pem'),
        }
        kafka_producer = Producer(kafka_conf)
        kafka_producer.list_topics(timeout=5.0)
        logger.info("✅ Connected to Kafka")
    except Exception as e:
        logger.error(f"❌ Kafka connection failed: {e}")
        kafka_producer = None


async def close_connections():
    global valkey_client, pg_pool, kafka_producer
    if valkey_client:
        valkey_client.close()
    if pg_pool:
        await pg_pool.close()
    if kafka_producer:
        kafka_producer.flush()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_connections()
    yield
    await close_connections()


app = FastAPI(title="SentimentArena API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- API ROUTES ---

@app.get("/", response_class=HTMLResponse)
async def root():
    static_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    if os.path.exists(static_path):
        with open(static_path, "r") as f:
            return f.read()
    return "<h1>SentimentArena API is Running</h1>"


@app.get("/api/sentiment")
async def get_sentiment():
    if not valkey_client:
        return JSONResponse({"error": "Valkey not connected"}, status_code=503)
    result = {}
    for symbol in TICKERS:
        try:
            data = valkey_client.get(f"sentiment:{symbol}")
            result[symbol] = json.loads(data) if data else {
                "score": 0,
                "label": "neutral",
                "articles": 0,
                "bullCount": 0,
                "bearCount": 0,
                "history": [],
                "latestTitle": ""
            }
        except Exception:
            result[symbol] = {"score": 0, "label": "neutral", "articles": 0}
    return result


@app.get("/api/stats")
async def get_stats():
    if not valkey_client:
        return JSONResponse({"error": "Valkey not connected"}, status_code=503)
    total_articles = 0
    bullish, bearish, neutral = 0, 0, 0
    for symbol in TICKERS:
        data = valkey_client.get(f"sentiment:{symbol}")
        if data:
            d = json.loads(data)
            total_articles += d.get("articles", 0)
            lbl = d.get("label", "neutral")
            if lbl == "bullish":
                bullish += 1
            elif lbl == "bearish":
                bearish += 1
            else:
                neutral += 1
    return {
        "total_articles": total_articles,
        "bullish": bullish,
        "bearish": bearish,
        "neutral": neutral
    }


@app.get("/api/news/{symbol}")
async def get_news(symbol: str, limit: int = 20):
    if not pg_pool:
        return JSONResponse({"error": "PostgreSQL not connected"}, status_code=503)
    try:
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT title, source, url, published_at, sentiment_score, sentiment_label
                FROM articles
                WHERE symbol = $1
                ORDER BY published_at DESC
                LIMIT $2
                """,
                symbol.upper(), limit
            )
            return [
                {
                    "title": r["title"],
                    "source": r["source"],
                    "url": r["url"],
                    "time": r["published_at"].isoformat() if r["published_at"] else None,
                    "sentiment_score": r["sentiment_score"],
                    "sentiment_label": r["sentiment_label"],
                }
                for r in rows
            ]
    except asyncpg.exceptions.UndefinedTableError:
        return JSONResponse({"error": "Database tables not initialized."}, status_code=500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/mood")
async def get_market_mood():
    if not valkey_client:
        return JSONResponse({"error": "Valkey not connected"}, status_code=503)
    scores = []
    for symbol in TICKERS:
        data = valkey_client.get(f"sentiment:{symbol}")
        if data:
            try:
                scores.append(json.loads(data).get("score", 0))
            except Exception:
                pass
    avg = round(sum(scores) / len(scores), 3) if scores else 0
    return {"score": avg, "label": mood_label(avg)}


# --- WEBSOCKETS & BROADCAST ---

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        dead = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                dead.append(connection)
        for conn in dead:
            self.disconnect(conn)


manager = ConnectionManager()


@app.post("/internal/broadcast")
async def internal_broadcast(request: Request):
    body = await request.json()
    symbol, data = body.get("symbol"), body.get("data")
    if symbol and data:
        await manager.broadcast({"type": "update", "symbol": symbol, "payload": data})
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.app_host, port=settings.app_port)