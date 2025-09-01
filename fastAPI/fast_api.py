import os
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import List
from datetime import datetime

import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# --- Setup and Configuration ---
load_dotenv()

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Environment Variables ---
DATABASE_URL = os.getenv("DATABASE_URL")
API_PROMETHEUS_PORT = int(os.getenv("API_PROMETHEUS_PORT", 9108))


# --- Pydantic Data Models ---
# Pydantic models define the schema for your API's input and output.
# This ensures data is structured and validated, a critical best practice.

class CryptoPrice(BaseModel):
    """Data model for a single crypto price record."""
    coin_id: str = Field(..., description="Unique ID of the cryptocurrency (e.g., 'bitcoin').")
    symbol: str = Field(..., description="Trading symbol of the cryptocurrency (e.g., 'btc').")
    current_price: float = Field(..., description="Current price in USD.")
    market_cap: int = Field(..., description="Market capitalization.")
    market_cap_rank: int = Field(..., description="Market capitalization rank.")
    timestamp: datetime = Field(..., description="Timestamp of the data point.")


class NewsSentiment(BaseModel):
    """Data model for a single news sentiment record."""
    title: str = Field(..., description="Title of the news article.")
    sentiment_score: float = Field(..., description="Sentiment score (e.g., from VADER).")
    timestamp: datetime = Field(..., description="Timestamp of the article.")
    link: str = Field(..., description="URL of the news article.")


class OnchainAnomaly(BaseModel):
    """Data model for a single on-chain anomaly record."""
    tx_hash: str = Field(..., description="Transaction hash.")
    from_address: str = Field(..., description="Address of the sender.")
    to_address: str = Field(..., description="Address of the receiver.")
    value_eth: float = Field(..., description="Transaction value in ETH.")
    timestamp: datetime = Field(..., description="Timestamp of the transaction.")


# --- FastAPI Lifespan Events ---
# This is an essential pattern for managing resources like database connections.
# The connection pool is created when the app starts and is gracefully closed when it shuts down.

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the database connection pool over the app's lifecycle."""
    global pool
    try:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable is not set.")
        pool = await asyncpg.create_pool(DATABASE_URL)
        logger.info("Database connection pool created.")
        yield
    finally:
        if pool:
            await pool.close()
            logger.info("Database connection pool closed.")


# --- API Instance ---
# The main application object
app = FastAPI(
    title="Crypto Intelligence API",
    description="A real-time API for crypto market data, news sentiment, and on-chain anomalies."
)


# --- API Endpoints ---
# Each endpoint serves a specific purpose, returning data from your pipelines.

@app.get("/api/v1/crypto/prices", response_model=List[CryptoPrice])
async def get_prices():
    """Retrieves the latest 10 crypto price records."""
    query = """
    SELECT
        coin_id,
        symbol,
        current_price,
        market_cap,
        market_cap_rank,
        timestamp
    FROM
        market_data
    ORDER BY
        timestamp DESC
    LIMIT 10;
    """
    async with pool.acquire() as conn:
        records = await conn.fetch(query)

    # convert the records to the Pydantic model
    return [CryptoPrice(**r) for r in records]


@app.get("/api/v1/news/sentiment", response_model=List[NewsSentiment])
async def get_sentiment():
    """Retrieves the latest 10 news sentiment records."""
    query = """
    SELECT
        title,
        sentiment_score,
        timestamp,
        link
    FROM
        news
    ORDER BY
        timestamp DESC
    LIMIT 10;
    """
    async with pool.acquire() as conn:
        records = await conn.fetch(query)

    return [NewsSentiment(**r) for r in records]


@app.get("/api/v1/onchain/anomalies", response_model=List[OnchainAnomaly])
async def get_anomalies():
    """Retrieves the latest 10 on-chain anomaly records."""
    # This assumes a table named 'onchain_anomalies' exists,
    # created by your anomaly_detector.py script.
    query = """
    SELECT
        tx_hash,
        from_address,
        to_address,
        value_eth,
        timestamp
    FROM
        onchain_anomalies
    ORDER BY
        timestamp DESC
    LIMIT 10;
    """
    async with pool.acquire() as conn:
        records = await conn.fetch(query)

    return [OnchainAnomaly(**r) for r in records]


# --- Main Entrypoint ---
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("fast_api:app", host="0.0.0.0", port=8000, reload=True)
