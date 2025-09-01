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

class CryptoPrice(BaseModel):
    """Data model for a single crypto price record."""
    id: str = Field(..., description="Unique ID of the cryptocurrency (e.g., 'bitcoin').")
    price: float = Field(..., description="Current price in USD.")
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
    id : int = Field(..., description="Unique ID of the anomaly record.")
    type : str = Field()
    from_address: str = Field(..., description="Address of the sender.")
    to_address: str = Field(..., description="Address of the receiver.")
    value: float = Field()
    hash: str = Field(..., description="Transaction hash.")
    block_number: str = Field()
    reason : str = Field(..., description=" Reason for anomaly")
    detected_at: datetime = Field(..., description="Time when anomaly was detected.")

class OnchainData(BaseModel):
    """Onchain data model."""
    id : int = Field(..., description="Unique ID of the onchain record.")
    type : str = Field(..., description="Type of the onchain data.")
    from_address: str = Field(..., description="Address of the sender.")
    to_address: str = Field(..., description="Address of the receiver.")
    value: float = Field(..., description="Value transferred.")
    hash: str = Field(..., description="Transaction hash.")
    block_number: str = Field(..., description="Block number of the transaction.")
    timestamp: datetime = Field(..., description="Timestamp of the transaction.")


# --- FastAPI Lifespan Events ---

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
app = FastAPI(
    title="Crypto Intelligence API",
    description="A real-time API for crypto market data, news sentiment, and on-chain anomalies.",
    lifespan=lifespan
)


# --- API Endpoints ---
@app.get("/api/v1/crypto/prices", response_model=List[CryptoPrice], tags=["Market Data"])
async def get_prices():
    """Retrieves the latest 10 crypto price records."""
    query = """
    SELECT
        id,
        price,
        market_cap,
        market_cap_rank,
        timestamp
    FROM
        prices
    ORDER BY
        timestamp DESC
    LIMIT 10;
    """
    async with pool.acquire() as conn:
        records = await conn.fetch(query)

    return [CryptoPrice(**r) for r in records]


@app.get("/api/v1/news/sentiment", response_model=List[NewsSentiment], tags=["News Sentiment"])
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


@app.get("/api/v1/onchain/anomalies", response_model=List[OnchainAnomaly], tags=["On-chain Analytics"])
async def get_anomalies():
    """Retrieves the latest 10 on-chain anomaly records."""
    query = """
    SELECT
        id,
        type,
        from_address,
        to_address,
        value,
        hash,
        block_number,
        reason,
        detected_at
    FROM
        onchain_anomalies
    ORDER BY
        timestamp DESC
    LIMIT 10;
    """
    async with pool.acquire() as conn:
        records = await conn.fetch(query)

    return [OnchainAnomaly(**r) for r in records]

@app.get("/api/v1/onchain/data", response_model=List[OnchainData], tags=["On-chain Analytics"])
async def get_onchain_data():
    """Retrieves the latest 10 on-chain data records."""
    query = """
    SELECT
        id,
        type,
        from_address,
        to_address,
        value,
        hash,
        block_number,
        timestamp
    FROM
        onchain_data
    ORDER BY
        timestamp DESC
    LIMIT 10;
    """
    async with pool.acquire() as conn:
        records = await conn.fetch(query)

    return [OnchainData(**r) for r in records]


# --- Main Entrypoint ---
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("fast_api:app", host="0.0.0.0", port=8000, reload=True)
