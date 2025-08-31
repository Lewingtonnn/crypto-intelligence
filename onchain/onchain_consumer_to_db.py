#!/usr/bin/env python3
"""
consumer_to_db.py
Consumes enriched on-chain transactions from Kafka and writes to Postgres reliably:
- connection pool
- batching
- retries with exponential backoff
- DLQ on permanent failure
- commits Kafka offsets only after successful DB write
- graceful shutdown
- Prometheus metrics
"""

import os
import json
import asyncio
import logging
import signal
from typing import List, Tuple, Any

from aiokafka import AIOKafkaConsumer
import asyncpg
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge, start_http_server

load_dotenv()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("consumer_to_db")

# Config (env override)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_ENRICHED_TOPIC = os.getenv("KAFKA_ENRICHED_TOPIC", "onchain.enriched")
GROUP_ID = os.getenv("GROUP_ID", "onchain-to-db-group")
DATABASE_URL = os.getenv("DATABASE_URL")
TABLE_NAME = os.getenv("TABLE_NAME", "onchain_data")
DLQ_TABLE = os.getenv("DLQ_TABLE", "onchain_dlq")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 9110))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL", 5))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
POOL_MIN = int(os.getenv("DB_POOL_MIN", 1))
POOL_MAX = int(os.getenv("DB_POOL_MAX", 10))

# Prometheus metrics
MSG_RECEIVED = Counter("onchain_msgs_received_total", "On-chain messages received from Kafka topic")
MSG_STORED = Counter("onchain_msgs_stored_total", "On-chain messages stored in the database")
FAILED_STORES = Counter("onchain_failed_stores_total", "Failed attempts to store messages in the database")
DLQ_WRITES = Counter("onchain_dlq_writes_total", "Messages written to DLQ after retries")
LAST_STORED_ID = Gauge("onchain_last_stored_id", "ID of the last successfully stored message")

# Globals
_stop = asyncio.Event()
pool: asyncpg.pool.Pool | None = None


# -----------------------
# Helpers
# -----------------------
async def retry_with_backoff(coro_fn, retries: int = MAX_RETRIES, base_delay: float = 1.0):
    """Run coroutine factory (callable) with exponential backoff. coro_fn returns an awaitable."""
    attempt = 0
    while True:
        try:
            return await coro_fn()
        except Exception as e:
            attempt += 1
            if attempt > retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(f"Transient error (attempt {attempt}/{retries}), retrying in {delay}s: {e}")
            await asyncio.sleep(delay)


# -----------------------
# DB helpers
# -----------------------
async def create_pool():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=POOL_MIN, max_size=POOL_MAX)
    logger.info("✅ Postgres connection pool created")


async def ensure_tables():
    """Ensure main table and DLQ exist and recommended indexes."""
    async with pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                type TEXT,
                from_address TEXT,
                to_address TEXT,
                value NUMERIC,
                hash TEXT UNIQUE,
                block_number BIGINT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {DLQ_TABLE} (
                id SERIAL PRIMARY KEY,
                payload JSONB,
                reason TEXT,
                failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # indexes: important for z-score queries / lookups later
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_from ON {TABLE_NAME} (from_address, id DESC);")
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_block ON {TABLE_NAME} (block_number);")
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_hash ON {TABLE_NAME} (hash);")
    logger.info("✅ Ensured DB tables and indexes exist")


async def write_dlq(payload: Any, reason: str):
    """Write failed payload to DLQ table (non-blocking best-effort)."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO {DLQ_TABLE} (payload, reason) VALUES ($1, $2)",
                json.dumps(payload), reason
            )
            DLQ_WRITES.inc()
            logger.error("Wrote message to DLQ")
    except Exception as e:
        logger.critical(f"Failed to write to DLQ: {e}. Payload lost.")


async def store_batch(batch: List[Tuple]):
    """
    Insert batch into DB with retries and idempotency protection.
    Batch items must match columns (type, from_address, to_address, value, hash, block_number).
    """
    async def _do_store():
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(
                    f"""
                    INSERT INTO {TABLE_NAME} (type, from_address, to_address, value, hash, block_number)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (hash) DO NOTHING
                    """,
                    batch
                )

    try:
        await retry_with_backoff(_do_store)
        MSG_STORED.inc(len(batch))
        # update last stored id
        async with pool.acquire() as conn:
            last_id = await conn.fetchval(f"SELECT MAX(id) FROM {TABLE_NAME}")
            LAST_STORED_ID.set(last_id or 0)
        logger.info(f"💾 Stored batch of {len(batch)} records")
        return True
    except Exception as e:
        FAILED_STORES.inc(len(batch))
        logger.error(f"Failed to store batch after retries: {e}")
        return False


# -----------------------
# Kafka consumer + main loop
# -----------------------
async def consume_and_store():
    consumer = AIOKafkaConsumer(
        KAFKA_ENRICHED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # critical: commit only after DB success
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    await consumer.start()
    try:
        await create_pool()
        await ensure_tables()
    except Exception:
        logger.exception("Failed to initialize database. Exiting.")
        await consumer.stop()
        return

    batch = []
    last_batch_time = asyncio.get_event_loop().time()

    try:
        async for msg in consumer:
            if _stop.is_set():
                break
            MSG_RECEIVED.inc()
            payload = msg.value

            batch.append((
                payload.get("type"),
                payload.get("from_address"),
                payload.get("to_address"),
                payload.get("value"),
                payload.get("hash"),
                payload.get("block_number")
            ))

            now = asyncio.get_event_loop().time()
            if len(batch) >= BATCH_SIZE or (now - last_batch_time) >= BATCH_INTERVAL:
                if not batch:
                    last_batch_time = now
                    continue

                # store with retry; on permanent failure write DLQ
                success = await store_batch(batch)
                if success:
                    # commit offsets only after successful DB write
                    try:
                        await consumer.commit()
                    except Exception as e:
                        logger.error(f"Failed to commit Kafka offset: {e}")
                else:
                    # write entire batch to DLQ for manual or automated recovery
                    await write_dlq(batch, "store_batch_failed_after_retries")
                batch.clear()
                last_batch_time = now

        # if loop exits, flush remaining batch before shutdown
        if batch:
            success = await store_batch(batch)
            if success:
                try:
                    await consumer.commit()
                except Exception as e:
                    logger.error(f"Failed to commit Kafka offset on final flush: {e}")
            else:
                await write_dlq(batch, "store_batch_failed_on_shutdown")
            batch.clear()

    finally:
        await consumer.stop()
        if pool:
            await pool.close()
        logger.info("Consumer and DB pool closed.")


# -----------------------
# Graceful shutdown
# -----------------------
def _signal_handler():
    logger.info("Shutdown signal received.")
    _stop.set()


def main():
    start_http_server(PROMETHEUS_PORT)
    logger.info(f"Prometheus server started on port {PROMETHEUS_PORT}")

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, _signal_handler)
    loop.add_signal_handler(signal.SIGTERM, _signal_handler)

    try:
        loop.run_until_complete(consume_and_store())
    finally:
        loop.run_until_complete(asyncio.sleep(0.1))
        loop.close()
        logger.info("Exited.")


if __name__ == "__main__":
    main()
