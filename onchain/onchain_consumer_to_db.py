import os
import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
import asyncpg
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge, start_http_server

# --- Load env ---
load_dotenv()

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Config ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_ENRICHED_TOPIC = os.getenv("KAFKA_ENRICHED_TOPIC", "onchain.enriched")
GROUP_ID = os.getenv("GROUP_ID", "onchain-to-db-group")
DATABASE_URL = os.getenv("DATABASE_URL")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 9110))
TABLE_NAME = os.getenv("TABLE_NAME", "onchain_data")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL", 5))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))

# --- Prometheus ---
MSG_RECEIVED = Counter("onchain_msgs_received_total", "On-chain messages received from Kafka topic")
MSG_STORED = Counter("onchain_msgs_stored_total", "On-chain messages stored in the database")
FAILED_STORES = Counter("onchain_failed_stores_total", "Failed attempts to store messages in the database")
LAST_STORED_ID = Gauge("onchain_last_stored_id", "ID of the last successfully stored message")


async def create_pool():
    """
    Create a PostgreSQL connection pool.
    """
    return await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)


async def ensure_table(pool):
    """
    Ensure the target table exists.
    """
    async with pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                type TEXT,
                from_address TEXT,
                to_address TEXT,
                value NUMERIC,
                hash TEXT,
                block_number BIGINT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    logger.info(f"✅ Ensured table {TABLE_NAME} exists")


async def store_batch(pool, batch):
    """
    Store a batch of messages into PostgreSQL with retry logic.
    """
    retries = 0
    while retries < MAX_RETRIES:
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(f"""
                        INSERT INTO {TABLE_NAME} (type, from_address, to_address, value, hash, block_number)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """, batch)
            MSG_STORED.inc(len(batch))
            async with pool.acquire() as conn:
                last_id = await conn.fetchval(f"SELECT MAX(id) FROM {TABLE_NAME}")
                LAST_STORED_ID.set(last_id or 0)
            logger.info(f"💾 Stored {len(batch)} records in the database")
            return
        except Exception as e:
            retries += 1
            FAILED_STORES.inc(len(batch))
            wait_time = 2 ** retries
            logger.error(f"❌ Failed to store batch (attempt {retries}/{MAX_RETRIES}): {e}")
            logger.info(f"⏳ Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)

    logger.critical(f"🚨 Dropping batch after {MAX_RETRIES} retries")


async def consume_and_store():
    """
    Consume enriched on-chain data from Kafka and store it in PostgreSQL.
    """
    consumer = AIOKafkaConsumer(
        KAFKA_ENRICHED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    logger.info("Starting on-chain to DB consumer...")
    await consumer.start()

    pool = await create_pool()
    await ensure_table(pool)

    batch = []
    last_batch_time = asyncio.get_event_loop().time()

    try:
        async for msg in consumer:
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

            current_time = asyncio.get_event_loop().time()
            if len(batch) >= BATCH_SIZE or (current_time - last_batch_time) >= BATCH_INTERVAL:
                if batch:
                    await store_batch(pool, batch)
                    batch.clear()
                    last_batch_time = current_time

    finally:
        await consumer.stop()
        await pool.close()
        logger.info("Consumer and database connection closed.")
        LAST_STORED_ID.set(0)


if __name__ == "__main__":
    start_http_server(PROMETHEUS_PORT)
    logger.info(f"✅ Prometheus server started on port {PROMETHEUS_PORT}")
    asyncio.run(consume_and_store())
