# sentiment/sentiment_to_db.py
import os
import json
import asyncio
import logging
import signal
from typing import List, Tuple, Any

from aiokafka import AIOKafkaConsumer
import asyncpg
from prometheus_client import start_http_server, Counter, Histogram, Gauge
from dotenv import load_dotenv

load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("sentiment_to_db")

# Config
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 20))
BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL", 5))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news.scored")
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
GROUP_ID = os.getenv("GROUP_ID", "sentiment-to-db-group")
DATABASE_URL = os.getenv("DATABASE_URL")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 9104))
DLQ_TABLE = os.getenv("SENTIMENT_DLQ_TABLE", "sentiment_dlq")
TABLE_NAME = os.getenv("SENTIMENT_TABLE", "news")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
POOL_MIN = int(os.getenv("DB_POOL_MIN", 1))
POOL_MAX = int(os.getenv("DB_POOL_MAX", 5))

# Metrics
ROWS_WRITTEN = Counter("sentiment_rows_written_total", "rows written to postgres")
BATCH_LATENCY_SECONDS = Histogram("sentiment_batch_latency_seconds", "db write latency")
MESSAGE_PROCESSING_ERRORS = Counter("sentiment_message_processing_errors_total", "Total messages with processing errors")
DLQ_WRITES = Counter("sentiment_dlq_writes_total", "Messages written to DLQ")
CONSUMER_UP = Gauge("sentiment_to_db_status", "Sentiment consumer status (1=up,0=down)")
LAST_STORED_ID = Gauge("sentiment_last_stored_id", "Last stored DB id for sentiment")

_stop = asyncio.Event()
pool: asyncpg.pool.Pool | None = None

def _signal_handler():
    log.info("Shutdown signal received for sentiment_to_db.")
    _stop.set()

async def retry_with_backoff(coro_fn, retries: int = MAX_RETRIES, base_delay: float = 1.0):
    attempt = 0
    while True:
        try:
            return await coro_fn()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            if attempt > retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            log.warning(f"Transient error (attempt {attempt}/{retries}): {e}. Retrying in {delay}s")
            await asyncio.sleep(delay)

async def create_pool():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=POOL_MIN, max_size=POOL_MAX)
    log.info("Postgres pool created for sentiment_to_db")

async def ensure_tables():
    async with pool.acquire() as conn:
        # main table with unique constraint on link to avoid duplicates
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                timestamp BIGINT,
                title TEXT,
                link TEXT UNIQUE,
                sentiment_score REAL
            )
        """)
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {DLQ_TABLE} (
                id SERIAL PRIMARY KEY,
                payload JSONB,
                reason TEXT,
                failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    log.info("Ensured sentiment and DLQ tables exist")

async def write_dlq(payload: Any, reason: str):
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"INSERT INTO {DLQ_TABLE} (payload, reason) VALUES ($1, $2)", json.dumps(payload), reason)
            DLQ_WRITES.inc()
            log.error("Wrote message to DLQ")
    except Exception as e:
        log.critical(f"Failed to write to DLQ: {e}. Payload lost.")

async def store_batch(batch: List[Tuple]):
    async def _do_store():
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(
                    f"""
                    INSERT INTO {TABLE_NAME} (timestamp, title, link, sentiment_score)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (link) DO UPDATE SET
                        title = EXCLUDED.title,
                        sentiment_score = EXCLUDED.sentiment_score
                    """,
                    batch
                )
    try:
        start = asyncio.get_event_loop().time()
        await retry_with_backoff(_do_store)
        latency = asyncio.get_event_loop().time() - start
        BATCH_LATENCY_SECONDS.observe(latency)
        ROWS_WRITTEN.inc(len(batch))
        async with pool.acquire() as conn:
            last_id = await conn.fetchval(f"SELECT MAX(id) FROM {TABLE_NAME}")
            LAST_STORED_ID.set(last_id or 0)
        log.info(f"Stored batch of {len(batch)} sentiment records (latency={latency:.3f}s)")
        return True
    except Exception as e:
        log.error(f"Failed to store batch after retries: {e}")
        return False

async def consume_and_store():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    await consumer.start()
    CONSUMER_UP.set(1)
    try:
        await create_pool()
        await ensure_tables()
    except Exception as e:
        log.exception("Failed to initialize DB for sentiment_to_db. Exiting.")
        await consumer.stop()
        CONSUMER_UP.set(0)
        return

    batch = []
    offsets_to_commit = []  # store (topic_partition, offset) tuples for commit after DB write
    last_batch_time = asyncio.get_event_loop().time()

    try:
        async for msg in consumer:
            if _stop.is_set():
                break
            try:
                payload = msg.value
                # validate minimal fields
                if not payload or "ts" not in payload or "title" not in payload or "sentiment" not in payload:
                    MESSAGE_PROCESSING_ERRORS.inc()
                    log.warning(f"Skipping malformed payload: {payload}")
                    continue

                batch.append((payload["ts"], payload["title"], payload.get("link"), float(payload["sentiment"])))
                offsets_to_commit.append((msg.topic, msg.partition, msg.offset))

                now = asyncio.get_event_loop().time()
                if len(batch) >= BATCH_SIZE or (now - last_batch_time) >= BATCH_INTERVAL:
                    if batch:
                        success = await store_batch(batch)
                        if success:
                            # commit the latest offset for each partition
                            try:
                                # build offsets mapping {tp: offset+1}
                                offsets_map = {}
                                for t, p, o in offsets_to_commit:
                                    offsets_map.setdefault((t, p), o)
                                # For each partition, commit the maximum offset+1
                                to_commit = {}
                                for (t, p), o in offsets_map.items():
                                    # aiokafka expects {TopicPartition: offset}
                                    from aiokafka.structs import TopicPartition
                                    tp = TopicPartition(t, p)
                                    to_commit[tp] = o + 1
                                if to_commit:
                                    await consumer.commit(to_commit)
                            except Exception as e:
                                log.error(f"Failed to commit offsets to Kafka: {e}")
                        else:
                            # store to DLQ
                            await write_dlq(batch, "sentiment_store_failed_after_retries")
                        batch.clear()
                        offsets_to_commit.clear()
                        last_batch_time = now

            except asyncio.CancelledError:
                log.info("Sentiment consumer cancelled mid-loop.")
                raise
            except Exception as e:
                MESSAGE_PROCESSING_ERRORS.inc()
                log.exception(f"Error processing message: {e}")

        # flush remaining batch on shutdown
        if batch:
            success = await store_batch(batch)
            if success:
                try:
                    # commit final offsets
                    offsets_map = {}
                    for t, p, o in offsets_to_commit:
                        offsets_map.setdefault((t, p), o)
                    to_commit = {}
                    from aiokafka.structs import TopicPartition
                    for (t, p), o in offsets_map.items():
                        tp = TopicPartition(t, p)
                        to_commit[tp] = o + 1
                    if to_commit:
                        await consumer.commit(to_commit)
                except Exception as e:
                    log.error(f"Failed to commit final offsets: {e}")
            else:
                await write_dlq(batch, "sentiment_store_failed_on_shutdown")
            batch.clear()
            offsets_to_commit.clear()

    finally:
        try:
            await consumer.stop()
        except Exception as e:
            log.warning(f"Failed to stop Kafka consumer cleanly: {e}")
        if pool:
            await pool.close()
        CONSUMER_UP.set(0)
        log.info("Sentiment consumer shutdown complete.")

# Entrypoint and signal wiring for standalone runs
if __name__ == "__main__":
    import platform
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    if platform.system() != "Windows":
        try:
            loop.add_signal_handler(signal.SIGINT, _signal_handler)
            loop.add_signal_handler(signal.SIGTERM, _signal_handler)
        except NotImplementedError:
            pass

    start_http_server(PROMETHEUS_PORT)
    log.info(f"Prometheus metrics server started on port {PROMETHEUS_PORT}")

    try:
        loop.run_until_complete(consume_and_store())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        log.info("Event loop closed")
