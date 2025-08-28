import logging
import asyncio
import os
import json
import asyncpg
from aiokafka import AIOKafkaConsumer
from datetime import datetime
from prometheus_client import start_http_server, Gauge, Counter
from dotenv import load_dotenv

# --- Setup and Configuration ---
load_dotenv()

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- Prometheus Metrics ---
ROWS_WRITTEN = Counter('rows_written_total', 'Total number of rows written to the database.')
BATCH_LATENCY_SECONDS = Gauge('batch_write_latency_seconds', 'Latency of writing a batch to the database.')
MESSAGE_PROCESSING_ERRORS = Counter('message_processing_errors_total', 'Total number of messages that failed to process.')
DB_CONNECTION_STATUS = Gauge('db_connection_status', 'Status of the database connection (1=up, 0=down).')

# --- Constants and Environment Variables ---
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 2000))
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto-market-data')
DATABASE_URL = os.getenv('DATABASE_URL')
KAFKA_SERVERS = os.getenv('KAFKA_SERVERS', 'localhost:9092')
PROMETHEUS_PORT = int(os.getenv('PROMETHEUS_PORT', 9102))

# --- Database Schema Management ---
async def create_prices_tables(conn):
    """
    Ensures the necessary database table exists.
    """
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                timestamp BIGINT PRIMARY KEY, 
                id TEXT NOT NULL, 
                price NUMERIC, 
                market_cap NUMERIC, 
                market_cap_rank NUMERIC
            )
        """)
        log.info('Prices table checked/created successfully.')
    except asyncpg.exceptions.PostgresError as e:
        log.critical(f'Failed to create prices table: {e}')
        raise

# --- Message Processing and Validation ---
def parse_and_validate_message(msg_value):
    """
    Parses and validates a JSON message from Kafka.
    Returns a tuple of parsed data or raises a ValueError on failure.
    """
    try:
        val = json.loads(msg_value)
        # Type and existence checks
        if not all(k in val for k in ('timestamp', 'id', 'price', 'market_cap', 'market_cap_rank')):
            raise ValueError("Missing required fields in message.")

        # Type coercion and validation
        if isinstance(val['timestamp'], str):
            timestamp_ms = int(datetime.fromisoformat(val['timestamp']).timestamp() * 1000)
        else:
            timestamp_ms = int(val['timestamp'])

        data_tuple = (
            timestamp_ms,
            str(val['id']),
            float(val['price']),
            float(val['market_cap']),
            float(val['market_cap_rank'])
        )
        return data_tuple

    except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
        log.error(f"Failed to process message due to format error: {e}. Message value: {msg_value}")
        MESSAGE_PROCESSING_ERRORS.inc()
        return None

# --- Main Application Logic ---
async def consume_and_process():
    """
    The main consumer loop for reading from Kafka and writing to Postgres.
    """
    pool = None
    consumer = None

    try:
        # Database Connection Pool
        log.info('Attempting to connect to postgres...')
        pool = await asyncpg.create_pool(DATABASE_URL, command_timeout=60)
        DB_CONNECTION_STATUS.set(1)
        log.info('Successfully connected to database.')

        # Ensure table exists on startup
        async with pool.acquire() as conn:
            await create_prices_tables(conn)

        # Kafka Consumer
        log.info(f"Attempting to connect to Kafka topic: {KAFKA_TOPIC}...")
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            group_id="crypto-data-group",
            auto_offset_reset='earliest',
            enable_auto_commit=True,  # Let Kafka manage offsets for simplicity
            auto_commit_interval_ms=1000
        )
        await consumer.start()
        log.info('Kafka consumer started.')

        batch = []
        log.info('Starting consumer loop.')

        # Main Consumption Loop
        while True:
            messages = await consumer.getmany(timeout_ms=5000, max_records=BATCH_SIZE)
            for tp, messages_for_tp in messages.items():
                for msg in messages_for_tp:
                    parsed_data = parse_and_validate_message(msg.value)
                    if parsed_data:
                        batch.append(parsed_data)

            if batch:
                log.info(f"Batch full ({len(batch)} items), committing to DB...")
                try:
                    async with pool.acquire() as conn:
                        start_time = asyncio.get_event_loop().time()
                        await conn.executemany("""
                            INSERT INTO prices (timestamp, id, price, market_cap, market_cap_rank)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (timestamp) DO UPDATE SET
                                id = EXCLUDED.id,
                                price = EXCLUDED.price,
                                market_cap = EXCLUDED.market_cap,
                                market_cap_rank = EXCLUDED.market_cap_rank
                        """, batch)
                        end_time = asyncio.get_event_loop().time()
                        BATCH_LATENCY_SECONDS.set(end_time - start_time)
                        ROWS_WRITTEN.inc(len(batch))
                        log.info('Batch written successfully.')

                except asyncpg.exceptions.PostgresError as e:
                    log.error(f"Failed to write batch to DB: {e}. Skipping batch.")
                    DB_CONNECTION_STATUS.set(0) # Indicate DB is down
                    # The loop continues, and we'll re-attempt the DB connection later
                finally:
                    batch.clear()

            await asyncio.sleep(1) # Prevents busy-waiting

    except Exception as e:
        log.critical(f"An unhandled critical error occurred: {e}", exc_info=True)
    finally:
        if consumer:
            await consumer.stop()
            log.info("Kafka consumer stopped.")
        if pool:
            await pool.close()
            log.info("Database pool closed.")
        DB_CONNECTION_STATUS.set(0)
        log.info("Application shutdown complete.")

if __name__ == '__main__':
    log.info(f'Starting Prometheus metrics server on port {PROMETHEUS_PORT}.')
    try:
        start_http_server(PROMETHEUS_PORT)
    except OSError as e:
        log.error(f"Failed to start Prometheus server on port {PROMETHEUS_PORT}: {e}")

    asyncio.run(consume_and_process())