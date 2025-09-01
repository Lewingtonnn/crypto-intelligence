from aiokafka import AIOKafkaConsumer
import logging
import asyncio
import os
import json
import asyncpg
from prometheus_client import start_http_server
from prometheus_client import Counter, Histogram, Gauge
ROWS_WRITTEN = Counter('sentiment_rows_written_total', 'rows to postgres')
BATCH_LATENCY_SECONDS = Histogram('sentiment_batch_latency_seconds', 'db write latency')
MESSAGE_PROCESSING_ERRORS = Counter("sentiment_message_processing_errors_total", "Total messages with processing errors")

from dotenv import load_dotenv
load_dotenv()

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)
# --- Constants and Environment Variables ---
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 20))
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'news.scored')
DATABASE_URL = os.getenv('DATABASE_URL')
KAFKA_SERVERS = os.getenv('KAFKA_SERVERS', 'localhost:9092')
PROMETHEUS_PORT = int(os.getenv('PROMETHEUS_PORT', 9104))

# --- Database Schema Management ---
async def create_sentiment_tables(conn):
    """
    Ensures the necessary database table exists.
    """
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                timestamp BIGINT, 
                title TEXT NOT NULL,
                link TEXT,
                sentiment_score REAL
            )
        """)
        log.info('Sentiment table checked/created successfully.')
    except asyncpg.exceptions.PostgresError as e:
        log.critical(f'Failed to create sentiment table: {e}')
        raise

# --- Message Processing and Validation ---
def parse_and_validate_message(msg_value):
    """
    Parses and validates a JSON message from Kafka.
    Returns a tuple of parsed data or raises a ValueError on failure.
    """
    try:
        val = json.loads(msg_value)
        if 'ts' not in val or 'title' not in val or 'sentiment' not in val:
            raise ValueError("Missing required fields in message")
        return (val['ts'], val['title'], float(val['sentiment']), val['link'])
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        raise ValueError(f"Invalid message format: {e}")

# --- Main Consumer and Database Ingestion Logic ---
async def consume_and_store():
    consumer = None
    conn = None
    try:
        # Start Prometheus metrics server
        start_http_server(PROMETHEUS_PORT)
        log.info(f"✅ Prometheus metrics server started on port {PROMETHEUS_PORT}")

        # Connect to PostgreSQL
        log.info("Connecting to PostgreSQL database....")
        conn = await asyncpg.connect(DATABASE_URL)
        log.info("✅ Connected to PostgreSQL database, checking/creating tables...")
        await create_sentiment_tables(conn)
        log.info("✅ Connected PostgreSQL database and ensured tables exist")

        # Setup Kafka consumer
        log.info("Starting Kafka Consumer for Sentiment to DB...")
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            group_id="sentiment-to-db-group",
            auto_offset_reset="earliest"
        )
        await consumer.start()
        log.info("✅ Kafka Consumer for Sentiment to DB started")

        batch = []
        async for msg in consumer:
            try:
                data_tuple = parse_and_validate_message(msg.value)
                log.info(f"Received message for timestamp {data_tuple[0]} with sentiment {data_tuple[2]:.3f}")
                batch.append(data_tuple)

                if len(batch) >= BATCH_SIZE:
                    start_time = asyncio.get_event_loop().time()
                    async with conn.transaction():
                        await conn.executemany("""
                            INSERT INTO news (timestamp, title, sentiment_score, link) 
                            VALUES ($1, $2, $3, $4)
                            
                        """, batch)
                    latency = asyncio.get_event_loop().time() - start_time
                    BATCH_LATENCY_SECONDS.observe(latency)
                    ROWS_WRITTEN.inc(len(batch))
                    log.info(f"🗄️  Inserted batch of {len(batch)} records into the database in {latency:.3f} seconds")
                    batch.clear()
            except ValueError as e:
                MESSAGE_PROCESSING_ERRORS.inc()
                log.error(f"Failed to process message: {e}. Skipping message.")
            except asyncpg.exceptions.PostgresError as e:
                MESSAGE_PROCESSING_ERRORS.inc()
                log.error(f"Database error: {e}. Retrying batch insertion.")
                # On DB error, we do not clear the batch to retry insertion

    except Exception as e:
        log.critical(f"A critical error occurred: {e}", exc_info=True)
    finally:
        if consumer:
            await consumer.stop()
            log.info("Kafka Consumer stopped")
        if conn:
            await conn.close()
            log.info("PostgreSQL connection closed")

if __name__ == "__main__":
    asyncio.run(consume_and_store())

