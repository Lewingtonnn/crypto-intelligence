import os
import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
import asyncpg
import aiohttp
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge, start_http_server

# --- Load environment variables ---
load_dotenv()

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("anomaly-detector")

# --- Config ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_ENRICHED_TOPIC", "onchain.enriched")
GROUP_ID = os.getenv("ANOMALY_GROUP_ID", "anomaly-detector-group")
DATABASE_URL = os.getenv("DATABASE_URL")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 9111))
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ANOMALY_TABLE = os.getenv("ANOMALY_TABLE", "anomalies")

# Detection thresholds
VALUE_THRESHOLD = float(os.getenv("ANOMALY_VALUE_THRESHOLD", 100000))  # 100k default

# --- Prometheus metrics ---
ANOMALIES_FOUND = Counter("anomalies_found_total", "Total anomalies detected")
LAST_ANOMALY_ID = Gauge("last_anomaly_id", "ID of last stored anomaly in DB")
SLACK_ALERTS_SENT = Counter("slack_alerts_sent_total", "Slack alerts sent for anomalies")
FAILED_SLACK_ALERTS = Counter("failed_slack_alerts_total", "Failed Slack alerts")

# --- Global ---
pool = None  # Postgres connection pool


# --- Utility: Slack Alert ---
async def send_slack_alert(anomaly: dict):
    if not SLACK_WEBHOOK_URL:
        logger.warning("⚠️ Slack webhook URL not configured. Skipping alert.")
        return
    async with aiohttp.ClientSession() as session:
        try:
            payload = {"text": f"🚨 Anomaly detected: {json.dumps(anomaly, indent=2)}"}
            async with session.post(SLACK_WEBHOOK_URL, json=payload) as resp:
                if resp.status == 200:
                    SLACK_ALERTS_SENT.inc()
                    logger.info("✅ Slack alert sent.")
                else:
                    FAILED_SLACK_ALERTS.inc()
                    logger.error(f"Slack alert failed with status {resp.status}")
        except Exception as e:
            FAILED_SLACK_ALERTS.inc()
            logger.error(f"Slack alert error: {e}")


# --- Utility: Store anomaly in DB ---
async def store_anomaly(anomaly: dict):
    global pool
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {ANOMALY_TABLE} (
                    id SERIAL PRIMARY KEY,
                    type TEXT,
                    from_address TEXT,
                    to_address TEXT,
                    value NUMERIC,
                    hash TEXT,
                    block_number BIGINT,
                    reason TEXT,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            anomaly_id = await conn.fetchval(f"""
                INSERT INTO {ANOMALY_TABLE} (type, from_address, to_address, value, hash, block_number, reason)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
            """, anomaly.get("type"),
                 anomaly.get("from_address"),
                 anomaly.get("to_address"),
                 anomaly.get("value"),
                 anomaly.get("hash"),
                 anomaly.get("block_number"),
                 anomaly.get("reason"))
            LAST_ANOMALY_ID.set(anomaly_id or 0)
            logger.info(f"💾 Stored anomaly in DB (id={anomaly_id})")
    except Exception as e:
        logger.error(f"Failed to store anomaly: {e}")


# --- Detection Logic ---
def detect_anomaly(tx: dict):
    """
    Detect anomalies based on simple business rules.
    """
    anomalies = []

    # Rule 1: Very large transaction
    if tx.get("value") and float(tx["value"]) > VALUE_THRESHOLD:
        anomalies.append("High-value transfer")

    # Rule 2: Self transfer (same from/to)
    if tx.get("from_address") == tx.get("to_address"):
        anomalies.append("Self-transfer detected")

    # Rule 3: Missing critical fields
    if not tx.get("hash") or not tx.get("block_number"):
        anomalies.append("Incomplete transaction data")

    return anomalies


# --- Consumer Loop ---
async def consume_and_detect():
    global pool
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    await consumer.start()
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    logger.info("✅ Connected to Kafka & PostgreSQL")

    try:
        async for msg in consumer:
            tx = msg.value
            anomalies = detect_anomaly(tx)

            if anomalies:
                for reason in anomalies:
                    anomaly_record = {**tx, "reason": reason}
                    ANOMALIES_FOUND.inc()
                    logger.warning(f"🚨 Anomaly detected: {anomaly_record}")

                    # Fire off async tasks
                    await asyncio.gather(
                        store_anomaly(anomaly_record),
                        send_slack_alert(anomaly_record)
                    )
    finally:
        await consumer.stop()
        await pool.close()
        logger.info("Consumer stopped & DB pool closed.")


# --- Entry point ---
if __name__ == "__main__":
    start_http_server(PROMETHEUS_PORT)
    logger.info(f"📊 Prometheus server running on port {PROMETHEUS_PORT}")
    asyncio.run(consume_and_detect())
