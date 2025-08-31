
"""
anomaly_detector.py
Consumes enriched transactions (or reads from DB) and detects anomalies:
- uses DB-driven rolling Z-score per from_address
- stores anomalies to DB (anomalies table)
- sends Slack alerts (with retries)
- writes persistent DLQ if storing fails
- Prometheus metrics + graceful shutdown
"""

import os
import json
import math
import asyncio
import logging
import signal
from typing import Optional, Dict, Any

import asyncpg
import aiohttp
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge, start_http_server

load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("anomaly_detector")

# Config
DATABASE_URL = os.getenv("DATABASE_URL")
TABLE_NAME = os.getenv("TABLE_NAME", "onchain_data")
ANOMALY_TABLE = os.getenv("ANOMALY_TABLE", "onchain_anomalies")
DLQ_TABLE = os.getenv("ANOMALY_DLQ_TABLE", "anomaly_dlq")
PROMETHEUS_PORT = int(os.getenv("ANOMALY_PROMETHEUS_PORT", 9111))
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Z-score config
Z_WINDOW = int(os.getenv("Z_WINDOW", 100))     # number of recent tx to use as baseline
Z_THRESH = float(os.getenv("Z_THRESHOLD", 4.0))  # flag if |z| >= threshold

# Operational config
LOOP_INTERVAL = int(os.getenv("ANOMALY_LOOP_INTERVAL", 10))  # how often to run detection loop
POOL_MIN = int(os.getenv("DB_POOL_MIN", 1))
POOL_MAX = int(os.getenv("DB_POOL_MAX", 5))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))

# Prometheus metrics
ANOMALIES_FOUND = Counter("anomalies_found_total", "Total anomalies detected")
SLACK_ALERTS_SENT = Counter("anomaly_slack_alerts_sent_total", "Slack alerts sent")
SLACK_ALERTS_FAILED = Counter("anomaly_slack_alerts_failed_total", "Slack alerts failed")
ANOMALIES_STORED = Counter("anomalies_stored_total", "Anomalies persisted to DB")
ANOMALY_DLQ_WRITES = Counter("anomaly_dlq_writes_total", "Anomalies written to DLQ")
LAST_ANOMALY_ID = Gauge("last_anomaly_id", "Last saved anomaly DB id")

# Globals
_stop = asyncio.Event()
pool: asyncpg.pool.Pool | None = None


# -----------------------
# Retry helper
# -----------------------
async def retry_with_backoff(coro_fn, retries: int = MAX_RETRIES, base_delay: float = 1.0):
    attempt = 0
    while True:
        try:
            return await coro_fn()
        except Exception as e:
            attempt += 1
            if attempt > retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(f"Transient error (attempt {attempt}/{retries}). Retrying in {delay}s: {e}")
            await asyncio.sleep(delay)


# -----------------------
# Slack notifier
# -----------------------
async def send_slack_alert(payload: Dict[str, Any]):
    if not SLACK_WEBHOOK_URL:
        logger.info("No SLACK_WEBHOOK_URL configured; skipping Slack alert.")
        return

    async def _post():
        async with aiohttp.ClientSession() as session:
            text = f"🚨 Anomaly detected:\n```{json.dumps(payload, indent=2)}```"
            async with session.post(SLACK_WEBHOOK_URL, json={"text": text}) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"Slack returned {resp.status}: {text}")
                return True

    try:
        await retry_with_backoff(_post)
        SLACK_ALERTS_SENT.inc()
        logger.info("✅ Slack alert sent")
    except Exception as e:
        SLACK_ALERTS_FAILED.inc()
        logger.error(f"Failed to send Slack alert after retries: {e}")


# -----------------------
# DB helpers: ensure tables + dlq
# -----------------------
async def ensure_tables():
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
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {DLQ_TABLE} (
                id SERIAL PRIMARY KEY,
                payload JSONB,
                reason TEXT,
                failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # indexes for fast lookups
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{ANOMALY_TABLE}_hash ON {ANOMALY_TABLE} (hash);")
    logger.info("✅ Ensured anomaly tables exist")


async def store_anomaly(anom: Dict[str, Any]) -> Optional[int]:
    """Store anomaly with retries; on permanent failure return None."""
    async def _do_store():
        async with pool.acquire() as conn:
            anomaly_id = await conn.fetchval(
                f"""
                INSERT INTO {ANOMALY_TABLE} (type, from_address, to_address, value, hash, block_number, reason)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                RETURNING id
                """,
                anom.get("type"),
                anom.get("from_address"),
                anom.get("to_address"),
                anom.get("value"),
                anom.get("hash"),
                anom.get("block_number"),
                anom.get("reason")
            )
            return anomaly_id

    try:
        anomaly_id = await retry_with_backoff(_do_store)
        ANOMALIES_STORED.inc()
        LAST_ANOMALY_ID.set(anomaly_id or 0)
        logger.info(f"Stored anomaly id={anomaly_id}")
        return anomaly_id
    except Exception as e:
        logger.error(f"Failed to store anomaly after retries: {e}")
        return None


async def write_anomaly_dlq(payload: Dict[str, Any], reason: str):
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"INSERT INTO {DLQ_TABLE} (payload, reason) VALUES ($1, $2)", json.dumps(payload), reason)
            ANOMALY_DLQ_WRITES.inc()
            logger.error("Wrote anomaly to DLQ table")
    except Exception as e:
        logger.critical(f"Failed to write anomaly to DLQ: {e}")


# -----------------------
# Z-score logic (DB-driven baseline)
# -----------------------
async def compute_zscore_for_address(from_address: str, value: float, window: int = Z_WINDOW) -> Optional[float]:
    """
    Query last `window` values for from_address and compute z-score for `value`.
    Returns None if not enough baseline or std == 0.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT value FROM {TABLE_NAME} WHERE from_address=$1 AND value IS NOT NULL ORDER BY id DESC LIMIT $2",
            from_address, window
        )
    if not rows:
        return None
    values = [float(r["value"]) for r in rows if r["value"] is not None]
    if not values:
        return None
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(var) if var > 0 else 0.0
    if std == 0.0:
        return None
    z = (float(value) - mean) / std
    return z


# -----------------------
# Detection rules & loop
# -----------------------
async def detect_and_alert():
    """
    Single pass: look at newest transactions and evaluate anomalies.
    Strategy:
     - Query recent transactions that are not yet evaluated (here we use latest N rows)
     - For each tx compute zscore and static checks
     - Persist anomaly + alert via Slack
    """
    # We'll take last N rows as candidate set to evaluate on each loop.
    CANDIDATE_LIMIT = int(os.getenv("ANOMALY_CANDIDATE_LIMIT", 500))

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT id, type, from_address, to_address, value, hash, block_number, timestamp "
            f"FROM {TABLE_NAME} ORDER BY id DESC LIMIT $1", CANDIDATE_LIMIT
        )

    for r in rows:
        tx = dict(r)
        # normalization & guards
        if tx.get("value") is None:
            continue

        from_addr = tx.get("from_address")
        value = float(tx.get("value"))

        # Static rules
        reasons = []
        if from_addr == tx.get("to_address"):
            reasons.append("self_transfer")
        if (not tx.get("hash")) or (not tx.get("block_number")):
            reasons.append("incomplete_data")

        # Z-score rule
        z = await compute_zscore_for_address(from_addr, value)
        if z is not None and abs(z) >= Z_THRESH:
            reasons.append(f"z_score:{z:.2f}")

        # If any reason flagged -> persist + alert
        if reasons:
            ANOMALIES_FOUND.inc()
            reason_text = ";".join(reasons)
            anomaly_record = {
                "type": tx.get("type"),
                "from_address": from_addr,
                "to_address": tx.get("to_address"),
                "value": value,
                "hash": tx.get("hash"),
                "block_number": tx.get("block_number"),
                "reason": reason_text
            }

            # Persist anomaly
            anomaly_id = await store_anomaly(anomaly_record)
            if anomaly_id is None:
                # store failed after retries -> write DLQ
                await write_anomaly_dlq(anomaly_record, "store_failed_after_retries")
            # Fire Slack alert (best-effort with retry)
            await send_slack_alert(anomaly_record)


# -----------------------
# Loop + graceful shutdown
# -----------------------
def _signal_handler():
    logger.info("Shutdown signal received.")
    _stop.set()


async def main_loop():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=POOL_MIN, max_size=POOL_MAX)
    logger.info("Connected to Postgres pool")
    await ensure_tables()

    while not _stop.is_set():
        try:
            await detect_and_alert()
        except Exception as e:
            logger.exception(f"Error in detection loop: {e}")
        await asyncio.wait([_stop.wait()], timeout=LOOP_INTERVAL)


def main():
    start_http_server(PROMETHEUS_PORT)
    logger.info(f"Prometheus server started on port {PROMETHEUS_PORT}")

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, _signal_handler)
    loop.add_signal_handler(signal.SIGTERM, _signal_handler)

    try:
        loop.run_until_complete(main_loop())
    finally:
        if pool:
            loop.run_until_complete(pool.close())
        loop.run_until_complete(asyncio.sleep(0.1))
        loop.close()
        logger.info("Anomaly detector shutdown complete.")


if __name__ == "__main__":
    main()
