import asyncio
import aiohttp
import json
import logging
import time
from aiokafka import AIOKafkaProducer
from datetime import datetime
from prometheus_client import start_http_server, Counter, Gauge

from config import KAFKA_SERVERS, KAFKA_TOPIC_CRYPTO_DATA, COINGECKO_API_URL, COINS, POLLING_INTERVAL_SECONDS, \
    METRICS_PORT_PRODUCER

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Prometheus Metrics Setup ---
PRICE_SENT = Counter("crypto_prices_sent_total", "Total number of crypto price messages produced")
LAST_PRICE = Gauge("last_crypto_price", "Last known price of a crypto coin", ["symbol"])
FAILED_API_CALLS = Counter("api_calls_failed_total", "Total number of failed API calls")
PRODUCER_UP = Gauge("producer_status", "Producer service status (1=up, 0=down)")


async def fetch_and_produce_data():
    producer = None
    try:
        PRODUCER_UP.set(1)
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await producer.start()
        logger.info(f'✅ Kafka Producer connected to {KAFKA_SERVERS}')

        async with aiohttp.ClientSession() as session:
            while True:
                params = {"vs_currency": "usd", "ids": COINS}
                try:
                    async with session.get(COINGECKO_API_URL, params=params, timeout=10) as response:
                        response.raise_for_status()
                        data = await response.json()

                        for coin_data in data:
                            # --- Data Validation and Transformation ---
                            required_fields = ["id", "current_price", "market_cap", "market_cap_rank", "last_updated"]
                            if not all(
                                    field in coin_data and coin_data[field] is not None for field in required_fields):
                                logger.warning(f"Skipping malformed data for coin ID: {coin_data.get('id', 'N/A')}")
                                continue

                            message = {
                                "id": coin_data["id"],
                                "price": float(coin_data["current_price"]),
                                "market_cap": float(coin_data["market_cap"]),
                                "market_cap_rank": int(coin_data["market_cap_rank"]),
                                "timestamp": int(datetime.fromisoformat(coin_data["last_updated"]).timestamp() * 1000),
                            }
                            await producer.send_and_wait(
                                KAFKA_TOPIC_CRYPTO_DATA,
                                key=coin_data["id"].encode("utf-8"),
                                value=message,
                            )
                            logger.info(f"Produced message for {coin_data['id']}")
                            PRICE_SENT.inc()
                            LAST_PRICE.labels(symbol=coin_data['id']).set(coin_data['current_price'])

                except aiohttp.ClientError as e:
                    logger.error(f"HTTP error during API call: {e}. Retrying...")
                    FAILED_API_CALLS.inc()
                except Exception as e:
                    logger.error(f"Unexpected error: {e}", exc_info=True)
                    # We continue the loop to attempt recovery.

                await asyncio.sleep(POLLING_INTERVAL_SECONDS)

    except asyncio.CancelledError:
        logger.info("Task was cancelled, shutting down.")
    finally:
        if producer:
            await producer.stop()
            logger.info("Kafka producer stopped.")
        PRODUCER_UP.set(0)


if __name__ == "__main__":
    start_http_server(METRICS_PORT_PRODUCER)
    logger.info(f'✅ Prometheus server started on port {METRICS_PORT_PRODUCER}')
    asyncio.run(fetch_and_produce_data())