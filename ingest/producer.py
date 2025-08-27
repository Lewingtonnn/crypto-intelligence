import asyncio
import aiohttp
import json
import logging
import os
from aiokafka import AIOKafkaProducer
from datetime import datetime
from dotenv import load_dotenv

from metrics import PRICE_SENT, LAST_PRICE, FAILED_MESSAGES
from prometheus_client import start_http_server
METRICS_PORT = int(os.getenv("METRICS_PORT", "9102"))


# Load env config
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config
coins = os.getenv("COINS", "bitcoin,ethereum,dogecoin")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-market-data")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"




async def fetch_and_produce_data():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    logger.info(f'Kafka Producer connected to {KAFKA_BOOTSTRAP_SERVERS}')

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                params = {"vs_currency": "usd", "ids": coins}
                try:
                    async with session.get(COINGECKO_API_URL, params=params) as response:
                        response.raise_for_status()
                        data = await response.json()

                        for coin_data in data:
                            message = {
                                "id": coin_data["id"],
                                "price": coin_data.get("current_price"),
                                "market_cap": coin_data.get("market_cap"),
                                "market_cap_rank": coin_data.get("market_cap_rank"),
                                "price_change_percentage_24h": coin_data.get("price_change_percentage_24h"),
                                "total_volume": coin_data.get("total_volume"),
                                "timestamp": coin_data.get("last_updated"),
                            }

                            await producer.send_and_wait(
                                KAFKA_TOPIC,
                                key=coin_data["id"].encode("utf-8"),
                                value=message,
                            )
                            logger.info(f"Produced message for {coin_data['id']}")
                            PRICE_SENT.inc()
                            price= coin_data['price']
                            LAST_PRICE.labels(symbol=coin_data['id']).set(price)

                except aiohttp.ClientError as e:
                    logger.error(f"HTTP error: {e}. Retrying in 60s...")
                    FAILED_MESSAGES.inc()
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")

                await asyncio.sleep(60)

    finally:
        await producer.stop()


if __name__ == "__main__":
    try:
        start_http_server(METRICS_PORT, addr='0.0.0.0')
        logger.info('Started http server')
        asyncio.run(fetch_and_produce_data())
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
