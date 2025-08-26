import asyncio
import aiohttp
import json
import logging
from aiokafka import AIOKafkaProducer
from datetime import datetime


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
COIN_IDS = [
    'bitcoin', 'ethereum'
]

KAFKA_TOPIC = "crypto-market-data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"


# ... (your existing imports)

async def fetch_and_produce_data():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    await producer.start()
    logger.info(f'KAFKA Producer started and connected to {KAFKA_BOOTSTRAP_SERVERS}')
    try:
        async with aiohttp.ClientSession() as session:
            params = {
                'vs_currency': 'usd',
                'ids': ','.join(COIN_IDS),
                'per_page': len(COIN_IDS),
                'sparkline': False
            }
            while True:
                try:
                    async with session.get(COINGECKO_API_URL, params=params) as response:
                        response.raise_for_status()
                        data = await response.json()

                        for coin_data in data:
                            # Proactive Preparation: Handle potential None or False values
                            price = coin_data.get('current_price')
                            market_cap = coin_data.get('market_cap')
                            total_volume = coin_data.get('total_volume')

                            # Normalize boolean and None values to a consistent type (e.g., None)
                            if isinstance(price, bool): price = None
                            if isinstance(market_cap, bool): market_cap = None
                            if isinstance(total_volume, bool): total_volume = None

                            message = {
                                'id': coin_data['id'],
                                'current_price': price,
                                'market_cap': market_cap,
                                'total_volume': total_volume,
                                'timestamp': datetime.now(datetime.UTC).isoformat()
                            }

                            # Calibrated Intensity: Use a robust logger for critical events
                            await producer.send_and_wait(
                                KAFKA_TOPIC,
                                key=coin_data['id'].encode('utf-8'),
                                value=message
                            )
                            logger.info(f"Produced message for {coin_data['id']}")

                except aiohttp.ClientError as e:
                    logger.error(f"HTTP error occurred: {e}. Retrying...")
                except Exception as e:
                    logger.error(f"An unexpected error occurred: {e}")

                await asyncio.sleep(60)
    finally:
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(fetch_and_produce_data())
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")