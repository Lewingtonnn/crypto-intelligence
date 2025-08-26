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
    'bitcoin', 'ethereum', 'solana', 'dogecoin', 'xrp', 'cardano', 'polkadot',
    'chainlink', 'litecoin', 'avalanche'
]

KAFKA_TOPIC = "crypto-market-data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"


async def fetch_and_produce_data():

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    await producer.start()
    try:

        async with aiohttp.ClientSession() as session:
            # Proactive Preparation ⚔️: Prepare the API request parameters.
            params = {
                'vs_currency': 'usd',
                'ids': ','.join(COIN_IDS),
                'per_page': len(COIN_IDS),
                'sparkline': False
            }
            while True:
                try:
                    # We make an asynchronous GET request to the CoinGecko API.
                    async with session.get(COINGECKO_API_URL, params=params) as response:
                        # Fail fast if the response isn't a success (200 OK).
                        response.raise_for_status()
                        data = await response.json()
                        # We process and send each coin's data to Kafka.
                        for coin_data in data:
                            message = {
                                'id': coin_data['id'],
                                'current_price': coin_data['current_price'],
                                'market_cap': coin_data['market_cap'],
                                'total_volume': coin_data['total_volume'],
                                'timestamp': datetime.utcnow().isoformat()
                            }
                            # Send the message to our Kafka topic. The `key` ensures all data for a
                            # specific coin goes to the same partition, maintaining order.
                            await producer.send_and_wait(
                                KAFKA_TOPIC,
                                key=coin_data['id'].encode('utf-8'),
                                value=message
                            )
                            logger.info(f"Produced message for {coin_data['id']}")
                except aiohttp.ClientError as e:
                    # Handle network-related errors gracefully, and don't give up.
                    logger.error(f"HTTP error occurred: {e}. Retrying...")
                except Exception as e:
                    # Catch all other exceptions and log them for post-mortem analysis.
                    logger.error(f"An unexpected error occurred: {e}")

                # This is our heartbeat. We sleep for a minute to avoid being rate-limited.
                await asyncio.sleep(60)
    finally:
        # Crucial for a clean shutdown and portfolio credibility.
        await producer.stop()

# This is the entry point, the single point of failure we must manage.
if __name__ == "__main__":
    try:
        # Run the async function.
        asyncio.run(fetch_and_produce_data())
    except KeyboardInterrupt:
        # Clean shutdown on user interrupt. This is part of building a robust system.
        logger.info("Producer stopped by user.")