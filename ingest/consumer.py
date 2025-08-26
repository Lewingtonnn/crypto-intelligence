# consumer.py

import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_TOPIC = "crypto-market-data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

async def consume_data():

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="crypto-data-group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    await consumer.start()
    try:
        async for msg in consumer:
            logger.info(f"Consumed message: {msg.topic}:{msg.partition}:{msg.offset}: "
                        f"key={msg.key.decode('utf-8')}, value={msg.value}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        # Clean shutdown is a non-negotiable.
        await consumer.stop()

# This is the entry point.
if __name__ == "__main__":
    try:
        asyncio.run(consume_data())
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")