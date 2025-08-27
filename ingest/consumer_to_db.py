import logging
import os, json
import aiobotocore, asyncpg
from aiokafka import AIOKafkaConsumer
from datetime import datetime
from metrics import ROWS_WRITTEN, BATCH_LATENCY_SECONDS
from prometheus_client import start_http_server

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BATCH_SIZE = 200
KAFKA_TOPIC = 'prices.ticker'

async def create_prices_tables(conn):
    await conn.execute('''CREATE TABLE IF NPOT EXISTS prices (timestamp BIGINT, coin_name ''')