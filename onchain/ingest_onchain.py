import os, asyncio, json, time, aiohttp
import logging
from aiokafka import AIOKafkaProducer
from prometheus_client import Counter, start_http_server, Gauge
from dotenv import load_dotenv

load_dotenv()

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- Configuration ---
API_KEY = os.getenv("ETHERSCAN_API_KEY")
WHALE_ADDRESS = os.getenv("WHALE_ADDRESS", "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8")
BASE_URL = os.getenv("ETHERSCAN_API_URL", "https://api.etherscan.io/api")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "onchain.raw")
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 9106))

# --- Metrics ---
MSGS_SENT = Counter("onchain_msgs_sent_total", "On-chain messages produced")
FAILED_MESSAGES = Counter("onchain_failed_msgs_total", "On-chain messages that failed to produce")
LAST_PROCESSED_BLOCK = Gauge("onchain_last_processed_block", "Last block number successfully processed")


async def fetch_and_produce():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    await producer.start()
    log.info('✅ Connected to Kafka')

    start_http_server(PROMETHEUS_PORT)
    log.info(f'✅ Prometheus metrics server started on port {PROMETHEUS_PORT}')

    # Store the last block number to fetch only new data
    last_block = 0

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    # Fetch transactions from the last processed block to the latest
                    url = f"https://api.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&tag=latest&boolean=true&apikey={API_KEY}"
                    log.info(f'attempting to fetch data from api {url}')
                    async with session.get(url, timeout=10) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                    if data['result'] and isinstance(data['result'], list):
                        transactions = data['result']

                        # Sort transactions to process them in order and find the new 'last_block'
                        transactions.sort(key=lambda x: int(x['blockNumber']))

                        for tx in transactions:
                            # Skip transactions from blocks we have already processed
                            block_number = int(tx['blockNumber'])
                            if block_number <= last_block:
                                continue

                            # The transaction is a single message
                            msg = {
                                "ts": int(time.time() * 1000),
                                "tx_hash": tx['hash'],
                                "block_number": block_number,
                                "from_address": tx['from'],
                                "to_address": tx['to'],
                                "value_eth": int(tx['value']) / 10 ** 18,  # Convert from Wei to Ether
                                "payload": tx
                            }

                            await producer.send_and_wait(KAFKA_TOPIC, value=msg)
                            MSGS_SENT.inc()
                            log.info(f"Produced message for transaction: {tx['hash']} in block {block_number}")

                            # Update the last processed block
                            last_block = block_number
                            LAST_PROCESSED_BLOCK.set(last_block)


                except aiohttp.ClientError as e:
                    log.error(f"HTTP error fetching data: {e}. Retrying...")
                    FAILED_MESSAGES.inc()
                except Exception as e:
                    log.critical(f"An unexpected error occurred: {e}", exc_info=True)
                    FAILED_MESSAGES.inc()

                await asyncio.sleep(15)  # Fetch new data every 15 seconds

    finally:
        await producer.stop()
        log.info("Producer stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(fetch_and_produce())
    except KeyboardInterrupt:
        log.info("Producer stopped by user.")