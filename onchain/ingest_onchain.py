# onchain/ingest_onchain.py
import os
import asyncio
import json
import time
import aiohttp
import logging
import signal
from aiokafka import AIOKafkaProducer
from prometheus_client import Counter, Gauge, start_http_server
from dotenv import load_dotenv

load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("onchain_producer")

# Config
API_KEY = os.getenv("ETHERSCAN_API_KEY")
BASE_URL = os.getenv("ETHERSCAN_API_URL", "https://api.etherscan.io/api")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "onchain2.raw")
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 9106))
POLL_INTERVAL = int(os.getenv("ONCHAIN_POLL_INTERVAL_SECONDS", 15))
HTTP_TIMEOUT = int(os.getenv("ONCHAIN_HTTP_TIMEOUT", 10))
MAX_API_RETRIES = int(os.getenv("ONCHAIN_API_MAX_RETRIES", 5))

# Metrics
MSGS_SENT = Counter("onchain_msgs_sent_total", "On-chain messages produced")
FAILED_MESSAGES = Counter("onchain_failed_msgs_total", "On-chain messages that failed to produce")
LAST_PROCESSED_BLOCK = Gauge("onchain_last_processed_block", "Last block number successfully processed")
PRODUCER_UP = Gauge("onchain_producer_status", "Producer service status (1=up,0=down)")

_stop = asyncio.Event()

def _signal_handler():
    log.info("Shutdown signal received for onchain producer.")
    _stop.set()

async def _retry_api_call(fn, retries=MAX_API_RETRIES, base_delay=1.0):
    attempt = 0
    while True:
        try:
            return await fn()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            if attempt > retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            log.warning(f"Transient error when calling API (attempt {attempt}/{retries}): {e}. Retrying in {delay}s")
            await asyncio.sleep(delay)

async def fetch_and_produce():
    producer = None
    PRODUCER_UP.set(0)
    last_block = 0  # keep across iterations in this process

    # start prometheus once
    start_http_server(PROMETHEUS_PORT)
    log.info(f"Prometheus metrics server started on port {PROMETHEUS_PORT}")

    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        await producer.start()
        PRODUCER_UP.set(1)
        log.info("✅ Kafka producer connected for onchain ingest")

        async with aiohttp.ClientSession() as session:
            while not _stop.is_set():
                try:
                    # Build API URL to fetch latest block with transactions
                    url = f"{BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag=latest&boolean=true&apikey={API_KEY}"
                    async def _call():
                        async with session.get(url, timeout=HTTP_TIMEOUT) as resp:
                            resp.raise_for_status()
                            return await resp.json()

                    data = await _retry_api_call(_call)

                    if not data or "result" not in data or not data["result"]:
                        log.debug("No block data returned from API.")
                    else:
                        block = data["result"]
                        txs = block.get("transactions", [])
                        # convert hex blocknumber to int
                        try:
                            block_num = int(block.get("number", "0x0"), 16)
                        except Exception:
                            block_num = 0

                        # sort transactions to preserve ordering
                        txs.sort(key=lambda x: int(x.get("blockNumber", "0x0"), 16) if x.get("blockNumber") else 0)

                        for tx in txs:
                            if _stop.is_set():
                                break
                            try:
                                block_number = int(tx.get("blockNumber", "0x0"), 16) if tx.get("blockNumber") else block_num
                                if block_number <= last_block:
                                    continue

                                msg = {
                                    "ts": int(time.time() * 1000),
                                    "tx_hash": tx.get("hash"),
                                    "block_number": block_number,
                                    "from_address": tx.get("from"),
                                    "to_address": tx.get("to"),
                                    "value_eth": int(tx.get("value", "0x0"), 16) / 10 ** 18 if tx.get("value") else 0.0,
                                    "payload": tx
                                }

                                await producer.send_and_wait(KAFKA_TOPIC, value=msg)
                                MSGS_SENT.inc()
                                last_block = block_number
                                LAST_PROCESSED_BLOCK.set(last_block)
                                log.info(f"Produced tx {tx.get('hash')} block={block_number}")

                            except asyncio.CancelledError:
                                raise
                            except Exception as e:
                                FAILED_MESSAGES.inc()
                                log.error(f"Failed producing transaction {tx.get('hash')}: {e}", exc_info=True)

                except asyncio.CancelledError:
                    log.info("Onchain producer cancelled.")
                    break
                except Exception as e:
                    # transient API or processing error — already retried on API, log and continue
                    log.exception(f"Unexpected error in onchain fetch loop: {e}")
                    FAILED_MESSAGES.inc()

                # delay until next poll, but allow cancellation
                try:
                    await asyncio.wait_for(_stop.wait(), timeout=POLL_INTERVAL)
                except asyncio.TimeoutError:
                    continue

    finally:
        # teardown
        try:
            if producer:
                await producer.stop()
        except Exception as e:
            log.warning(f"Producer.stop() raised during shutdown: {e}")
        PRODUCER_UP.set(0)
        log.info("Onchain producer shutdown complete.")

if __name__ == "__main__":
    import platform
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    if platform.system() != "Windows":
        try:
            loop.add_signal_handler(signal.SIGINT, _signal_handler)
            loop.add_signal_handler(signal.SIGTERM, _signal_handler)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(fetch_and_produce())
    except KeyboardInterrupt:
        log.info("Producer interrupted by user.")
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        log.info("Event loop closed")
