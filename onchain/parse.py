import os
import asyncio
import json
import logging
import decimal

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from hexbytes import HexBytes
from eth_abi import decode
from eth_utils import to_checksum_address
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "onchain2.raw")
KAFKA_ENRICHED_TOPIC = os.getenv("KAFKA_ENRICHED_TOPIC", "onchain.enriched")
GROUP_ID = "onchain-enrichment-group"

# Function selectors
TRANSFER_SIGNATURE = "a9059cbb"      # transfer(address,uint256)
TRANSFER_FROM_SIGNATURE = "23b872dd" # transferFrom(address,address,uint256)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        return super().default(obj)


async def enrich_onchain_data():
    consumer = AIOKafkaConsumer(
        KAFKA_RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, cls=DecimalEncoder).encode("utf-8"),
    )

    log.info("Starting on-chain data enrichment consumer...")
    await consumer.start()
    await producer.start()

    try:
        log.info("On-chain enrichment process started.")
        async for msg in consumer:
            try:
                payload = msg.value.get("payload", {})
                transactions = payload.get("result", {}).get("transactions", [])
                log.info(f"Processing offset={msg.offset}, {len(transactions)} tx found")

                for tx in transactions:
                    tx_input = tx.get("input", "")
                    if not tx_input or tx_input == "0x":
                        continue

                    function_signature = tx_input[2:10]  # skip "0x"
                    enriched_data = None

                    if function_signature == TRANSFER_SIGNATURE:
                        to_address, value = decode(["address", "uint256"], HexBytes(tx_input))
                        enriched_data = {
                            "type": "transfer",
                            "from_address": tx.get("from"),
                            "to_address": to_checksum_address(to_address.hex()),
                            "value": decimal.Decimal(value),
                            "hash": tx.get("hash"),
                            "block_number": tx.get("blockNumber"),
                            "timestamp": msg.value.get("ts"),
                        }

                    elif function_signature == TRANSFER_FROM_SIGNATURE:
                        from_addr, to_address, value = decode(
                            ["address", "address", "uint256"], HexBytes(tx_input)
                        )
                        enriched_data = {
                            "type": "transferFrom",
                            "from_address": to_checksum_address(from_addr.hex()),
                            "to_address": to_checksum_address(to_address.hex()),
                            "value": decimal.Decimal(value),
                            "hash": tx.get("hash"),
                            "block_number": tx.get("blockNumber"),
                            "timestamp": msg.value.get("ts"),
                        }

                    if enriched_data:
                        log.info(
                            f"✨ Enriched {enriched_data['hash']} ({enriched_data['type']})"
                        )
                        await producer.send_and_wait(
                            KAFKA_ENRICHED_TOPIC, value=enriched_data
                        )

            except asyncio.CancelledError:
                log.info("Enrichment cancelled, shutting down...")
                raise
            except Exception as e:
                log.error(f"Failed to process message at offset {msg.offset}: {e}", exc_info=True)

    except asyncio.CancelledError:
        log.info("Consumer loop cancelled.")
        raise
    finally:
        await consumer.stop()
        await producer.stop()
        log.info("On-chain enrichment process shut down.")


if __name__ == "__main__":
    asyncio.run(enrich_onchain_data())
