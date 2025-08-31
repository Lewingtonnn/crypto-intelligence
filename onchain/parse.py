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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "onchain2.raw")
KAFKA_ENRICHED_TOPIC = os.getenv("KAFKA_ENRICHED_TOPIC", "onchain.enriched")
GROUP_ID = "onchain-enrichment-group"

# Function selectors for ERC-20 transfers
# transfer(address to, uint256 value)
TRANSFER_SIGNATURE = "a9059cbb"
# transferFrom(address from, address to, uint256 value)
TRANSFER_FROM_SIGNATURE = "23b872dd"


async def enrich_onchain_data():
    """
    Consumes raw on-chain data, enriches it by parsing hex input,
    and produces it to a new Kafka topic.
    """
    consumer = AIOKafkaConsumer(
        KAFKA_RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, cls=DecimalEncoder).encode('utf-8')
    )

    log.info("Starting on-chain data enrichment consumer...")
    await consumer.start()
    await producer.start()

    try:
        log.info("On-chain enrichment process started.")
        async for msg in consumer:
            log.info(f"Processing message at offset {msg.offset}")
            payload = msg.value.get("payload", {})
            transactions = payload.get("result", {}).get("transactions", [])
            log.info(f"Found {len(transactions)} transactions in the message.")

            for tx in transactions:
                tx_input = tx.get("input", "")

                # Check if it's a contract interaction with input data
                if not tx_input or tx_input == "0x":
                    continue

                # Strip the '0x' prefix for easier handling
                tx_input = tx_input[2:]

                # Extract the 4-byte function signature
                function_signature = tx_input[:8]

                enriched_data = None

                try:
                    # Case 1: ERC-20 Transfer
                    if function_signature == TRANSFER_SIGNATURE:
                        to_address, value = decode(
                            ['address', 'uint256'],
                            HexBytes('0x' + tx_input[8:])
                        )
                        to_address = to_checksum_address(to_address.hex())
                        enriched_data = {
                            "type": "transfer",
                            "from_address": tx.get("from"),
                            "to_address": to_address,
                            "value": str(decimal.Decimal(value)),  # Use Decimal for precision
                            "hash": tx.get("hash"),
                            "block_number": tx.get("blockNumber"),
                            "timestamp": msg.value.get("ts")
                        }

                    # Case 2: ERC-20 transferFrom
                    elif function_signature == TRANSFER_FROM_SIGNATURE:
                        from_address_param, to_address, value = decode(
                            ['address', 'address', 'uint256'],
                            HexBytes('0x' + tx_input[8:])
                        )
                        to_address = to_checksum_address(to_address.hex())
                        from_address_param=to_checksum_address(from_address_param.hex())
                        enriched_data = {
                            "type": "transferFrom",
                            "from_address": from_address_param,
                            "to_address": to_address,
                            "value": str(decimal.Decimal(value)),
                            "hash": tx.get("hash"),
                            "block_number": tx.get("blockNumber"),
                            "timestamp": msg.value.get("ts")
                        }

                    # If we have enriched data, send it to the new topic
                    if enriched_data:
                        log.info(
                            f"✨ Enriched transaction {enriched_data['hash']}: {enriched_data['type']} for {enriched_data['value']}")
                        await producer.send_and_wait(
                            KAFKA_ENRICHED_TOPIC,
                            value=enriched_data
                        )
                except Exception as e:
                    log.error(f"Failed to parse transaction {tx.get('hash')}: {e}")
                    continue

    finally:
        await consumer.stop()
        await producer.stop()
        log.info("On-chain enrichment process shut down.")


# A helper class for JSON serialization of Decimal objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


if __name__ == "__main__":
    asyncio.run(enrich_onchain_data())