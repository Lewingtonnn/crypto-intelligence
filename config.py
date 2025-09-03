import os
from dotenv import load_dotenv

load_dotenv()

# --- Kafka & Core Services ---
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "kafka:9092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DATABASE_URL = os.getenv('DATABASE_URL')
PROMETHEUS_PORT_PRODUCER = int(os.getenv("PROMETHEUS_PORT_PRODUCER", 9101))
PROMETHEUS_PORT_CONSUMER_DB = int(os.getenv("PROMETHEUS_PORT_CONSUMER_DB", 9102))
PROMETHEUS_PORT_NEWS_INGEST = int(os.getenv("PROMETHEUS_PORT_NEWS_INGEST", 9103))
PROMETHEUS_PORT_SENTIMENT_SCORER = int(os.getenv("PROMETHEUS_PORT_SENTIMENT_SCORER", 9104))
PROMETHEUS_PORT_ONCHAIN_INGEST = int(os.getenv("PROMETHEUS_PORT_ONCHAIN_INGEST", 9105))
PROMETHEUS_PORT_ONCHAIN_CONSUMER = int(os.getenv("PROMETHEUS_PORT_ONCHAIN_CONSUMER", 9106))
PROMETHEUS_PORT_ANOMALY_DETECTOR = int(os.getenv("PROMETHEUS_PORT_ANOMALY_DETECTOR", 9107))
PROMETHEUS_PORT_SENTIMENT_TO_DB = int(os.getenv("PROMETHEUS_PORT_SENTIMENT_TO_DB", 9108))

# --- Crypto Price Data ---
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINS = "bitcoin,ethereum,cardano,solana,polkadot"
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL_SECONDS", 15))
KAFKA_TOPIC_CRYPTO_DATA = os.getenv("KAFKA_TOPIC_CRYPTO_DATA", "crypto-market-data")

# --- News Data ---
RSS_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
    "https://www.coindesk.com/feed",
    "https://blog.chain.link/rss/"
]
KAFKA_TOPIC_NEWS_RAW = os.getenv("KAFKA_TOPIC_NEWS_RAW", "news.raw")
KAFKA_TOPIC_NEWS_SCORED = os.getenv("KAFKA_TOPIC_NEWS_SCORED", "news.scored")

# --- On-Chain Data ---
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
WHALE_ADDRESS = os.getenv("WHALE_ADDRESS", "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8")
ETHERSCAN_BASE_URL = os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/api")
KAFKA_TOPIC_ONCHAIN_RAW = os.getenv("KAFKA_TOPIC_ONCHAIN_RAW", "onchain.raw")
KAFKA_TOPIC_ONCHAIN_ENRICHED = os.getenv("KAFKA_TOPIC_ONCHAIN_ENRICHED", "onchain.enriched")

# Add other constants here as needed.
METRICS_PORT_NEWS_INGEST = int(os.getenv("METRICS_PORT_NEWS_INGEST", 9103))
METRICS_PORT_PRODUCER = int(os.getenv("METRICS_PORT_PRODUCER", 9102))
METRICS_PORT_SENTIMENT_SCORER = int(os.getenv("METRICS_PORT_SENTIMENT_SCORER", 9105))
METRICS_PORT_ONCHAIN_INGEST = int(os.getenv("METRICS_PORT_ONCHAIN_INGEST", 9105))
METRICS_PORT_ONCHAIN_CONSUMER = int(os.getenv("METRICS_PORT_ONCHAIN_CONSUMER", 9106))
METRICS_PORT_SENTIMENT_TO_DB = int(os.getenv("METRICS_PORT_SENTIMENT_TO_DB", 9108))