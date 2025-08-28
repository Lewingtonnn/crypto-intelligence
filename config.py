import os
from dotenv import load_dotenv

load_dotenv()

# --- Kafka Configuration ---
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NEWS_RAW = os.getenv("KAFKA_TOPIC_NEWS_RAW", "news.raw")
KAFKA_TOPIC_NEWS_SCORED = os.getenv("KAFKA_TOPIC_NEWS_SCORED", "news.scored")
KAFKA_TOPIC_CRYPTO_DATA = os.getenv("KAFKA_TOPIC_CRYPTO_DATA", "crypto-market-data")

# --- API Configuration ---
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINS = os.getenv("COINS", "bitcoin,ethereum,dogecoin")
RSS_FEEDS = os.getenv("RSS_FEEDS", "https://news.google.com/rss/search?q=bitcoin,https://news.google.com/rss/search?q=ethereum,https://news.google.com/rss/search?q=solana,https://news.google.com/rss/search?q=cardano,https://news.google.com/rss/search?q=polkadot").split(',')

# --- Application Configuration ---
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL_SECONDS", 60))
METRICS_PORT_PRODUCER = int(os.getenv("METRICS_PORT_PRODUCER", "9101"))
METRICS_PORT_NEWS_INGEST = int(os.getenv("METRICS_PORT_NEWS_INGEST", "9103"))
METRICS_PORT_SENTIMENT_SCORER = int(os.getenv("METRICS_PORT_SENTIMENT_SCORER", "9105"))

# --- Other Constants ---
# Add other constants here as needed.