import asyncio
import json
import time
import feedparser
import logging
from aiokafka import AIOKafkaProducer
from prometheus_client import Counter, Gauge, start_http_server

from config import KAFKA_SERVERS, KAFKA_TOPIC_NEWS_RAW, RSS_FEEDS, POLLING_INTERVAL_SECONDS, METRICS_PORT_NEWS_INGEST

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Prometheus Metrics Setup ---
NEWS_SENT = Counter("news_messages_sent_total", "News messages produced")
NEWS_INGEST_UP = Gauge("news_ingest_status", "News ingest service status (1=up, 0=down)")


async def fetch_feeds(producer):
    """Fetch RSS feeds and send new articles to Kafka."""
    # NOTE: In a real-world scenario, 'seen' would be a persistent store (e.g., Redis, a database)
    # to avoid re-ingesting on service restart. This in-memory set is for demonstration.
    seen = set()

    while True:
        for url in RSS_FEEDS:
            try:
                feed = feedparser.parse(url)
                if feed.bozo:
                    logger.error(f"Error parsing feed {url}: {feed.bozo_exception}")
                    continue

                for e in feed.entries:
                    uid = e.link
                    if uid in seen:
                        continue

                    seen.add(uid)
                    msg = {
                        "ts": int(time.time() * 1000),
                        "title": e.title,
                        "link": e.link,
                        "source_url": url
                    }
                    await producer.send_and_wait(KAFKA_TOPIC_NEWS_RAW, json.dumps(msg).encode("utf-8"))
                    NEWS_SENT.inc()
                    logger.info(f"📰 Sent news: {msg['title']}")
            except Exception as e:
                logger.error(f"Failed to process feed {url}: {e}")

        await asyncio.sleep(POLLING_INTERVAL_SECONDS)


async def main():
    producer = None
    try:
        NEWS_INGEST_UP.set(1)
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_SERVERS)
        await producer.start()
        logger.info("✅ Kafka Producer for News started")

        await fetch_feeds(producer)
    except Exception as e:
        logger.critical(f"A critical error occurred: {e}", exc_info=True)
    finally:
        if producer:
            await producer.stop()
            logger.info("Kafka producer stopped.")
        NEWS_INGEST_UP.set(0)


if __name__ == "__main__":
    start_http_server(METRICS_PORT_NEWS_INGEST)
    logger.info(f"✅ Prometheus server started on port {METRICS_PORT_NEWS_INGEST}")
    asyncio.run(main())