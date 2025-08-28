import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from prometheus_client import Gauge, Counter, start_http_server

from config import KAFKA_SERVERS, KAFKA_TOPIC_NEWS_RAW, KAFKA_TOPIC_NEWS_SCORED, METRICS_PORT_SENTIMENT_SCORER

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Sentiment Analyzer and Metrics ---
analyzer = SentimentIntensityAnalyzer()
MESSAGES_PROCESSED = Counter("messages_processed_total", "Total messages processed and scored")
MESSAGES_SKIPPED = Counter("messages_skipped_total", "Total messages skipped due to errors")
SENTIMENT_SCORE_GAUGE = Gauge("current_sentiment_score", "Sentiment score of the last processed article", ["title"])
SENTIMENT_SCORER_UP = Gauge("sentiment_scorer_status", "Sentiment scorer service status (1=up, 0=down)")


async def score_news():
    consumer, producer = None, None
    try:
        SENTIMENT_SCORER_UP.set(1)
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC_NEWS_RAW,
            bootstrap_servers=KAFKA_SERVERS,
            group_id="sentiment-scorer-group",
            auto_offset_reset="earliest"
        )
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_SERVERS)

        await consumer.start()
        await producer.start()
        logger.info("✅ Kafka Consumer/Producer for Sentiment started")

        async for msg in consumer:
            try:
                # Process the message
                obj = json.loads(msg.value)
                if "title" not in obj:
                    logger.warning(f"Skipping message with no 'title': {obj}")
                    MESSAGES_SKIPPED.inc()
                    continue

                score = analyzer.polarity_scores(obj["title"])["compound"]
                obj["sentiment"] = score

                # Check for idempotency: if we have a key, we can use it to deduplicate
                # For this simple case, we'll just log and send.
                # A more robust solution would use a database to track processed messages.
                await producer.send_and_wait(KAFKA_TOPIC_NEWS_SCORED, json.dumps(obj).encode("utf-8"))

                logger.info(f"📝 Scored: {obj['title']} -> sentiment={score:.3f}")
                MESSAGES_PROCESSED.inc()
                SENTIMENT_SCORE_GAUGE.labels(title=obj['title']).set(score)

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON from message: {e}. Skipping message.")
                MESSAGES_SKIPPED.inc()
            except Exception as e:
                logger.critical(f"An unexpected error occurred during message processing: {e}", exc_info=True)
                MESSAGES_SKIPPED.inc()

    except asyncio.CancelledError:
        logger.info("Task was cancelled, shutting down.")
    finally:
        if consumer:
            await consumer.stop()
            logger.info("Kafka consumer stopped.")
        if producer:
            await producer.stop()
            logger.info("Kafka producer stopped.")
        SENTIMENT_SCORER_UP.set(0)


if __name__ == "__main__":
    start_http_server(METRICS_PORT_SENTIMENT_SCORER)
    logger.info(f"✅ Prometheus server started on port {METRICS_PORT_SENTIMENT_SCORER}")
    asyncio.run(score_news())