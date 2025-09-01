import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from prometheus_client import Gauge, Counter, start_http_server

from config import (
    KAFKA_SERVERS,
    KAFKA_TOPIC_NEWS_RAW,
    KAFKA_TOPIC_NEWS_SCORED,
    METRICS_PORT_SENTIMENT_SCORER,
)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Sentiment Analyzer and Metrics ---
analyzer = SentimentIntensityAnalyzer()
MESSAGES_PROCESSED = Counter("messages_processed_total", "Total messages processed and scored")
MESSAGES_SKIPPED = Counter("messages_skipped_total", "Total messages skipped due to errors")
LAST_SENTIMENT_SCORE = Gauge("last_sentiment_score", "Sentiment score of the most recent article")
SENTIMENT_SCORER_UP = Gauge("sentiment_scorer_status", "Sentiment scorer service status (1=up, 0=down)")


async def score_news():
    consumer, producer = None, None
    try:
        SENTIMENT_SCORER_UP.set(1)
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC_NEWS_RAW,
            bootstrap_servers=KAFKA_SERVERS,
            group_id="sentiment-scorer-group",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),  # ✅ decode once
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        await consumer.start()
        await producer.start()
        logger.info("✅ Kafka Consumer/Producer for Sentiment started")

        async for msg in consumer:
            try:
                obj = msg.value  # already deserialized
                if "title" not in obj:
                    logger.warning(f"Skipping message with no 'title': {obj}")
                    MESSAGES_SKIPPED.inc()
                    continue

                score = analyzer.polarity_scores(obj["title"])["compound"]
                obj["sentiment"] = score

                # ✅ set key for idempotency (using link if available)
                key = None
                if "link" in obj:
                    key = obj["link"].encode("utf-8")

                await producer.send_and_wait(KAFKA_TOPIC_NEWS_SCORED, value=obj, key=key)

                logger.info(f"📝 Scored: {obj['title']} -> sentiment={score:.3f}")
                MESSAGES_PROCESSED.inc()
                LAST_SENTIMENT_SCORE.set(score)

            except asyncio.CancelledError:
                logger.info("Sentiment scorer cancelled mid-loop.")
                raise
            except Exception as e:
                logger.error(f"Error during message processing: {e}", exc_info=True)
                MESSAGES_SKIPPED.inc()

    except asyncio.CancelledError:
        logger.info("Task was cancelled, shutting down.")
        raise
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
