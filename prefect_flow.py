from prefect import flow, task
import logging
#----import necessary files-----


# Set up logging for Prefect tasks
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



@task(name="Ingest Crypto Prices")
async def ingest_prices_task():
    logger.info("Starting crypto price ingestion.")
    from ingest.producer import fetch_and_produce_data
    await fetch_and_produce_data()
    logger.info("Crypto price ingestion complete.")


@task(name="Ingest News Articles")
async def ingest_news_task():
    logger.info("Starting news ingestion.")
    from sentiment.news_ingest import main
    await main()
    logger.info("News ingestion complete.")

@task(name="Ingest Onchain data")
async def ingest_onchain_task():
    logger.info("Starting onchain data ingestion.")
    from onchain.ingest_onchain import fetch_and_produce
    await fetch_and_produce()
    logger.info("Onchain data ingestion complete.")

@task(name="Enrich Onchain data")
async def enrich_onchain_task():
    logger.info("Starting onchain data enrichment.")
    from onchain.parse import enrich_onchain_data as enrich_loop
    await enrich_loop()
    logger.info("Onchain data enrichment complete.")

@task(name="Anomaly Detection")
async def anomaly_detection_task():
    logger.info("Starting anomaly detection.")
    from onchain.anomaly_detector import main_loop as detect_anomalies
    await detect_anomalies()
    logger.info("Anomaly detection complete.")

@task(name="Onchain to DB")
async def onchain_to_db_task():
    logger.info("Starting onchain data to DB.")
    from onchain.onchain_consumer_to_db import consume_and_store
    await consume_and_store()
    logger.info("Onchain data stored to DB.")

@task(name="Score News Sentiment")
async def score_sentiment_task():
    logger.info("Starting sentiment scoring.")
    from sentiment.score_and_join_async import score_news
    await score_news()
    logger.info("Sentiment scoring complete.")


@task(name="Store Prices to DB")
async def store_prices_task():
    logger.info("Starting crypto prices to DB.")
    from ingest.consumer_to_db import consume_and_process
    await consume_and_process()
    logger.info("Crypto prices stored.")


@task(name="Store Sentiment to DB")
async def store_sentiment_task():
    logger.info("Starting news sentiment to DB.")
    from sentiment.sentiment_to_db import consume_and_store
    await consume_and_store()
    logger.info("News sentiment stored.")



@flow(name="Crypto Data Pipeline", log_prints=True)
async def crypto_data_pipeline_flow():
    logger.info("Running the full crypto data pipeline.")

    # RUN ALL TASKS IN PARALLEL
    ingest_prices_result = ingest_prices_task()
    ingest_news_result = ingest_news_task()
    ingest_onchain_result = ingest_onchain_task()



    logger.info("Crypto data pipeline flow completed.")


if __name__ == "__main__":
    import asyncio

    asyncio.run(crypto_data_pipeline_flow())
