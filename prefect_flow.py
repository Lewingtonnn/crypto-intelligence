from prefect import flow, task
import logging

# Set up logging for Prefect tasks
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Prefect Task decorators turn your functions into traceable, observable units.
# The `name` argument is for a clear UI representation.
@task(name="Ingest Crypto Prices")
async def ingest_prices_task():
    logger.info("Starting crypto price ingestion.")
    from producer import fetch_and_produce_data
    await fetch_and_produce_data()
    logger.info("Crypto price ingestion complete.")


@task(name="Ingest News Articles")
async def ingest_news_task():
    logger.info("Starting news ingestion.")
    from news_ingest import main
    await main()
    logger.info("News ingestion complete.")


@task(name="Score News Sentiment")
async def score_sentiment_task():
    logger.info("Starting sentiment scoring.")
    from score_and_join_async import score_news
    await score_news()
    logger.info("Sentiment scoring complete.")


@task(name="Store Prices to DB")
async def store_prices_task():
    logger.info("Starting crypto prices to DB.")
    from consumer_to_db import consume_and_store
    await consume_and_store()
    logger.info("Crypto prices stored.")


@task(name="Store Sentiment to DB")
async def store_sentiment_task():
    logger.info("Starting news sentiment to DB.")
    from sentiment_to_db import consume_and_store
    await consume_and_store()
    logger.info("News sentiment stored.")


# A Prefect Flow is the complete, runnable pipeline.
@flow(name="Crypto Data Pipeline", log_prints=True)
async def crypto_data_pipeline_flow():
    logger.info("Running the full crypto data pipeline.")

    # Define the flow of tasks. Prefect understands dependencies.
    # We can run ingestion tasks in parallel, as they are independent.
    ingest_prices_result = ingest_prices_task()
    ingest_news_result = ingest_news_task()

    # Store tasks can only run after their respective ingestion tasks are complete.
    # This is handled automatically by Prefect.
    store_prices_task(wait_for=[ingest_prices_result])

    # Sentiment scoring must wait for news ingestion to complete.
    score_sentiment_result = score_sentiment_task(wait_for=[ingest_news_result])

    # Storing sentiment to DB must wait for scoring to complete.
    store_sentiment_task(wait_for=[score_sentiment_result])

    logger.info("Crypto data pipeline flow completed.")


if __name__ == "__main__":
    import asyncio

    asyncio.run(crypto_data_pipeline_flow())
