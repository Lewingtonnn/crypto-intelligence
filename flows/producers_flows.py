from prefect import flow, get_run_logger
import asyncio

@flow(name="Producers - Long Running")
async def producers_flow():
    logger = get_run_logger()
    logger.info("Starting producers supervisor")

    from ingest.producer import fetch_and_produce_data
    from sentiment.news_ingest import main as news_producer_main
    from onchain.ingest_onchain import fetch_and_produce as onchain_producer_main

    tasks = [
        asyncio.create_task(fetch_and_produce_data(), name="prices_producer"),
        asyncio.create_task(news_producer_main(), name="news_producer"),
        asyncio.create_task(onchain_producer_main(), name="onchain_producer"),
    ]

    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    except asyncio.CancelledError:
        logger.info("Producers supervisor received cancel signal. Shutting down gracefully...")
        # Gracefully cancel pending tasks if the flows is cancelled
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    else:
        # If an exception occurs, gather exceptions from done tasks
        for d in done:
            if d.cancelled():
                continue
            exc = d.exception()
            if exc:
                logger.error("Producer task failed: %s", exc)
                raise exc
    finally:
        logger.info("Producers supervisor exiting")