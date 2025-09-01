# flows/storers_flow.py
from prefect import flow, get_run_logger
import asyncio, signal

async def _cancel_all_tasks():
    logger = get_run_logger()
    logger.info("Shutting down gracefully for storers...")

def _install_sigterm_cancel(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(_cancel_all_tasks()))

@flow(name="Storers - Long Running (Consumers -> DB)")
async def storers_flow():
    logger = get_run_logger()
    logger.info("Starting storers supervisor")

    from ingest.consumer_to_db import consume_and_process as prices_consumer
    from onchain.onchain_consumer_to_db import consume_and_store as onchain_consumer
    from sentiment.sentiment_to_db import consume_and_store as sentiment_consumer

    loop = asyncio.get_event_loop()
    _install_sigterm_cancel(loop)

    tasks = [
        asyncio.create_task(prices_consumer(), name="prices_storer"),
        asyncio.create_task(onchain_consumer(), name="onchain_storer"),
        asyncio.create_task(sentiment_consumer(), name="sentiment_storer"),
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    for d in done:
        if d.cancelled():
            continue
        exc = d.exception()
        if exc:
            logger.error("Storer task failed: %s", exc)
            raise exc

    logger.info("Storers supervisor exiting")
