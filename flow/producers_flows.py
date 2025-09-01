# flows/producers_flow.py
from prefect import flow, get_run_logger
import asyncio
import signal

logger = get_run_logger()

def _install_sigterm_cancel(loop):
    # Allow graceful shutdown on SIGTERM/SIGINT
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(_cancel_all_tasks()))

async def _cancel_all_tasks():
    logger.info("Shutting down gracefully...")
    for task in asyncio.all_tasks():
        task.cancel()

@flow(name="Producers - Long Running")
async def producers_flow():
    """
    Run all producers (infinite loops) as a single supervisor flow.
    Each underlying function must be an async function that either loops forever,
    or we re-run it in a loop here.
    """
    logger.info("Starting producers supervisor")

    # Import inside function to avoid import cost at top-level
    from ingest.producer import fetch_and_produce_data
    from sentiment.news_ingest import main as news_producer_main
    from onchain.ingest_onchain import fetch_and_produce as onchain_producer_main

    loop = asyncio.get_event_loop()
    _install_sigterm_cancel(loop)

    # Create tasks for each producer. These should be infinite loops in their implementation.
    tasks = [
        asyncio.create_task(fetch_and_produce_data(), name="prices_producer"),
        asyncio.create_task(news_producer_main(), name="news_producer"),
        asyncio.create_task(onchain_producer_main(), name="onchain_producer"),
    ]

    # Wait until cancelled or any producer fails
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    for d in done:
        if d.cancelled():
            continue
        exc = d.exception()
        if exc:
            logger.error("Producer task failed: %s", exc)
            # let Prefect know by re-raising
            raise exc

    logger.info("Producers supervisor exiting")
