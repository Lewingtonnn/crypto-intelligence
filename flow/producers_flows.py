# flows/producers_flows.py
from prefect import flow, get_run_logger
import asyncio, signal

async def _cancel_all_tasks():
    logger = get_run_logger()
    logger.info("Shutting down gracefully...")
    for task in asyncio.all_tasks():
        task.cancel()

def _install_sigterm_cancel(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(_cancel_all_tasks()))

@flow(name="Producers - Long Running")
async def producers_flow():
    logger = get_run_logger()
    logger.info("Starting producers supervisor")

    from ingest.producer import fetch_and_produce_data
    from sentiment.news_ingest import main as news_producer_main
    from onchain.ingest_onchain import fetch_and_produce as onchain_producer_main

    loop = asyncio.get_event_loop()
    _install_sigterm_cancel(loop)

    tasks = [
        asyncio.create_task(fetch_and_produce_data(), name="prices_producer"),
        asyncio.create_task(news_producer_main(), name="news_producer"),
        asyncio.create_task(onchain_producer_main(), name="onchain_producer"),
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    for d in done:
        if d.cancelled():
            continue
        exc = d.exception()
        if exc:
            logger.error("Producer task failed: %s", exc)
            raise exc

    logger.info("Producers supervisor exiting")
