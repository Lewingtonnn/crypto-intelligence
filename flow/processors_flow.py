# flows/processors_flow.py
from prefect import flow, get_run_logger
import asyncio, signal

async def _cancel_all_tasks():
    logger = get_run_logger()
    logger.info("Shutting down gracefully for processors...")

def _install_sigterm_cancel(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(_cancel_all_tasks()))

@flow(name="Processors (Enrich and Score) - Long Running")
async def processors_flow():
    logger = get_run_logger()
    logger.info("Starting processors supervisor")

    from onchain.parse import enrich_onchain_data
    from sentiment.score_and_join_async import score_news

    loop = asyncio.get_event_loop()
    _install_sigterm_cancel(loop)

    tasks = [
        asyncio.create_task(enrich_onchain_data(), name="onchain_enricher"),
        asyncio.create_task(score_news(), name="news_scorer"),
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    for d in done:
        if d.cancelled():
            continue
        exc = d.exception()
        if exc:
            logger.error("Processor task failed: %s", exc)
            raise exc

    logger.info("Processors supervisor exiting")
