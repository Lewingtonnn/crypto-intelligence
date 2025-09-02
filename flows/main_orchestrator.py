from prefect import flow, get_run_logger
from prefect.client.orchestration import get_client

from producers_flows import producers_flow
from storers_flow import storers_flow
from processors_flow import processors_flow
from anomaly_flow import anomaly_detection_flow
import asyncio
from prefect import flow, get_run_logger

# Import your sub-flows
from producers_flows import producers_flow
from storers_flow import storers_flow
from processors_flow import processors_flow
from anomaly_flow import anomaly_detection_flow
# from materialized_views_flow import refresh_materialized_views_flow


async def run_continuous_flows(logger):
    """
    Run the streaming flows (producers, storers, processors) in parallel.
    These should be long-lived services that never exit unless they fail.
    """
    logger.info("Launching continuous services: producers, storers, processors...")

    await asyncio.gather(
        asyncio.to_thread(producers_flow),
        asyncio.to_thread(storers_flow),
        asyncio.to_thread(processors_flow),
    )


async def run_periodic_jobs(logger):
    """
    Periodically trigger anomaly detection and materialized views refresh.
    Replace the sleep interval with your desired schedule.
    """
    while True:
        logger.info("Triggering batch anomaly detection flow...")
        anomaly_detection_flow()

        # logger.info("Refreshing materialized views...")
        # refresh_materialized_views_flow()

        # Wait before running again (e.g., every 30 mins)
        await asyncio.sleep(1800)


@flow(name="Crypto Intelligence Pipeline - Master Orchestrator")
async def master_orchestrator():
    """
    Supervises both streaming and batch jobs.
    Streaming flows run continuously in parallel.
    Batch jobs run on a periodic loop.
    """
    logger = get_run_logger()

    logger.info("Starting Master Orchestrator for Crypto Intelligence Pipeline.")

    # Run both streaming and periodic batch jobs in parallel
    await asyncio.gather(
        run_continuous_flows(logger),
        run_periodic_jobs(logger),
    )
