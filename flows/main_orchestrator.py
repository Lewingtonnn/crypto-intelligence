from prefect import flow, get_run_logger
from prefect.client.orchestration import get_client

from producers_flows import producers_flow
from storers_flow import storers_flow
from processors_flow import processors_flow
from anomaly_flow import anomaly_detection_flow

@flow(name="Crypto Intelligence Pipeline - Master Orchestrator")
async def master_orchestrator():
    logger = get_run_logger()
    client = get_client()

    logger.info("Starting continuous services...")

    # Start long-running flows
    prod = await producers_flow.submit()
    stor = await storers_flow.submit()
    proc = await processors_flow.submit()

    # Run anomaly detection as a batch job
    await anomaly_detection_flow.submit()

    # Monitor + restart if needed
    async for flow_run in client.stream_flow_run_logs(prod.id):
        if "FAILED" in flow_run.message:
            logger.error("Producer flow failed. Restarting...")
            await producers_flow.submit()
