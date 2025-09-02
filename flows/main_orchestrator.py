from prefect import flow, get_run_logger
import asyncio

# Import your existing flows as sub-flows
from producers_flows import producers_flow
from storers_flow import storers_flow
from processors_flow import processors_flow
from anomaly_flow import anomaly_detection_flow


# from materialized_views_flow import refresh_materialized_views_flow # You must create this

@flow(name="Crypto Intelligence Pipeline - Master Orchestrator")
def master_orchestrator():
    """
    Main orchestration flow for the Crypto Intelligence Pipeline.
    This flow serves as the single point of control, ensuring all
    sub-pipelines run in the correct, managed state.
    """
    logger = get_run_logger()

    # Kick off the long-running producer, storer, and processor flows.
    # We submit them so they run as separate flows for visibility and isolation.
    logger.info("Starting continuous data ingestion and processing services.")

    # Correctly submit the sub-flows as separate runs for visibility
    producers_flow.submit()
    storers_flow.submit()
    processors_flow.submit()

    # Schedule the batch jobs.
    # This is a placeholder for your refresh logic.
    logger.info("Scheduling materialized view refresh and batch anomaly detection.")