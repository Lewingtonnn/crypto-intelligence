from prefect import flow, get_run_logger
from producers_flows import producers_flow
from storers_flow import storers_flow
from processors_flow import processors_flow


@flow(name="Crypto Intelligence Pipeline - Master Orchestrator")
def master_orchestrator():
    """
    Main orchestration flow for the Crypto Intelligence Pipeline.
    This flow serves as the single point of control, ensuring all
    sub-pipelines run in the correct, managed state.
    """
    logger = get_run_logger()

    # Kick off the long-running producer, storer, and processor flows.
    # We don't wait for them, as they are continuous services.
    logger.info("Starting continuous data ingestion and processing services.")
    producers_flow.submit()
    storers_flow.submit()
    processors_flow.submit()
    logger.info("Data ingestion and processing services started.")

    # Schedule the batch jobs.
    # This is a placeholder for your refresh logic. The 'refresh_materialized_views_flow'
    # needs to be created. It should contain the SQL `REFRESH MATERIALIZED VIEW` commands.
    # We will trigger this flow on a schedule.
    logger.info("Scheduling materialized view refresh and batch anomaly detection.")

    # The anomaly detection flow should be a dependency of the materialized views.
    # It should wait for the data to be ready before running.
    # We will not call these directly but use Prefect's scheduling via a deployment.


# The deployment definitions and `serve()` call have been removed.
# Deployments are now managed externally using the `prefect deploy` CLI command.

# The `if __name__ == "__main__":` block is now empty as it is no longer needed.
# Prefect will discover and run the `master_orchestrator` flow directly.