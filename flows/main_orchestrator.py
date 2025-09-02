from prefect import flow, serve, get_run_logger
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule
from datetime import timedelta, datetime

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
    # We don't wait for them, as they are continuous services.
    logger.info("Starting continuous data ingestion and processing services.")
    producers_flow.submit()
    storers_flow.submit()
    processors_flow.submit()

    # Schedule the batch jobs.
    # This is a placeholder for your refresh logic. The 'refresh_materialized_views_flow'
    # needs to be created. It should contain the SQL `REFRESH MATERIALIZED VIEW` commands.
    # We will trigger this flow on a schedule.
    logger.info("Scheduling materialized view refresh and batch anomaly detection.")

    # The anomaly detection flow should be a dependency of the materialized views.
    # It should wait for the data to be ready before running.
    # We will not call these directly but use Prefect's scheduling via a deployment.

# Define the deployments for your long-running and scheduled jobs
# This is how Prefect knows what to run and when.

# Deployment for the continuous services.
continuous_deployment = Deployment.build_from_flow(
    flow=master_orchestrator,
    name="continuous-pipeline-services",
    tags=["continuous", "production"],
    schedule=(IntervalSchedule(interval=timedelta(minutes=5), anchor_date=datetime.now())),
    description="Deploys the continuous producer, storer, and processor flows.",
)

# # Deployment for the batch analytics/alerts. This is where you connect the dependencies.
# # This assumes you create a materialized_views_flow that triggers the anomaly_detection_flow
# batch_analytics_deployment = Deployment.build_from_flow(
#     flow=refresh_materialized_views_flow,
#     name="batch-analytics-and-alerts",
#     tags=["batch", "analytics"],
#     schedule=(IntervalSchedule(interval=timedelta(minutes=15))),
#     description="Deploys the batched materialized view refreshes and anomaly detection."
# )

if __name__ == "__main__":
    serve(continuous_deployment)