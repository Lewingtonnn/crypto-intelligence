# flows/anomaly_flow.py
from prefect import flow, task, get_run_logger
from onchain.anomaly_detector import detect_once
from datetime import timedelta

logger = get_run_logger()

@task
def run_anomaly_detection_once():
    """
    Task to run the anomaly detection process once."""
    logger.info("Running anomaly detection...")
    try:
        detect_once()
        logger.info("Anomaly detection completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during anomaly detection: {e}")

@flow(name="Anomaly Detection - Scheduled")
def anomaly_detection_flow():
    logger.info("Starting anomaly detection job")
    run_anomaly_detection_once.run()
    logger.info("Anomaly detection job complete")


# Example: building a deployment schedule (optional)
# deployment = Deployment.build_from_flow(
#     flow=anomaly_detection_flow,
#     name="anomaly_detection_every_5m",
#     schedule=IntervalSchedule(interval=timedelta(minutes=5)),
# )
# deployment.apply()
