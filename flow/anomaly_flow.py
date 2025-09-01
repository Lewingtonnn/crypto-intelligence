# flows/anomaly_flow.py
from prefect import flow, task, get_run_logger
from onchain.anomaly_detector import detect_once

@task
def run_anomaly_detection_once():
    logger = get_run_logger()
    logger.info("Running anomaly detection...")
    try:
        detect_once()
        logger.info("Anomaly detection completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during anomaly detection: {e}")

@flow(name="Anomaly Detection - Scheduled")
def anomaly_detection_flow():
    logger = get_run_logger()
    logger.info("Starting anomaly detection job")
    run_anomaly_detection_once()
    logger.info("Anomaly detection job complete")
