# from prefect import flow, task, get_run_logger
# from sqlalchemy import create_engine, text
# import os
#
# # A task to connect to the database. This is a good practice to centralize connection logic.
# @task(name="connect_to_postgres")
# def get_db_engine():
#     """Returns a SQLAlchemy engine to connect to the Postgres database."""
#     db_user = os.getenv("POSTGRES_USER", "postgres")
#     db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
#     db_host = os.getenv("POSTGRES_HOST", "postgres")
#     db_port = os.getenv("POSTGRES_PORT", "5432")
#     db_name = os.getenv("POSTGRES_DB", "crypto_db")
#     database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
#     return create_engine(database_url)
#
# # A task to execute a SQL query. This is a reusable, high-standard task.
# @task(name="execute_sql")
# def execute_sql_task(sql_query: str):
#     """Executes a given SQL query against the Postgres database."""
#     logger = get_run_logger()
#     engine = get_db_engine()
#     try:
#         with engine.connect() as conn:
#             conn.execute(text(sql_query))
#             conn.commit()
#             logger.info("SQL query executed successfully.")
#     except Exception as e:
#         logger.error(f"Error executing SQL query: {e}")
#         raise
#
# @flow(name="Materialized Views - Refresh")
# def refresh_materialized_views_flow():
#     """
#     Refreshes the materialized views for the Crypto Intelligence Pipeline.
#     This flow is designed to be run on a schedule.
#     """
#     logger = get_run_logger()
#     logger.info("Starting materialized view refresh.")
#
#     # SQL to refresh the views. These should be the actual queries for your project.
#     # The blueprint mentions these, so you must define them.
#     refresh_price_view_sql = """
#     REFRESH MATERIALIZED VIEW prices_15m;
#     """
#     refresh_news_view_sql = """
#     REFRESH MATERIALIZED VIEW news_15m;
#     """
#     refresh_sentiment_view_sql = """
#     REFRESH MATERIALIZED VIEW price_sentiment_correlation;
#     """
#
#     # Call the reusable task to refresh each view.
#     execute_sql_task.submit(refresh_price_view_sql)
#     execute_sql_task.submit(refresh_news_view_sql)
#     execute_sql_task.submit(refresh_sentiment_view_sql)
#
#     logger.info("Materialized views refresh submitted successfully.")