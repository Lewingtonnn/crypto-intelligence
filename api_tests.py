# # # # import requests
# # # #
# # # # # # Get all available coins
# # # # all_coins = requests.get("https://api.coingecko.com/api/v3/coins/list").json()
# # # #
# # # # # Example: user chooses coins dynamically
# # # # chosen = ["bitcoin", "ethereum", "dogecoin"]
# # # # coins = ",".join(chosen)
# # # #
# # # # # Get their prices
# # # # url = "https://api.coingecko.com/api/v3/coins/markets"
# # # # params = {"vs_currency": "usd", "ids": coins}
# # # #
# # # # data = requests.get(url, params=params).json()
# # # #
# # # # for coin in data:
# # # #     print(coin["id"], coin["current_price"])
# # # #     print(coin)
# # #
# # # import asyncio
# # # import os
# # # import asyncpg
# # # import logging
# # #
# # # # Set up logging for clear output
# # # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# # # log = logging.getLogger(__name__)
# # #
# # # async def test_db_connection():
# # #     """
# # #     Attempts to connect to the database and print the current server version.
# # #     """
# # #     db_url = os.getenv('DATABASE_URL')
# # #     if not db_url:
# # #         log.error("DATABASE_URL environment variable is not set.")
# # #         return False
# # #
# # #     try:
# # #         log.info("Attempting to connect to the database...")
# # #         # Create a single connection for the test
# # #         conn = await asyncpg.connect(db_url)
# # #         log.info("Connection successful.")
# # #
# # #         # Execute a simple query to verify the connection
# # #         server_version = await conn.fetchval('SELECT version();')
# # #         log.info(f"Successfully connected to PostgreSQL version: {server_version}")
# # #
# # #         # Close the connection
# # #         await conn.close()
# # #         log.info("Connection closed.")
# # #         return True
# # #
# # #     except Exception as e:
# # #         log.error(f"Failed to connect to the database. Error: {e}")
# # #         return False
# # #
# # # if __name__ == '__main__':
# # #     # Run the test
# # #     success = asyncio.run(test_db_connection())
# # #     if success:
# # #         log.info("Database connection test passed.")
# # #     else:
# # #         log.error("Database connection test failed.")
# #
# # import feedparser
# #
# # url = "https://news.google.com/rss/search?q=bitcoin"
# # feed = feedparser.parse(url)
# #
# # print("Feed title:", feed.feed.title)
# # print("Number of entries:", len(feed.entries))
# #
# # for entry in feed.entries[:3]:  # just show first 3
# #     print("Title:", entry.title)
# #     print("Link:", entry.link)
# #     print("Published:", entry.published)
# #     print("-" * 500)
# #
# #
# # from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# #
# # analyzer = SentimentIntensityAnalyzer()
# #
# # sentences = [
# #     "Bitcoin crashes to $20,000 — investors panic!",
# #     "Ethereum adoption surges as institutions join.",
# #     "The market remains uncertain with mixed signals.",
# #     "GOOD NEWS!!!!!!!!!!!!!!!"
# # ]
# #
# # for text in sentences:
# #     score = analyzer.polarity_scores(text)
# #     print(f"Text: {text}")
# #     print("Scores:", score)
# #     print("-" * 50)
# #
#
#
# from prometheus_client import Counter, start_http_server
# import time
#
# # Define a metric
# REQUESTS = Counter('demo_requests_total', 'Total requests made')
#
# # Start metrics server on port 9105
# start_http_server(9105)
# print("Prometheus metrics exposed on :9105/metrics")
#
# # Fake some increments
# while True:
#     REQUESTS.inc()
#     print("Added one request. Total =", REQUESTS._value.get())
#     time.sleep(2)

import asyncpg, asyncio

async def conn():
    try:
        conn = await asyncpg.connect(
            "postgresql://crypto_pipeline_user:W1nn1ng254@crypto-pipeline-db.cg5g4i6iaa8q.us-east-1.rds.amazonaws.com:5432/cryptodb?sslmode=require"
        )
        print("✅ Connected to AWS Postgres successfully!")
        await conn.close()
    except Exception as e:
        print("❌ Connection failed:", str(e))

asyncio.run(conn())

