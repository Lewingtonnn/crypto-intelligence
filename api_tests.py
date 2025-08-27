import requests

# # Get all available coins
all_coins = requests.get("https://api.coingecko.com/api/v3/coins/list").json()

# Example: user chooses coins dynamically
chosen = ["bitcoin", "ethereum", "dogecoin"]
coins = ",".join(chosen)

# Get their prices
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {"vs_currency": "usd", "ids": coins}

data = requests.get(url, params=params).json()

for coin in data:
    print(coin["id"], coin["current_price"])
    print(coin)
