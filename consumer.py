import requests

api = "https://api.coingecko.com/api/v3/coins/markets"

data=requests.get(api)

print(data.json())