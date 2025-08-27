import time
from aiokafka import AIOKafkaProducer
import json


async def ingest_feeds():
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    FEEDS = [
        'https://news.google.com/rss/search?q=bitcoin',
        'https://news.google.com/rss/search?q=ethereum'
    ]

    SEEN = set()
    while True:
        for url in FEEDS:
            feed = feedparser.parse(url)
            for e in feed.entries
                uid = e.link
                if uid in seen : continue
                seen.add(uid)
                msg = {'timestamp': int(time.time()*1000), "title" :e.title, 'link': e.link
                    }
                producer.send('news.raw', json.dumps(msg).encode('utf-8'))

        time.sleep(60)

