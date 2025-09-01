from prometheus_client import Gauge, Histogram, Counter

PRICE_SENT = Counter('price_messages_sent_total', "messages produced")
LAST_PRICE = Gauge('last_price_value', 'last price seen', ['symbol'])
FAILED_MESSAGES = Counter('Failed_attempts', "Number of failed messages")
ROWS_WRITTEN = Counter('rows_written', 'number of rows written')
BATCH_LATENCY_SECONDS = Histogram('batch_latency_seconds','number of seconds type shii')
