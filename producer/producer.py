import time
import json
import requests
from kafka import KafkaProducer

API_key= "daetkcpr01qqo7nu41tgdaetkcpr01qqo7nu41u0"
API_url = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

# Try localhost first, fallback to host.docker.internal
try:
    producer=KafkaProducer(
        bootstrap_servers=["localhost:29092"],
        api_version=(7, 4, 1),
        connections_max_idle_ms=5400,
        max_block_ms=3000,
        request_timeout_ms=300,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Connected to Kafka at localhost:29092", flush=True)
except Exception as e:
    print(f"Failed to connect to localhost:29092, trying host.docker.internal:29092: {e}", flush=True)
    producer=KafkaProducer(
        bootstrap_servers=["host.docker.internal:29092"],
        api_version=(7, 4, 1),
        connections_max_idle_ms=54000,
        max_block_ms=3000,
        request_timeout_ms=3000,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

print("Producer started successfully!", flush=True)

def fetch_quote(symbol):
    url=f"{API_url}?symbol={symbol}&token={API_key}"
    try:
        print(f"Fetching data for {symbol} from {url}", flush=True)
        response=requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        data['symbol'] = symbol
        data['fetched_at'] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}", flush=True)
        return None

while True:
    print(f"Starting fetch cycle at {time.time()}", flush=True)
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing: {quote}", flush=True)
            producer.send("stock_quotes", value=quote)
            print(f"Sent quote for {symbol}: {quote}", flush=True)
    time.sleep(6)  # Fetch quotes every 60 seconds
