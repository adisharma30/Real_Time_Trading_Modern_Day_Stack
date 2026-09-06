import time
import json
import requests
from kafka import KafkaProducer

API_key= "daetkcpr01qqo7nu41tgdaetkcpr01qqo7nu41u0"
API_url = "https://finnhub.io//api/v1/quote"
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

producer=KafkaProducer(
    bootstrap_servers=["host.docker.internal:29092"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_quote(symbol):
    url=f"{API_url}?symbol={symbol}&token={API_key}"
