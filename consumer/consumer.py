import time
import boto3
import json
from kafka import KafkaConsumer

s3_client = boto3.client('s3',
                         endpoint_url='https://localhost:9003',
                         aws_access_key_id='admin',
                         aws_secret_access_key='password123')

bucket_name='bronze-transaction'


consumer = KafkaConsumer(
    'stock-quotes',
    bootstrap_servers=['host.docker.internal:29092'],
    enable_auto_commit=True,
    group_id='bronze-consumer',
    auto_offset_reset='earliest',
    api_version=(7, 4, 1),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in consumer:
    record = message.value
    symbol = record.get('symbol', 'unknown')
    ts=record.get('fetched_at', int(time.time()))
    key=f'{symbol}/{ts}.json'

    s3_client.put_object(Bucket=bucket_name,
                         Key=key,
                         Body=json.dumps(record),
                         ContentType='application/json')