import time
import json
import boto3
from botocore.config import Config
from kafka import KafkaConsumer

s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9002',
    aws_access_key_id='admin',
    aws_secret_access_key='password123'
)

bucket_name = 'bronze-transaction'

consumer = KafkaConsumer(
    'stock-quotes',
    bootstrap_servers=['localhost:29092'],
    enable_auto_commit=True,
    group_id='bronze-consumer-debug-2',
    auto_offset_reset='earliest',
    api_version=(7, 4, 1),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
print('Consumer streaming started! Waiting for Kafka messages...', flush=True)

for message in consumer:
    record = message.value
    symbol = record.get('symbol')
    ts = record.get('fetched_at', int(time.time()))
    key = f'{symbol}/{ts}.json'

    print(f'Received message for {symbol} at {ts}. Uploading to MinIO...', flush=True)

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(record),
            ContentType='application/json'
        )
        print('Saved record for symbol:', symbol, 'at timestamp:', ts, 'to S3 bucket:', bucket_name, 'with key:', key, flush=True)
    except Exception as e:
        print(f'Failed to upload {key} to S3: {e}', flush=True)
        continue
