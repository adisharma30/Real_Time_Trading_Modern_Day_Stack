import os
import boto3
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transaction"
LOCAL_DIR = "C:/Users/91787/Python_Repo/Real_Time_Trading_Mds/tmp/minio_downloads"  # use absolute path for Airflow

SNOWFLAKE_USER = "ADISHARMA"
SNOWFLAKE_PRIVATE_KEY_PATH = "/opt/airflow/secrets/rsa_key.p8"
SNOWFLAKE_ACCOUNT = "oc33285.eu-central-1"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCKS_MDS"
SNOWFLAKE_SCHEMA = "COMMON"

def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    local_files = []
    for obj in objects:
        key = obj["Key"]
        local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
        s3.download_file(BUCKET, key, local_file)
        print(f"Downloaded {key} -> {local_file}")
        local_files.append(local_file)
    return local_files

def load_to_snowflake(**kwargs):
    local_files = kwargs['ti'].xcom_pull(task_ids='download_minio')
    if not local_files:
        print("No files to load.")
        return

    with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        private_key=pkb,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()

    for f in local_files:
        cur.execute(f"PUT file://{f} @%bronze_stock_quote_raw")
        print(f"Uploaded {f} to Snowflake stage")

    cur.execute("""
        COPY INTO bronze_stock_quote_raw
        FROM @%bronze_stock_quote_raw
        FILE_FORMAT = (TYPE=JSON)
    """)
    print("COPY INTO executed")

    cur.close()
    conn.close()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # every 1 minutes
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    task1 >> task2