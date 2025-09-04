import os
from dotenv import load_dotenv

load_dotenv()

POSTGRESQL_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'stock_data'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password123'),
    'port': int(os.getenv('POSTGRES_PORT', 5432))
}

INFLUXDB_CONFIG = {
    'url': os.getenv('INFLUXDB_URL', 'http://influxdb:8086'),
    'token': os.getenv('INFLUXDB_TOKEN','A0aZWRsN3R4CGDlh0pV-Zv_n0yABGiVgufZaFzBWlL29FBtQTpOzXtvygZrJdHkzMz7aBccfy-ATZCF-wcgLUQ=='),
    'org': os.getenv('INFLUXDB_ORG', 'test'),
    'bucket': os.getenv('INFLUXDB_BUCKET', 'test')
}

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_SERVERS', 'kafka:29092').split(','),
    'topic': os.getenv('KAFKA_TOPIC', 'stock-prices')
}

REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'redis'),
    'port': int(os.getenv('REDIS_PORT', 6379)),
    'db': int(os.getenv('REDIS_DB', 0))
}

# 파이프라인 설정 (오류 방지)
PIPELINE_RESET = os.getenv('PIPELINE_RESET', 'false').lower() == 'true'
INGEST_HISTORICAL = os.getenv('INGEST_HISTORICAL', 'false').lower() == 'true'
