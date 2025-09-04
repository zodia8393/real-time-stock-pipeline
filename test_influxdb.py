from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# 환경변수/설정값 입력
url = "http://localhost:8086"  # 또는 127.0.0.1
token = "HwVlR61c2qfHC2ezUnboO0y9bEOIL7P2xSFrVPspeuOIqjwYBWgVKJupleBS59mefmf2agmm8WeLoKCHqItY4g=="  # InfluxDB UI에서 복사한 All Access Token
org = "test"
bucket = "test"

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

point = Point("test_measurement") \
    .tag("test", "true") \
    .field("value", 42) \
    .time(datetime.utcnow())

try:
    write_api.write(bucket=bucket, record=point)
    print("✅ Write success!")
except Exception as e:
    print(f"❌ Write error: {e}")

client.close()
