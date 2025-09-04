from kafka import KafkaConsumer
import json
import logging
from datetime import datetime, timedelta
import psycopg2
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import redis
import time
from config.database import (
    KAFKA_CONFIG, 
    POSTGRESQL_CONFIG, 
    INFLUXDB_CONFIG, 
    REDIS_CONFIG,
    PIPELINE_RESET,
    INGEST_HISTORICAL
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockDataConsumer:
    def __init__(self, max_retries=10):
        """Kafka Consumer 및 데이터베이스 연결 설정 (재시도 로직 포함)"""
        self.max_retries = max_retries
        self.consumer = self._create_kafka_consumer_with_retry()
        
        # PostgreSQL 연결
        self.pg_conn = psycopg2.connect(
            host=POSTGRESQL_CONFIG['host'],
            database=POSTGRESQL_CONFIG['database'],
            user=POSTGRESQL_CONFIG['user'],
            password=POSTGRESQL_CONFIG['password'],
            port=POSTGRESQL_CONFIG['port']
        )
        
        # InfluxDB 연결
        self.influx_client = InfluxDBClient(
            url=INFLUXDB_CONFIG['url'],
            token=INFLUXDB_CONFIG['token'], 
            org=INFLUXDB_CONFIG['org']
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        
        # Redis 연결
        self.redis_client = redis.Redis(
            host=REDIS_CONFIG['host'],
            port=REDIS_CONFIG['port'],
            db=REDIS_CONFIG['db'],
            decode_responses=True
        )
        
        self.create_tables()

    def _create_kafka_consumer_with_retry(self):
        """Kafka 연결 재시도 로직"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Attempting Kafka connection... (attempt {attempt + 1}/{self.max_retries})")
                consumer = KafkaConsumer(
                    KAFKA_CONFIG['topic'],
                    bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    key_deserializer=lambda m: m.decode('utf-8') if m else None,
                    group_id='stock-consumer-group',
                    auto_offset_reset='latest',
                    api_version=(2, 8, 0),
                    request_timeout_ms=30000,
                    retry_backoff_ms=100
                )
                logger.info("✅ Kafka connection successful!")
                return consumer
            except Exception as e:
                logger.error(f"❌ Kafka connection failed (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    wait_time = min(2 ** attempt, 30)  # Exponential backoff, max 30s
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached. Unable to connect to Kafka.")
                    raise

    def create_tables(self):
        """PostgreSQL 테이블 생성"""
        with self.pg_conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    open_price DECIMAL(10,2) NOT NULL,
                    high_price DECIMAL(10,2) NOT NULL,
                    low_price DECIMAL(10,2) NOT NULL,
                    volume BIGINT NOT NULL,
                    change_percent DECIMAL(5,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_symbol_timestamp ON stock_prices(symbol, timestamp DESC);
            """)
            self.pg_conn.commit()
        logger.info("✅ PostgreSQL tables created")

    def save_to_postgresql(self, data: dict):
        """PostgreSQL에 데이터 저장"""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO stock_prices 
                    (symbol, timestamp, price, open_price, high_price, low_price, volume, change_percent)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data['symbol'],
                    datetime.fromisoformat(data['timestamp']),
                    data['price'],
                    data['open'],
                    data['high'],
                    data['low'],
                    data['volume'],
                    data['change_percent']
                ))
                self.pg_conn.commit()
                return True
        except Exception as e:
            logger.error(f"PostgreSQL save error: {e}")
            self.pg_conn.rollback()
            return False

    def save_to_influxdb(self, data: dict):
        """InfluxDB에 시계열 데이터 저장"""
        try:
            point = Point("stock_prices") \
                .tag("symbol", data['symbol']) \
                .field("price", data['price']) \
                .field("volume", data['volume']) \
                .field("change_percent", data['change_percent']) \
                .time(datetime.fromisoformat(data['timestamp']))
            
            self.write_api.write(bucket=INFLUXDB_CONFIG['bucket'], record=point)
            return True
        except Exception as e:
            logger.error(f"InfluxDB save error: {e}")
            return False

    def update_redis_cache(self, data: dict):
        """Redis 캐시 업데이트"""
        try:
            cache_key = f"stock:{data['symbol']}"
            cache_data = {
                'price': data['price'],
                'change_percent': data['change_percent'],
                'timestamp': data['timestamp']
            }
            self.redis_client.hset(cache_key, mapping=cache_data)
            self.redis_client.expire(cache_key, 300)
            return True
        except Exception as e:
            logger.error(f"Redis cache error: {e}")
            return False

    def clear_old_data(self):
        """기존 데이터 삭제"""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute("DELETE FROM stock_prices;")
                self.pg_conn.commit()
                logger.info("🧹 Cleared old data in PostgreSQL.")
        except Exception as e:
            logger.error(f"Error clearing old data: {e}")

    def ingest_historical_data(self, days_back: int = 7):
        """과거 데이터 생성 및 저장"""
        logger.info(f"⏳ Beginning to ingest historical data for {days_back} days...")
        
        symbols = ['005930', '000660', '035420', '035720', '207940', '051910']
        base_time = datetime.now() - timedelta(days=days_back)

        for symbol in symbols:
            for day in range(days_back):
                current_time = base_time + timedelta(days=day)
                if current_time.weekday() >= 5:
                    continue
                
                for hour in range(9, 16):
                    for minute in [0, 30]:
                        timestamp = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)
                        price = 100000 + (day * 1000) + (hour * 100) + (minute * 10)
                        
                        data = {
                            'symbol': symbol,
                            'timestamp': timestamp.isoformat(),
                            'price': float(price),
                            'open': float(price * 0.99),
                            'high': float(price * 1.01),
                            'low': float(price * 0.98),
                            'volume': 1000000 + day * 10000,
                            'change_percent': 0.5
                        }
                        
                        self.save_to_postgresql(data)
                        self.save_to_influxdb(data)
                        self.update_redis_cache(data)

        logger.info("✅ Historical data ingestion complete.")

    def process_messages(self):
        """Kafka 메시지 처리"""
        logger.info("🎧 Starting stock data consumer...")
        
        try:
            for message in self.consumer:
                data = message.value
                symbol = data['symbol']
                
                pg_ok = self.save_to_postgresql(data)
                influx_ok = self.save_to_influxdb(data)
                redis_ok = self.update_redis_cache(data)

                if pg_ok and influx_ok and redis_ok:
                    logger.info(f"📊 {symbol}: ${data['price']:.2f} ({data['change_percent']:+.2f}%) - Saved to all DBs")
                else:
                    logger.warning(f"⚠️ {symbol}: Not all DBs saved. Check logs!")

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            self.consumer.close()
            self.pg_conn.close()
            self.influx_client.close()

if __name__ == "__main__":
    consumer = StockDataConsumer()
    
    if PIPELINE_RESET:
        consumer.clear_old_data()
    
    if INGEST_HISTORICAL:
        consumer.ingest_historical_data(days_back=7)
    
    consumer.process_messages()
