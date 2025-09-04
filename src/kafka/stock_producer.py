import json
import logging
from kafka import KafkaProducer
from datetime import datetime
import random
from config.database import KAFKA_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SmartStockProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            api_version=(7, 4, 0),  # 브로커 버전과 맞춤
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10
        )
        self.symbols = {
            '005930': '삼성전자',
            '000660': 'SK하이닉스',
            '035420': 'NAVER',
            '035720': '카카오',
            '207940': '삼성바이오로직스',
            '051910': 'LG화학'
        }

    def generate_mock_data(self, symbol: str):
        return {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'price': float(100000 + random.randint(-1000, 1000)),
            'open': 0.0,
            'high': 0.0,
            'low': 0.0,
            'volume': random.randint(1000000, 50000000),
            'change_percent': round(random.uniform(-5.0, 5.0), 2)
        }

    def send_stock_data(self):
        for symbol in self.symbols.keys():
            data = self.generate_mock_data(symbol)
            try:
                future = self.producer.send(
                    KAFKA_CONFIG['topic'],
                    key=symbol,
                    value=data
                )
                future.get(timeout=10)
                logger.info(f"📈 {self.symbols[symbol]}({symbol}): ₩{data['price']:,.0f} ({data['change_percent']:+.2f}%) [open]")
            except Exception as e:
                logger.error(f"❌ Failed to send {symbol}: {e}")

    def start_streaming(self):
        logger.info("🚀 Starting Smart Stock Data Streaming...")
        while True:
            self.send_stock_data()
            import time
            time.sleep(30)

if __name__ == "__main__":
    producer = SmartStockProducer()
    producer.start_streaming()
