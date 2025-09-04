import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
import schedule
import random
from config.database import KAFKA_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SmartStockProducer:
    def __init__(self, max_retries=10):
        """시장 상황에 따라 자동 전환하는 스마트 Producer (재시도 로직 포함)"""
        self.max_retries = max_retries
        self.producer = self._create_kafka_producer_with_retry()
        
        self.symbols = {
            '005930': '삼성전자',
            '000660': 'SK하이닉스', 
            '035420': 'NAVER',
            '035720': '카카오',
            '207940': '삼성바이오로직스',
            '051910': 'LG화학'
        }
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        self.last_prices = {}

    def _create_kafka_producer_with_retry(self):
        """Kafka Producer 연결 재시도 로직"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Attempting Kafka Producer connection... (attempt {attempt + 1}/{self.max_retries})")
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    api_version=(2, 8, 0),
                    request_timeout_ms=30000,
                    retry_backoff_ms=100
                )
                logger.info("✅ Kafka Producer connection successful!")
                return producer
            except Exception as e:
                logger.error(f"❌ Kafka Producer connection failed (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    wait_time = min(2 ** attempt, 30)  # Exponential backoff, max 30s
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached. Unable to connect to Kafka.")
                    raise
    
    def is_market_open(self) -> bool:
        """한국 주식 시장 개장 시간 확인"""
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        
        market_start = now.replace(hour=9, minute=0, second=0, microsecond=0)
        market_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        return market_start <= now <= market_end
    
    def get_real_price(self, symbol: str) -> dict:
        """실제 네이버 주가 크롤링"""
        try:
            url = f"https://finance.naver.com/item/main.nhn?code={symbol}"
            response = self.session.get(url, timeout=10)
            response.encoding = 'euc-kr'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            price_tag = soup.find('p', {'class': 'no_today'})
            if not price_tag:
                return None
                
            span_tag = price_tag.find('span', {'class': 'blind'})
            if not span_tag:
                return None
            
            current_price = float(span_tag.text.replace(',', ''))
            
            last_price = self.last_prices.get(symbol, current_price)
            change_percent = ((current_price - last_price) / last_price * 100) if last_price else 0.0
            self.last_prices[symbol] = current_price
            
            return {
                'symbol': symbol,
                'name': self.symbols[symbol],
                'timestamp': datetime.now().isoformat(),
                'price': current_price,
                'open': current_price * (1 + random.uniform(-0.01, 0.01)),
                'high': current_price * (1 + random.uniform(0.005, 0.02)),
                'low': current_price * (1 + random.uniform(-0.02, -0.005)),
                'volume': random.randint(1000000, 50000000),
                'change_percent': round(change_percent, 2)
            }
            
        except Exception as e:
            logger.error(f"Error fetching real price for {symbol}: {e}")
            return None
    
    def generate_mock_realtime_data(self, symbol: str) -> dict:
        """주말/시간외 모의 실시간 데이터 생성"""
        base_price = self.last_prices.get(symbol, 100000)
        
        change = random.uniform(-0.005, 0.005)
        new_price = base_price * (1 + change)
        self.last_prices[symbol] = new_price
        
        return {
            'symbol': symbol,
            'name': self.symbols[symbol],
            'timestamp': datetime.now().isoformat(),
            'price': round(new_price, 0),
            'open': round(new_price * (1 + random.uniform(-0.002, 0.002)), 0),
            'high': round(new_price * (1 + random.uniform(0.001, 0.005)), 0),
            'low': round(new_price * (1 + random.uniform(-0.005, -0.001)), 0),
            'volume': random.randint(500000, 5000000),
            'change_percent': round(change * 100, 2)
        }
    
    def send_stock_data(self):
        """시장 상황에 맞는 데이터 전송"""
        market_open = self.is_market_open()
        status = "🟢 MARKET OPEN" if market_open else "🔴 MARKET CLOSED"
        
        logger.info(f"📊 Data Collection - {status}")
        
        for symbol in self.symbols.keys():
            if market_open:
                stock_data = self.get_real_price(symbol)
            else:
                stock_data = self.generate_mock_realtime_data(symbol)
            
            if stock_data:
                try:
                    future = self.producer.send(KAFKA_CONFIG['topic'], key=symbol, value=stock_data)
                    future.get(timeout=10)
                    
                    status_icon = "📈" if market_open else "🎭"
                    logger.info(
                        f"{status_icon} {stock_data['name']}({symbol}): "
                        f"₩{stock_data['price']:,.0f} ({stock_data['change_percent']:+.2f}%)"
                    )
                except Exception as e:
                    logger.error(f"❌ Failed to send {symbol}: {e}")
            
            time.sleep(0.5)
        
        self.producer.flush()
    
    def start_streaming(self):
        """스마트 실시간 스트리밍 시작"""
        logger.info("🚀 Starting Smart Stock Data Streaming...")
        logger.info("📋 Mode: Real-time (Market Hours) + Simulation (Weekend/After Hours)")
        
        schedule.every(30).seconds.do(self.send_stock_data)
        
        self.send_stock_data()
        
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    producer = SmartStockProducer()
    producer.start_streaming()
