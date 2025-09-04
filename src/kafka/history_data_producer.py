import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HistoryDataProducer:
    def __init__(self, kafka_servers=['localhost:9092']):
        """과거 주식 데이터 대량 수집 Producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )
        
        # 한국 주식 종목
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
    
    def generate_historical_data(self, symbol: str, days: int = 30) -> list:
        """과거 30일간의 모의 주식 데이터 생성"""
        try:
            # 현재 가격 가져오기 (기준점)
            url = f"https://finance.naver.com/item/main.nhn?code={symbol}"
            response = self.session.get(url, timeout=10)
            response.encoding = 'euc-kr'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            price_tag = soup.find('p', {'class': 'no_today'})
            if not price_tag:
                return []
            
            span_tag = price_tag.find('span', {'class': 'blind'})
            if not span_tag:
                return []
            
            current_price = float(span_tag.text.replace(',', ''))
            
            # 과거 데이터 생성
            historical_data = []
            base_date = datetime.now() - timedelta(days=days)
            
            for day in range(days):
                # 주말 제외 (월-금만)
                date = base_date + timedelta(days=day)
                if date.weekday() >= 5:  # 토요일(5), 일요일(6) 제외
                    continue
                
                # 가격 변동 시뮬레이션 (±5% 랜덤 워크)
                daily_change = random.uniform(-0.05, 0.05)
                price = current_price * (1 + daily_change * (days - day) / days)
                
                # 시간별 데이터 생성 (장중 9:00-15:30)
                trading_hours = [
                    (9, 0), (9, 30), (10, 0), (10, 30), (11, 0), (11, 30),
                    (12, 0), (12, 30), (13, 0), (13, 30), (14, 0), (14, 30),
                    (15, 0), (15, 30)
                ]
                
                for hour, minute in trading_hours:
                    timestamp = date.replace(hour=hour, minute=minute, second=0)
                    
                    # 시간별 미세 변동
                    hourly_change = random.uniform(-0.01, 0.01)
                    final_price = price * (1 + hourly_change)
                    
                    stock_data = {
                        'symbol': symbol,
                        'name': self.symbols.get(symbol, 'Unknown'),
                        'timestamp': timestamp.isoformat(),
                        'price': round(final_price, 0),
                        'open': round(final_price * (1 + random.uniform(-0.005, 0.005)), 0),
                        'high': round(final_price * (1 + random.uniform(0.001, 0.02)), 0),
                        'low': round(final_price * (1 + random.uniform(-0.02, -0.001)), 0),
                        'volume': random.randint(1000000, 50000000),
                        'change_percent': round(daily_change * 100, 2),
                        'is_historical': True  # 히스토리 데이터 표시
                    }
                    
                    historical_data.append(stock_data)
            
            return historical_data
            
        except Exception as e:
            logger.error(f"Error generating historical data for {symbol}: {e}")
            return []
    
    def send_historical_data(self):
        """모든 종목의 과거 데이터를 Kafka로 전송"""
        logger.info("📈 Starting historical data collection...")
        
        for symbol in self.symbols.keys():
            logger.info(f"🔄 Processing {self.symbols[symbol]}({symbol})...")
            
            historical_data = self.generate_historical_data(symbol, days=30)
            
            if historical_data:
                sent_count = 0
                for data in historical_data:
                    try:
                        future = self.producer.send('stock-prices', key=symbol, value=data)
                        future.get(timeout=5)
                        sent_count += 1
                        
                        # 진행상황 표시 (10개마다)
                        if sent_count % 10 == 0:
                            logger.info(f"  📤 {sent_count}/{len(historical_data)} sent...")
                        
                    except Exception as e:
                        logger.error(f"Failed to send historical data: {e}")
                
                logger.info(f"✅ {self.symbols[symbol]}: {sent_count}개 히스토리 데이터 전송 완료")
            
            time.sleep(2)  # 서버 부하 방지
        
        self.producer.flush()
        logger.info("🎉 All historical data collection completed!")
    
    def run(self):
        """히스토리 데이터 수집 실행"""
        self.send_historical_data()

if __name__ == "__main__":
    producer = HistoryDataProducer()
    producer.run()
