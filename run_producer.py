#!/usr/bin/env python3
"""주식 데이터 Producer 실행"""

from src.kafka.stock_producer import StockDataProducer
import logging

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    producer = StockDataProducer()
    producer.start_streaming()
