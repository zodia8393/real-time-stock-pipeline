#!/usr/bin/env python3
"""주식 데이터 Consumer 실행"""

from src.kafka.stock_consumer import StockDataConsumer
import logging

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    consumer = StockDataConsumer()
    consumer.process_messages()
