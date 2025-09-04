#!/usr/bin/env python3
from src.kafka.naver_stock_producer import NaverStockProducer
import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    producer = NaverStockProducer()
    producer.start_streaming()
