
import json
import requests
import sys
from datetime import datetime, timedelta
from time import sleep
import random
import yfinance as yf

from kafka import KafkaProducer

def fetch_stock_data():
    with open('/home/jovyan/notebooks/stock_indexes.txt', 'r') as file:
        for line in file:
            symbol = line.strip()
            stock = yf.Ticker(symbol)
            try:
                stock_info = stock.info
                if stock_info:
                    message = {'type': 'Stock',
                               "date": datetime.now().isoformat(),
                               'symbol': stock_info.get('symbol', 'No Data'),
                               'currentPrice': stock_info.get('currentPrice', 'No Data'),
                               'open': stock_info.get('open', 'No Data'),
                               'previousClose': stock_info.get('previousClose', 'No Data'),
                               'dayLow': stock_info.get('dayLow', 'No Data'),
                               'dayHigh': stock_info.get('dayHigh', 'No Data'),
                               'recommendation': stock_info.get('recommendationKey', 'No Data'),
                               
                               'targetHighPrice': stock_info.get('targetHighPrice', 'No Data'),
                               'targetLowPrice': stock_info.get('targetLowPrice', 'No Data'),
                               'targetMeanPrice': stock_info.get('targetMeanPrice', 'No Data'),
                               'targetMedianPrice': stock_info.get('targetMedianPrice', 'No Data'),
                               'recommendationMean': stock_info.get('recommendationMean', 'No Data'),
                               
                               'totalRevenue': stock_info.get('totalRevenue', 'No Data'),
                               'revenuePerShare': stock_info.get('revenuePerShare', 'No Data'),
                               'totalDebt': stock_info.get('totalDebt', 'No Data'),
                               'debtToEquity': stock_info.get('debtToEquity', 'No Data'),
                               'totalCash': stock_info.get('totalCash', 'No Data'),
                               'totalCashPerShare': stock_info.get('totalCashPerShare', 'No Data'),
                               'ebitda': stock_info.get('ebitda', 'No Data'),
                               'earningsGrowth': stock_info.get('earningsGrowth', 'No Data'),
                               'revenueGrowth': stock_info.get('revenueGrowth', 'No Data')
                              }
                    yield message
                else:
                    print("No data for symbol:", symbol)
            except:
                print("Errorfor symbol", symbol)

        sleep(10)

if __name__ == "__main__":
    SERVER = "broker:9092"

    stock_producer = KafkaProducer(
        bootstrap_servers=[SERVER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        api_version=(3, 7, 0),)
    
    try:
        while True:
            for stock_data in fetch_stock_data():
                stock_producer.send("stock", value=stock_data)

    except KeyboardInterrupt:
        stock_producer.close()
