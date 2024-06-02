
import json
import requests
import sys
from datetime import datetime, timedelta
from time import sleep
import random
import yfinance as yf

from kafka import KafkaProducer

def fetch_crypto_data():
    with open('/home/jovyan/notebooks/crypto_index.txt', 'r') as file:
        ids = file.readlines()
        ids = [id.strip() for id in ids]
    
    for id in ids:
        url = f"https://api.coingecko.com/api/v3/coins/markets?ids={id}&vs_currency=usd"
    
        headers = {
            "accept": "application/json",
            "x-cg-demo-api-key": "CG-4zHc6u3aAS8gNQbRbR5ruUPb"
        }
    
        response = requests.get(url, headers=headers)
    
        if response.status_code == 200:
            data = response.json()
            if data:
                entry = {
                    "type": "Crypto",
                    "date": datetime.now().isoformat(), 
                    "name": data[0]["name"],
                    "current_price": data[0]["current_price"],
                    "high_24h": data[0]["high_24h"],
                    "low_24h": data[0]["low_24h"],
                    "price_change_24h": data[0]["price_change_24h"],
                    "price_change_percentage_24h": data[0]["price_change_percentage_24h"],
                    "market_cap": data[0]["market_cap"],
                    "total_volume": data[0]["total_volume"],
                    "ath": data[0]["ath"],
                    "atl": data[0]["atl"]
                }
                yield entry
            else:
                print(f"No data for ID: {id}")
        else:
            print(f"Error for ID: {id} code: {response.status_code}")

    sleep(10)

if __name__ == "__main__":
    SERVER = "broker:9092"
    
    crypto_producer = KafkaProducer(
        bootstrap_servers=[SERVER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        api_version=(3, 7, 0),)

    
    try:
        while True:
            
            for crypto_data in fetch_crypto_data():
                crypto_producer.send("crypto", value=crypto_data)
                
    except KeyboardInterrupt:
        crypto_producer.close()
