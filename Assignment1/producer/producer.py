import time
import json
import requests
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
TOPIC = "api-raw-data"

if __name__ == '__main__':
    try:
        while True:
            resp = requests.get(API_URL, timeout=10)
            if resp.status_code == 200:
                response = resp.json()
                data = [
                    {"coin": "bitcoin", "price": response.get("bitcoin", {}).get("usd")},
                    {"coin": "ethereum", "price": response.get("ethereum", {}).get("usd")}
                ]
                for record in data:
                    record["timestamp"] = int(time.time())
                    record["status"] = "active"
                    producer.send(TOPIC, record)
                    print("Sent:", record)
                producer.flush()
            else:
                print("CoinGecko returned status", resp.status_code)
            time.sleep(10)
    except KeyboardInterrupt:
        print("Producer stopped by user")
    except Exception as e:
        print("Producer error:", e)
