import json
from kafka import KafkaConsumer
from pymongo import MongoClient

TOPIC = 'api-processed-data'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
)

# Connect to local MongoDB (adjust URI if needed)
client = MongoClient('mongodb://localhost:27017/')
db = client['crypto_db']
collection = db['prices']

if __name__ == '__main__':
    try:
        for message in consumer:
            payload = message.value
            collection.insert_one(payload)
            print('Inserted:', payload)
    except KeyboardInterrupt:
        print('Consumer stopped by user')
    except Exception as e:
        print('Consumer error:', e)
