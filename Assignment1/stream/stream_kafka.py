import json
import signal
import sys
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = 'localhost:9092'
RAW_TOPIC = 'api-raw-data'
PROCESSED_TOPIC = 'api-processed-data'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
    auto_offset_reset='earliest',
    enable_auto_commit=True,
)

def process_record(record: dict) -> dict | None:
    """Simple filter/enrich: keep bitcoin/ethereum and add price_usd."""
    try:
        if record.get('coin') in ('bitcoin', 'ethereum'):
            record['price_usd'] = record.get('price')
            return record
    except Exception:
        pass
    return None

def shutdown(signum, frame):
    print('Stream shutting down...')
    try:
        consumer.close()
    except Exception:
        pass
    try:
        producer.flush()
        producer.close()
    except Exception:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

if __name__ == '__main__':
    print('Starting stream_kafka.py (kafka-python) - reading', RAW_TOPIC)
    for message in consumer:
        try:
            record = message.value
            processed = process_record(record)
            if processed is not None:
                producer.send(PROCESSED_TOPIC, processed)
                producer.flush()
                print('Processed ->', processed)
        except Exception as exc:
            print('Processing error:', exc)