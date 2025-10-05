# crypto-kafka-pipeline (minimal)

This project provides a minimal local Kafka pipeline that fetches crypto prices from CoinGecko, processes them with Faust, and stores results in a local MongoDB instance (e.g., MongoDB Compass).

Folders:
- producer/: Kafka producer fetching CoinGecko API data
- stream/: Faust stream that enriches and forwards data
- consumer/: Kafka consumer inserting into local MongoDB

Quick start (Windows cmd.exe):

1. Start Kafka & Zookeeper

    docker-compose up -d

2. Create a Python virtualenv and install deps

    python -m venv .venv
    .venv\Scripts\activate
    pip install -r requirements.txt

3. Run the producer

    python producer\producer.py

4. Run the stream (Faust worker)

    python stream\stream.py worker -l info

5. Run the consumer

    python consumer\consumer.py

6. Verify in MongoDB Compass: connect to mongodb://localhost:27017/, open database `crypto_db` and collection `prices`.

Notes:
- Docker only contains Kafka and Zookeeper. MongoDB is expected to be running locally.
- If you run into dependency issues with Faust, consider using a separate environment or replacing Faust with a simple Kafka consumer-producer transform.
