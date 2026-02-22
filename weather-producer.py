import os
import time
import random
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# CRITICAL: Read the environment variable set in docker-compose.yml
# Inside Docker, this will be 'kafka:9092'
# If this variable is missing, it defaults to localhost (which will fail inside Docker)
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC_NAME = 'weather_data'

print(f"⚙️ Configuration: Connecting to {KAFKA_BROKER}...")

def create_producer():
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                api_version=(0, 10, 1)
            )
            print("✅ Connected to Kafka!")
        except NoBrokersAvailable:
            print("⏳ Kafka not ready. Retrying in 2 seconds...")
            time.sleep(2)
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(2)
    return producer

if __name__ == "__main__":
    producer = create_producer()
    cities = ["London", "New York", "Tokyo", "Paris", "Mumbai"]

    try:
        while True:
            data = {
                'city': random.choice(cities),
                'temp': random.randint(10, 40),
                'timestamp': int(time.time())
            }
            producer.send(TOPIC_NAME, value=data)
            print(f"Sent: {data}")
            time.sleep(1)
    except KeyboardInterrupt:
        producer.close()