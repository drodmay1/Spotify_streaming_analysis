from kafka import KafkaProducer
import json
import time

# Define Kafka settings
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "spotify_streams"

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Sample messages (you will replace this with real Spotify data later)
sample_data = [
    {"track": "Seven", "artist": "Jung Kook", "streams": 500000},
    {"track": "LALA", "artist": "Myke Towers", "streams": 400000},
    {"track": "vampire", "artist": "Olivia Rodrigo", "streams": 600000}
]

for message in sample_data:
    producer.send(KAFKA_TOPIC, message)
    print(f"Sent: {message}")
    time.sleep(1)  # Delay for readability

producer.close()
