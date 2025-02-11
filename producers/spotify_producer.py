import os
import pandas as pd
import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Path to the dataset
file_path = '../data/Spotify_Dataset.csv'  # Move one directory up

# Load the data into a DataFrame
df = pd.read_csv(file_path)

# Convert streams to integer (if not already)
df['streams'] = df['streams'].astype(int)

# Fill missing values
df.fillna({'track_name': 'Unknown', 'artist(s)_name': 'Unknown', 'streams': 0, 'genre': 'Unknown'}, inplace=True)

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define the Kafka topic
KAFKA_TOPIC = 'spotify_streams'

# Function to simulate real-time streaming with random data
def send_data():
    while True:
        # Randomly select a song from the dataset
        random_row = df.sample(n=1).iloc[0]

        # Simulate dynamic streaming counts
        random_streams = random.randint(50000, 5000000)

        # Create the message
        message = {
            'track': random_row['track_name'],
            'artist': random_row['artist(s)_name'],
            'streams': random_streams,  # Simulated new stream count
            'genre': random_row['genre'],
            'timestamp': datetime.utcnow().isoformat()
        }

        # Send the message to Kafka
        producer.send(KAFKA_TOPIC, message)
        print(f"Sent: {message}")

        # Random delay between messages (0.5 to 2 seconds)
        time.sleep(random.uniform(0.5, 2))

# Start streaming indefinitely
send_data()
