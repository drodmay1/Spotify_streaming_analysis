import os
import pandas as pd
import json
import time
from kafka import KafkaProducer

# Directly using the absolute path to the CSV file
file_path = '/Users/davidrodriguez/Documents/44671_6/Spotify_streaming_analysis/Spotify_streaming_analysis/data/spotify-2023_utf8.csv'

# Print the file path to debug
print(f"Using file path: {file_path}")

# Load the data into a DataFrame
df = pd.read_csv(file_path)

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define the topic
KAFKA_TOPIC = 'spotify_streams'

# Function to send data
def send_data():
    for index, row in df.iterrows():
        message = {
            'track': row['track_name'],
            'artist': row['artist(s)_name'],
            'streams': row['streams']
        }
        producer.send(KAFKA_TOPIC, message)
        print(f"Sent: {message}")
        time.sleep(1)  # Send a message every second to simulate real-time data

# Start sending data
send_data()
