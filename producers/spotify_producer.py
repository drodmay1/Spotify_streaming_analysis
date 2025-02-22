import os
import pandas as pd
import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Path to the dataset
file_path = 'producers/data/Spotify_Dataset.csv'

# Debugging: Print absolute path to check if the file exists
absolute_path = os.path.abspath(file_path)
print(f"DEBUG: Looking for dataset at {absolute_path}")

# Try opening the file manually
if not os.path.exists(absolute_path):
    raise FileNotFoundError(f"ERROR: File not found at {absolute_path}")

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

        # Generate timestamps spread over the last 7 days
        random_timestamp = (
            datetime.utcnow() - timedelta(
                days=random.randint(0, 6), 
                hours=random.randint(0, 23), 
                minutes=random.randint(0, 59)
            )
        ).strftime('%Y-%m-%d %H:%M:%S')  # Convert to human-readable format

        # DEBUG: Print generated timestamp
        print(f"DEBUG: Generated timestamp: {random_timestamp}")

        # Create the message
        message = {
            'track': random_row['track_name'],
            'artist': random_row['artist(s)_name'],
            'streams': random_streams,  # Simulated new stream count
            'genre': random_row['genre'],
            'valence': round(random.uniform(0, 1), 2),  # Random valence score between 0 and 1
            'energy': round(random.uniform(0, 1), 2),  # Random energy level
            'danceability': round(random.uniform(0, 1), 2),  # Random danceability level
            'timestamp': random_timestamp  # New timestamp logic
        }

        # Send the message to Kafka
        print(f"DEBUG: Sending message: {json.dumps(message, indent=2)}")  # Debugging print
        producer.send(KAFKA_TOPIC, message)

        # Random delay between messages (0.5 to 2 seconds)
        time.sleep(random.uniform(0.5, 2))

# Start streaming indefinitely
send_data()

