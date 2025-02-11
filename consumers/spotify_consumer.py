import json
from kafka import KafkaConsumer
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'spotify_streams',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Data structures for tracking trends
top_songs = defaultdict(int)  # Track top streamed songs
artist_streams = defaultdict(int)  # Total streams per artist
genre_distribution = defaultdict(int)  # Streams per genre

# Function to compute sentiment score from valence_% (if available)
def compute_sentiment(valence):
    return round(valence / 100, 2)  # Normalize between 0 and 1

# Consume messages
print("📥 Listening for new messages...")
for message in consumer:
    data = message.value
    
    track = data['track']
    artist = data['artist']
    streams = data['streams']
    genre = data['genre']
    
    # Update tracking dictionaries
    top_songs[track] += streams
    artist_streams[artist] += streams
    genre_distribution[genre] += streams

    print(f"✅ Processed: {track} | {artist} | Streams: {streams} | Genre: {genre}")

    # Optional: Stop after processing 50 messages (to test)
    if len(top_songs) >= 50:
        break

# Convert tracking data to Pandas DataFrames for visualization
df_top_songs = pd.DataFrame(list(top_songs.items()), columns=['Song', 'Streams']).nlargest(10, 'Streams')
df_genre_distribution = pd.DataFrame(list(genre_distribution.items()), columns=['Genre', 'Streams'])

# Plot Top Streamed Songs (Bar Chart)
plt.figure(figsize=(10, 5))
plt.bar(df_top_songs['Song'], df_top_songs['Streams'], color='blue')
plt.xlabel("Songs")
plt.ylabel("Total Streams")
plt.title("Top 10 Streamed Songs")
plt.xticks(rotation=45, ha='right')
plt.show()

# Plot Genre Distribution (Pie Chart)
plt.figure(figsize=(7, 7))
plt.pie(df_genre_distribution['Streams'], labels=df_genre_distribution['Genre'], autopct='%1.1f%%', startangle=140)
plt.title("Genre Distribution by Streams")
plt.show()

print("✅ Data processing & visualization complete! 🎵")
