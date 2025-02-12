import streamlit as st
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Set up the Streamlit app
st.set_page_config(page_title="Spotify Streaming Dashboard", layout="wide")

st.title("🎵 Spotify Streaming Dashboard")

# Connect to the SQLite database
DB_PATH = "data/spotify.db"

def load_data():
    """Load the most recent 100 records from the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT track, artist, streams, genre, sentiment_score, timestamp 
        FROM spotify_streams 
        ORDER BY timestamp DESC 
        LIMIT 100
    """
    df = pd.read_sql(query, conn)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    conn.close()
    return df

# Load data
df = load_data()

# Display sentiment analysis
st.subheader("📊 Average Sentiment Score")
avg_sentiment = df["sentiment_score"].mean()
st.metric(label="Overall Sentiment", value=f"{avg_sentiment:.2f}")

# Visualizing sentiment distribution by genre
st.subheader("📊 Sentiment Distribution by Genre")

sentiment_by_genre = df.groupby("genre").agg({"sentiment_score": "mean"})

fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(sentiment_by_genre.index, sentiment_by_genre["sentiment_score"], color="green")
ax.set_xlabel("Genre")
ax.set_ylabel("Average Sentiment Score")
ax.set_title("Sentiment Score by Genre")
plt.xticks(rotation=45)
st.pyplot(fig)

# Streaming sentiment trend over time
st.subheader("📈 Sentiment Trend Over Time")

df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")  # Ensure timestamp is in datetime format
df["date"] = df["timestamp"].dt.date  # Extract only the date (YYYY-MM-DD)
df_time = df.groupby("date", as_index=False)["sentiment_score"].mean()
df_time = df_time.sort_values("date", ascending=True)

st.line_chart(df_time.set_index("date"))
