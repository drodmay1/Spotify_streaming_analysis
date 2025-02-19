import streamlit as st
import sqlite3
import pandas as pd
import time
import matplotlib.pyplot as plt

# Set up the Streamlit app
st.set_page_config(page_title="Spotify Streaming Dashboard", layout="wide")

st.title("🎵 Spotify Streaming Dashboard")

# Connect to the SQLite database
DB_PATH = "data/spotify.db"

def load_data():
    """Load streaming data from the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT track, artist, streams, genre, timestamp FROM spotify_streams"
    df = pd.read_sql(query, conn)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", infer_datetime_format=True)
    conn.close()
    return df

# Sidebar for filtering options
st.sidebar.header("Filter Options")
selected_genre = st.sidebar.selectbox("Select Genre", ["All"] + list(load_data()["genre"].unique()))

# Load and filter data
df = load_data()
if selected_genre != "All":
    df = df[df["genre"] == selected_genre]

# Display metrics
total_streams = df["streams"].sum()
unique_tracks = df["track"].nunique()
unique_artists = df["artist"].nunique()

col1, col2, col3 = st.columns(3)
col1.metric("Total Streams", f"{total_streams:,}")
col2.metric("Unique Tracks", f"{unique_tracks}")
col3.metric("Unique Artists", f"{unique_artists}")

# Visualizations
st.subheader("📊 Top 10 Streamed Songs")
top_songs = df.groupby("track").agg({"streams": "sum"}).nlargest(10, "streams")

st.bar_chart(top_songs)

# Fix for the pie chart issue
st.subheader("🥧 Genre Distribution")
genre_distribution = df.groupby("genre").agg({"streams": "sum"})

fig, ax = plt.subplots()
ax.pie(genre_distribution["streams"], labels=genre_distribution.index, autopct="%1.1f%%", startangle=140)
ax.set_title("Genre Distribution by Streams")

st.pyplot(fig)  # Display Matplotlib pie chart

st.subheader("📈 Streaming Trend Over Time")
df_time = df.groupby(df["timestamp"].dt.date).agg({"streams": "sum"})
st.line_chart(df_time)

# Auto-refresh every 5 seconds
st.write("🔄 Refreshing every 5 seconds...")
time.sleep(5)
st.rerun()
