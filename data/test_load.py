import os
import pandas as pd

# Construct the full path to the CSV file
file_path = os.path.join(os.getcwd(), "data", "spotify-2023_utf8.csv")

# Load the dataset
df = pd.read_csv(file_path)

# Function to clean numeric columns
def clean_numeric_column(column):
    """ Remove non-numeric characters and convert to float, replacing errors with NaN. """
    return pd.to_numeric(column.astype(str).str.replace(r"[^\d]", "", regex=True), errors="coerce")

# Apply cleaning to 'streams' and other columns
df["streams"] = clean_numeric_column(df["streams"])
df["in_deezer_playlists"] = clean_numeric_column(df["in_deezer_playlists"])
df["in_shazam_charts"] = clean_numeric_column(df["in_shazam_charts"])

# Handle missing values
df["in_shazam_charts"].fillna(0, inplace=True)  # Fill missing values with 0
df["key"].fillna("Unknown", inplace=True)  # Fill missing key values with "Unknown"

# Display cleaned dataset info
df.info()
print("\nFirst 5 rows after cleaning:")
print(df.head())
