import os
import requests
import base64
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

'''
EXTRACT
'''
def get_access_token():
    creds = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post("https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {creds}"},
        data={"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN}
    )
    return r.json()["access_token"]


def get_recently_played(access_token):
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"limit": 50}
    )
    return r.json()


def get_artist_data(access_token, artist_ids):
    unique_ids = list(set(artist_ids))
    artists = []
    for artist_id in unique_ids:
        r = requests.get(f"https://api.spotify.com/v1/artists/{artist_id}",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if r.status_code == 200:
            artists.append(r.json())
        else:
            print(f"failed for {artist_id}: {r.status_code}")
    return artists


# --- run ---
token = get_access_token()
data  = get_recently_played(token)

# flatten recently played into records
records = []
for item in data["items"]:
    track   = item["track"]
    artists = track.get("artists", [])
    records.append({
        "played_at":         item["played_at"],
        "track_id":          track.get("id"),
        "track_name":        track.get("name"),
        "duration_ms":       track.get("duration_ms"),
        "popularity":        track.get("popularity"),
        "album_name":        track.get("album", {}).get("name"),
        "release_date":      track.get("album", {}).get("release_date"),
        "artist_ids":        [a["id"] for a in artists],
        "artist_names":      [a["name"] for a in artists],
        "primary_artist_id": artists[0]["id"] if artists else None,
    })

df = pd.DataFrame(records)

# fetch artist data
all_artist_ids = list(set(aid for ids in df["artist_ids"] for aid in ids))
artists_raw    = get_artist_data(token, all_artist_ids)

artists_df = pd.DataFrame([{
    "artist_id":         a.get("id"),
    "artist_name":       a.get("name"),
    "artist_genres":     a.get("genres", []),
    "artist_popularity": a.get("popularity"),
    "artist_followers":  a.get("followers", {}).get("total"),
} for a in artists_raw if a])

# join artist data onto plays
df = df.merge(artists_df, left_on="primary_artist_id",
                          right_on="artist_id", how="left")

'''
Transformations (formatting for postgres)
'''
# --- transform ---
import numpy as np

# proper timestamp
df["played_at"] = pd.to_datetime(df["played_at"], utc=True)

# convert duration to seconds
df["duration_s"] = (df["duration_ms"] / 1000).round(1)

# flatten lists to comma separated strings
df["artist_ids"]    = df["artist_ids"].apply(lambda x: ",".join(x) if x else None)
df["artist_names"]  = df["artist_names"].apply(lambda x: ",".join(x) if x else None)
df["artist_genres"] = df["artist_genres"].apply(lambda x: ",".join(x) if isinstance(x, list) else None)
df["popularity"]        = pd.to_numeric(df["popularity"],        errors="coerce")
df["artist_popularity"] = pd.to_numeric(df["artist_popularity"], errors="coerce")
df["artist_followers"]  = pd.to_numeric(df["artist_followers"],  errors="coerce")
# drop duplicate artist_id column from the merge (same as primary_artist_id)
df = df.drop(columns=["artist_id"])

print(df.dtypes)
print(df.head())

print(df.shape)
print(df.columns.tolist())
print(df.head())

'''LOAD'''

from sqlalchemy import create_engine

# --- load ---
DB_HOST     = os.getenv("POSTGRES_HOST")
DB_PORT     = os.getenv("POSTGRES_PORT")
DB_NAME     = os.getenv("POSTGRES_DB")
DB_USER     = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# drop duration_ms since we have duration_s
df = df.drop(columns=["duration_ms"])

# load to postgres — if table exists, append new rows
df.to_sql("plays", engine, if_exists="append", index=False)

print("loaded to postgres successfully")