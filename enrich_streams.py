import requests
import json

SC_APP_ID  = "ZDANEMAN1-API_4842F8D0"
SC_API_KEY = "a3ee22ad132a8790"
import os
import time
import requests
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

headers = {
    "x-app-id": SC_APP_ID,
    "x-api-key": SC_API_KEY
}

def get_soundcharts_uuid(spotify_track_id):
    r = requests.get(
        f"https://customer.api.soundcharts.com/api/v2/song/by-platform/spotify/{spotify_track_id}",
        headers=headers
    )
    remaining = r.headers.get("x-quota-remaining")
    print(f"quota remaining: {remaining}")
    if r.status_code == 200:
        return r.json()["object"]["uuid"]
    else:
        print(f"failed to get uuid for {spotify_track_id}: {r.status_code}")
        return None

def get_stream_count(uuid):
    r = requests.get(
        f"https://customer.api.soundcharts.com/api/v2/song/{uuid}/audience/spotify",
        headers=headers
    )
    remaining = r.headers.get("x-quota-remaining")
    print(f"quota remaining: {remaining}")
    if r.status_code == 200:
        items = r.json().get("items", [])
        if items and items[0].get("plots"):
            return items[0]["plots"][0]["value"]
    print(f"failed to get streams for {uuid}: {r.status_code}")
    return None

# --- get existing track ids already fetched ---
def get_existing_stream_track_ids():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT track_id FROM stream_counts"))
            return set(row[0] for row in result.fetchall())
    except Exception:
        return set()

# --- pull tracks from playlists with <= 30 songs ---
with engine.connect() as conn:
    result = conn.execute(text("""
        (SELECT DISTINCT track_id, track_name, artist_names
        FROM plays
        WHERE track_id IS NOT NULL
        ORDER BY track_id)
        
        union                    
                            
        (SELECT DISTINCT track_id, track_name, artist_names
        FROM playlist_tracks
        WHERE track_id IS NOT NULL
        AND playlist_id IN (
            SELECT playlist_id
            FROM playlist_tracks
            GROUP BY playlist_id
            HAVING COUNT(track_id) != 100
        )
        ORDER BY track_id)
    """))
    rows = result.fetchall()

track_ids = [(row[0], row[1], row[2]) for row in rows]
print(f"total unique tracks in qualifying playlists: {len(track_ids)}")

# filter out already fetched
existing = get_existing_stream_track_ids()
track_ids = [(tid, name, artist) for tid, name, artist in track_ids if tid not in existing]
print(f"new tracks to fetch: {len(track_ids)}")

# --- fetch and store ---
records = []
for i, (track_id, track_name, artist_names) in enumerate(track_ids):
    print(f"[{i+1}/{len(track_ids)}] fetching: {track_name} — {artist_names}")

    uuid = get_soundcharts_uuid(track_id)
    if not uuid:
        time.sleep(0.5)
        continue

    stream_count = get_stream_count(uuid)

    # write immediately after each fetch
    df = pd.DataFrame([{
        "track_id":         track_id,
        "track_name":       track_name,
        "artist_names":     artist_names,
        "soundcharts_uuid": uuid,
        "stream_count":     stream_count,
        "fetched_at":       pd.Timestamp.now(tz="UTC")
    }])
    df.to_sql("stream_counts", engine, if_exists="append", index=False)
    print(f"saved: {track_name} — {stream_count:,}" if stream_count else f"saved: {track_name} — no stream count found")

    time.sleep(0.3)