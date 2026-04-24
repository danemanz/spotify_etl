import os
import time
import requests
import base64
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv(r"C:\Users\zacha\Documents\My Code\spotify-etl\.env")

CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

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

def get_artist_data(access_token, artist_ids):
    unique_ids = list(set(artist_ids))
    artists = []
    for artist_id in unique_ids:
        r = requests.get(f"https://api.spotify.com/v1/artists/{artist_id}",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if r.status_code == 200:
            artists.append(r.json())
        elif r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 10))
            print(f"rate limited, waiting {retry_after}s...")
            time.sleep(retry_after)
            r = requests.get(f"https://api.spotify.com/v1/artists/{artist_id}",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            if r.status_code == 200:
                artists.append(r.json())
        else:
            print(f"failed for {artist_id}: {r.status_code}")
        time.sleep(0.5)
    return artists

def get_recently_played(access_token, after_ms=None):
    params = {"limit": 50}
    if after_ms:
        params["after"] = after_ms
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params
    )
    return r.json()

def get_last_played_at():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(played_at) FROM plays"))
            val = result.fetchone()[0]
            if val:
                return int(val.timestamp() * 1000)
    except Exception:
        pass
    return None

'''
RUN
'''
token = get_access_token()

after_ms = get_last_played_at()
if after_ms:
    print(f"fetching plays after last stored play...")
else:
    print("no existing plays found, fetching last 50...")

data = get_recently_played(token, after_ms)
items = data.get("items", [])
print(f"new plays found: {len(items)}")

if not items:
    print("no new plays to add, exiting")
    exit()

'''
TRANSFORM
'''
records = []
for item in items:
    track   = item["track"]
    artists = track.get("artists", [])
    records.append({
        "played_at":         item["played_at"],
        "track_id":          track.get("id"),
        "track_name":        track.get("name"),
        "album_name":        track.get("album", {}).get("name"),
        "release_date":      track.get("album", {}).get("release_date"),
        "artist_ids":        ",".join([a["id"] for a in artists]),
        "artist_names":      ",".join([a["name"] for a in artists]),
        "primary_artist_id": artists[0]["id"] if artists else None,
        "artist_name":       artists[0]["name"] if artists else None,
        "duration_s":        round(track.get("duration_ms", 0) / 1000, 1),
        "ms_played":         None,
        "platform":          None,
        "conn_country":      None,
    })

df = pd.DataFrame(records)
df["played_at"] = pd.to_datetime(df["played_at"], utc=True)

# Fetch genres for all primary artists
primary_ids = [r["primary_artist_id"] for r in records if r["primary_artist_id"]]
artist_genre_map = {}
if primary_ids:
    artist_data = get_artist_data(token, primary_ids)
    for a in artist_data:
        artist_genre_map[a["id"]] = ",".join(a.get("genres", []))

df["artist_genres"] = df["primary_artist_id"].map(artist_genre_map)

'''
LOAD
'''
with engine.connect() as conn:
    df.to_sql("plays_staging", conn, if_exists="replace", index=False)
    conn.execute(text("""
        INSERT INTO plays (
            played_at,
            track_id,
            track_name,
            album_name,
            release_date,
            artist_ids,
            artist_names,
            primary_artist_id,
            artist_name,
            artist_genres,
            duration_s,
            ms_played,
            platform,
            conn_country
        )
        SELECT
            played_at,
            track_id,
            track_name,
            album_name,
            release_date,
            artist_ids,
            artist_names,
            primary_artist_id,
            artist_name,
            artist_genres,
            duration_s,
            ms_played::integer,
            platform,
            conn_country
        FROM plays_staging
        ON CONFLICT (played_at, track_id) DO NOTHING
    """))
    conn.execute(text("DROP TABLE plays_staging"))
    conn.commit()

print(f"done — {len(df)} plays processed")
