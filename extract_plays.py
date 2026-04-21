import os
import time
import requests
import base64
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

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
            result = conn.execute(text(
                "SELECT MAX(played_at) FROM plays"
            ))
            val = result.fetchone()[0]
            if val:
                # convert to unix milliseconds for the Spotify cursor
                return int(val.timestamp() * 1000)
    except Exception:
        pass
    return None

'''
RUN
'''
token = get_access_token()

# get cursor — only fetch plays after the last one we stored
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
        "duration_ms":       track.get("duration_ms"),
        "album_name":        track.get("album", {}).get("name"),
        "release_date":      track.get("album", {}).get("release_date"),
        "artist_ids":        [a["id"] for a in artists],
        "artist_names":      [a["name"] for a in artists],
        "primary_artist_id": artists[0]["id"] if artists else None,
    })

df = pd.DataFrame(records)

# fetch artist data
all_artist_ids = list(set(aid for ids in df["artist_ids"] for aid in ids))
print(f"fetching {len(all_artist_ids)} artists...")
artists_raw = get_artist_data(token, all_artist_ids)

artists_df = pd.DataFrame([{
    "artist_id":   a.get("id"),
    "artist_name": a.get("name"),
    "artist_genres": ",".join(a.get("genres", [])),
} for a in artists_raw if a])

# join artist data
df = df.merge(artists_df, left_on="primary_artist_id",
                          right_on="artist_id", how="left")

# fix types
df["played_at"]    = pd.to_datetime(df["played_at"], utc=True)
df["duration_s"]   = (df["duration_ms"] / 1000).round(1)
df["artist_ids"]   = df["artist_ids"].apply(lambda x: ",".join(x) if x else None)
df["artist_names"] = df["artist_names"].apply(lambda x: ",".join(x) if x else None)
df = df.drop(columns=["duration_ms", "artist_id"], errors="ignore")

'''
LOAD — ignore duplicates via on_conflict_do_nothing
'''
from sqlalchemy.dialects.postgresql import insert

records_to_insert = df.to_dict(orient="records")
with engine.connect() as conn:
    for record in records_to_insert:
        stmt = insert(text("plays")).values(**record)
        # if played_at + track_id already exists, skip it
        conn.execute(
            text("""
                INSERT INTO plays ({cols})
                VALUES ({vals})
                ON CONFLICT (played_at, track_id) DO NOTHING
            """.format(
                cols=", ".join(record.keys()),
                vals=", ".join([f":{k}" for k in record.keys()])
            )),
            record
        )
    conn.commit()

print(f"done — {len(records_to_insert)} plays processed")