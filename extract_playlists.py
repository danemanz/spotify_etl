import os
import requests
import base64
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv(r"C:\Users\zacha\Documents\My Code\spotify-etl\.env")
# Setup
CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

def get_access_token():
    creds = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post("https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {creds}"},
        data={"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN}
    )
    return r.json()["access_token"]

def get_user_id(access_token):
    r = requests.get("https://api.spotify.com/v1/me",
        headers={"Authorization": f"Bearer {access_token}"}
    )
    return r.json().get("id")

def get_playlists(access_token):
    r = requests.get("https://api.spotify.com/v1/me/playlists",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"limit": 50}
    )
    return r.json().get("items", [])

def get_playlist_tracks(access_token, playlist_id):
    tracks = []
    # Try the /items endpoint instead of /tracks if the previous one failed
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/items"
    
    while url:
        print(f"  > Debug: Requesting {url}") # Add this to see the actual URL
        r = requests.get(url,
            headers={"Authorization": f"Bearer {access_token}"},
            params={"limit": 100}
        )
        
        # Add this to see what the API is actually sending back
        if r.status_code != 200:
            print(f"  > API Error: {r.status_code} - {r.text}")
            break
            
        data = r.json()
        new_items = data.get("items", [])
        print(f"  > Debug: Found {len(new_items)} items in this page")
        
        tracks.extend(new_items)
        url = data.get("next")
        
    return tracks

def get_stored_snapshots():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT playlist_id, snapshot_id FROM playlist_snapshots"))
            return {row[0]: row[1] for row in result.fetchall()}
    except Exception:
        return {}

# --- MAIN EXECUTION ---
token   = get_access_token()
user_id = get_user_id(token)
print(f"Logged in as User: {user_id}")

playlists_raw = get_playlists(token)
stored_snapshots = get_stored_snapshots()

for pl in playlists_raw:
    playlist_name = pl.get("name")
    playlist_id   = pl.get("id")
    owner_id      = pl.get("owner", {}).get("id")
    snapshot_id   = pl.get("snapshot_id")

    print(f"\n--- Checking: {playlist_name} ---")

    if owner_id != user_id:
        print("  > Skipping: Not the owner.")
        continue

    if stored_snapshots.get(playlist_id) == snapshot_id:
        print("  > Skipping: No changes found.")
        continue

    print("  > CHANGE DETECTED: Fetching tracks...")
    pl_tracks = get_playlist_tracks(token, playlist_id)
    
    records = []
    for item in pl_tracks:
        track = item.get("item")
        if not track or not track.get("id"): continue
        
        artists = track.get("artists", [])
        records.append({
            "playlist_id":   playlist_id,
            "playlist_name": playlist_name,
            "added_at":      item.get("added_at"),
            "track_id":      track.get("id"),
            "track_name":    track.get("name"),
            "duration_s":    round(track.get("duration_ms", 0) / 1000, 1),
            "album_name":    track.get("album", {}).get("name"),
            "release_date":  track.get("album", {}).get("release_date"),
            "artist_ids":    ",".join([a["id"] for a in artists]),
            "artist_names":  ",".join([a["name"] for a in artists]),
            "primary_artist_id": artists[0]["id"] if artists else None,
        })

    if not records:
        print("  > No tracks to save.")
        continue

    df = pd.DataFrame(records)
    df["added_at"] = pd.to_datetime(df["added_at"], utc=True, errors="coerce")

    try:
        # Transactions handle the "all or nothing" logic
        with engine.begin() as conn:
            # 1. Clear old data
            conn.execute(text("DELETE FROM playlist_tracks WHERE playlist_id = :pid"), {"pid": playlist_id})
            # 2. Insert new data
            df.to_sql("playlist_tracks", conn, if_exists="append", index=False)
            # 3. Update snapshot ONLY if the above worked
            conn.execute(text("""
                INSERT INTO playlist_snapshots (playlist_id, playlist_name, snapshot_id, last_updated)
                VALUES (:pid, :name, :sid, NOW())
                ON CONFLICT (playlist_id) DO UPDATE SET snapshot_id = :sid, last_updated = NOW()
            """), {"pid": playlist_id, "name": playlist_name, "sid": snapshot_id})
        
        print(f"  > SUCCESS: Synced {len(df)} tracks.")

    except Exception as e:
        print(f"  > DATABASE ERROR: {e}")

print("\nDone.")