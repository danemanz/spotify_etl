import os
import glob
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv(r"C:\Users\zacha\Documents\My Code\spotify-etl\.env")

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

FOLDER = r"C:\Users\zacha\Documents\My Code\spotify-etl\Spotify Extended Streaming History"

files = sorted(glob.glob(os.path.join(FOLDER, "Streaming_History_Audio_*.json")))

plays_records = []
skips_records = []

for path in files:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    for row in data:
        uri = row.get("spotify_track_uri")
        if not uri or not uri.startswith("spotify:track:"):
            continue
        track_id = uri.split(":")[-1]
        ms_played = row.get("ms_played")
        base = {
            "played_at":    row["ts"],
            "track_id":     track_id,
            "track_name":   row.get("master_metadata_track_name"),
            "artist_names": row.get("master_metadata_album_artist_name"),
            "album_name":   row.get("master_metadata_album_album_name"),
            "ms_played":    ms_played,
            "platform":     row.get("platform"),
            "conn_country": row.get("conn_country"),
        }
        if row.get("skipped"):
            skips_records.append({
                **base,
                "reason_start":   row.get("reason_start"),
                "reason_end":     row.get("reason_end"),
                "shuffle":        row.get("shuffle"),
                "offline":        row.get("offline"),
                "incognito_mode": row.get("incognito_mode"),
            })
        else:
            plays_records.append({
                **base,
                "release_date":      None,
                "artist_ids":        None,
                "primary_artist_id": None,
                "artist_name":       row.get("master_metadata_album_artist_name"),
                "artist_genres":     None,
                "duration_s":        row.get("ms_played", 0) / 1000.0,
            })

print(f"plays found: {len(plays_records)}, skips found: {len(skips_records)}")

def load_plays(records):
    if not records:
        print("no play records to load")
        return
    df = pd.DataFrame(records)
    df["played_at"] = pd.to_datetime(df["played_at"], utc=True)
    df = df.sort_values("played_at").drop_duplicates(subset=["played_at", "track_id"]).reset_index(drop=True)
    print(f"plays after dedup: {len(df)}")

    CHUNK = 10_000
    inserted = 0
    for i in range(0, len(df), CHUNK):
        chunk = df.iloc[i:i + CHUNK]
        with engine.connect() as conn:
            chunk.to_sql("plays_staging", conn, if_exists="replace", index=False)
            result = conn.execute(text("""
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
                    ms_played,
                    platform,
                    conn_country
                FROM plays_staging
                ON CONFLICT (played_at, track_id) DO NOTHING
            """))
            n = result.rowcount
            conn.execute(text("DROP TABLE plays_staging"))
            conn.commit()
            inserted += n
        print(f"  chunk {i // CHUNK + 1}: {n} inserted (total: {inserted})")
    print(f"done — {inserted} rows inserted into plays")

def load_skips(records):
    if not records:
        print("no skip records to load")
        return
    df = pd.DataFrame(records)
    df["played_at"] = pd.to_datetime(df["played_at"], utc=True)
    df = df.sort_values("played_at").drop_duplicates(subset=["played_at", "track_id"]).reset_index(drop=True)
    print(f"skips after dedup: {len(df)}")

    CHUNK = 10_000
    inserted = 0
    for i in range(0, len(df), CHUNK):
        chunk = df.iloc[i:i + CHUNK]
        with engine.connect() as conn:
            chunk.to_sql("past_skips_staging", conn, if_exists="replace", index=False)
            result = conn.execute(text("""
                INSERT INTO past_skips (
                    played_at, track_id, track_name, artist_names, album_name,
                    ms_played, platform, conn_country,
                    reason_start, reason_end, shuffle, offline, incognito_mode
                )
                SELECT
                    played_at, track_id, track_name, artist_names, album_name,
                    ms_played, platform, conn_country,
                    reason_start, reason_end, shuffle, offline, incognito_mode
                FROM past_skips_staging
                ON CONFLICT (played_at, track_id) DO NOTHING
            """))
            n = result.rowcount
            conn.execute(text("DROP TABLE past_skips_staging"))
            conn.commit()
            inserted += n
        print(f"  chunk {i // CHUNK + 1}: {n} inserted (total: {inserted})")
    print(f"done — {inserted} rows inserted into past_skips")

load_plays(plays_records)
load_skips(skips_records)
