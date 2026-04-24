import os
import time
import base64
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\zacha\Documents\My Code\spotify-etl\.env")

SPOTIFY_CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)


def get_access_token():
    credentials = base64.b64encode(
        f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode()
    ).decode()
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {credentials}"},
        data={"grant_type": "refresh_token", "refresh_token": SPOTIFY_REFRESH_TOKEN},
    )
    r.raise_for_status()
    return r.json()["access_token"]


def lookup_track(track_id, token):
    """Return (primary_artist_id, artist_ids, artist_name, artist_names_from_api) or None on skip."""
    r = requests.get(
        f"https://api.spotify.com/v1/tracks/{track_id}",
        headers={"Authorization": f"Bearer {token}"},
    )

    if r.status_code == 200:
        artists = r.json().get("artists", [])
        primary_artist_id      = artists[0]["id"]   if artists else None
        artist_ids             = ",".join(a["id"]   for a in artists)
        artist_name            = artists[0]["name"] if artists else None
        artist_names_from_api  = ",".join(a["name"] for a in artists)
        return primary_artist_id, artist_ids, artist_name, artist_names_from_api

    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", 10))
        print(f"\nrate limited, waiting {wait}s...")
        time.sleep(wait)
        # one retry
        r2 = requests.get(
            f"https://api.spotify.com/v1/tracks/{track_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        if r2.status_code == 200:
            artists = r2.json().get("artists", [])
            primary_artist_id     = artists[0]["id"]   if artists else None
            artist_ids            = ",".join(a["id"]   for a in artists)
            artist_name           = artists[0]["name"] if artists else None
            artist_names_from_api = ",".join(a["name"] for a in artists)
            return primary_artist_id, artist_ids, artist_name, artist_names_from_api
        print(f"still rate limited after retry, skipping {track_id}")
        return None

    if r.status_code == 403:
        print(f"\n403 for {track_id}")
        return None

    print(f"\nunexpected status {r.status_code} for {track_id}")
    return None


def fmt_duration(seconds):
    return str(timedelta(seconds=int(seconds)))


def main():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT track_id
            FROM plays
            WHERE primary_artist_id IS NULL
              AND track_id IS NOT NULL
        """)).fetchall()

    track_ids = [row[0] for row in rows]
    total = len(track_ids)
    print(f"tracks to enrich: {total}")

    if total == 0:
        print("nothing to do")
        return

    token = get_access_token()
    done = 0
    skipped = 0
    start = time.monotonic()

    try:
        for i, track_id in enumerate(track_ids):
            # refresh token every 500 requests
            if i > 0 and i % 500 == 0:
                token = get_access_token()

            result = lookup_track(track_id, token)
            time.sleep(0.2)

            if result is None:
                skipped += 1
                elapsed = time.monotonic() - start
                print(
                    f"\r[{done + skipped}/{total}] {track_id} — SKIPPED | "
                    f"elapsed: {fmt_duration(elapsed)}",
                    end="", flush=True,
                )
                continue

            primary_artist_id, artist_ids, artist_name, artist_names_from_api = result

            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE plays
                    SET primary_artist_id = :primary_artist_id,
                        artist_ids        = :artist_ids,
                        artist_name       = :artist_name,
                        artist_names      = :artist_names_from_api
                    WHERE track_id = :track_id
                      AND primary_artist_id IS NULL
                """), {
                    "primary_artist_id":    primary_artist_id,
                    "artist_ids":           artist_ids,
                    "artist_name":          artist_name,
                    "artist_names_from_api": artist_names_from_api,
                    "track_id":             track_id,
                })

            done += 1
            completed = done + skipped
            elapsed = time.monotonic() - start
            avg = elapsed / completed
            eta = avg * (total - completed)
            print(
                f"\r[{completed}/{total}] {track_id} — {artist_name} | "
                f"elapsed: {fmt_duration(elapsed)} | eta: {fmt_duration(eta)}",
                end="", flush=True,
            )

    except KeyboardInterrupt:
        pass

    print()
    elapsed = time.monotonic() - start
    print(f"done — {done} tracks enriched, {skipped} skipped, total time: {fmt_duration(elapsed)}")


if __name__ == "__main__":
    main()
