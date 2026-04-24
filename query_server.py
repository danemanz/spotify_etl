import os
import re
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\zacha\Documents\My Code\spotify-etl\.env")

POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT")
POSTGRES_DB       = os.getenv("POSTGRES_DB")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not ANTHROPIC_API_KEY:
    raise RuntimeError("ANTHROPIC_API_KEY not loaded — check .env file")
print(f"[startup] Anthropic key loaded: {ANTHROPIC_API_KEY[:8]}...{ANTHROPIC_API_KEY[-4:]}")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
engine = create_engine(DATABASE_URL)

app = Flask(__name__)
CORS(app)

SYSTEM_PROMPT = """You are a SQL expert. Generate a single PostgreSQL SELECT query based \
on the user's question. Return ONLY the raw SQL with no explanation, \
no markdown, no backticks, no preamble. The query must be a SELECT \
statement only — never INSERT, UPDATE, DELETE or DROP.

Database schema:

TABLE plays (
    played_at         TIMESTAMPTZ,
    track_id          TEXT,        -- always populated
    track_name        TEXT,        -- always populated
    album_name        TEXT,
    release_date      TEXT,
    artist_ids        TEXT,        -- NULL for historical plays; use artist_name instead
    artist_names      TEXT,        -- NULL for historical plays; use artist_name instead
    primary_artist_id TEXT,        -- NULL for historical plays; use artist_name instead
    artist_name       TEXT,        -- always populated; use this to identify unique artists
    artist_genres     TEXT,
    duration_s        DOUBLE PRECISION,
    ms_played         INTEGER,
    platform          TEXT,
    conn_country      TEXT
)

-- IMPORTANT data-quality rules for plays:
-- 1. primary_artist_id / artist_ids / artist_names are NULL for most historical plays.
--    Always use artist_name to identify or count unique artists.
-- 2. To identify a unique track, use (track_name, artist_name) — do NOT rely on track_id alone
--    since different entries of the same track may share a name but differ in ID, and vice versa.
-- 3. ms_played is the actual milliseconds listened (from streaming history).
--    duration_s is the full track duration in seconds.

-- CONSECUTIVE / STREAK DETECTION PATTERN:
-- To find songs played consecutively (back-to-back with no other song between them), use this
-- exact CTE chain:
--   WITH ordered AS (
--       SELECT track_name, artist_name, played_at,
--              CASE WHEN track_name  = LAG(track_name)  OVER (ORDER BY played_at)
--                    AND artist_name = LAG(artist_name) OVER (ORDER BY played_at)
--                   THEN 0 ELSE 1 END AS is_new_run
--       FROM plays
--   ),
--   runs AS (
--       SELECT track_name, artist_name,
--              SUM(is_new_run) OVER (ORDER BY played_at) AS run_id
--       FROM ordered
--   ),
--   run_lengths AS (
--       SELECT track_name, artist_name, run_id, COUNT(*) AS streak
--       FROM runs GROUP BY track_name, artist_name, run_id
--   )
--   SELECT track_name, artist_name, MAX(streak) AS max_consecutive_plays
--   FROM run_lengths
--   GROUP BY track_name, artist_name
--   ORDER BY max_consecutive_plays DESC
--   LIMIT 5;

TABLE playlist_tracks (
    playlist_id       TEXT,
    playlist_name     TEXT,
    added_at          TIMESTAMPTZ,
    track_id          TEXT,
    track_name        TEXT,
    duration_s        DOUBLE PRECISION,
    album_name        TEXT,
    release_date      TEXT,
    artist_ids        TEXT,
    artist_names      TEXT,
    primary_artist_id TEXT,
    artist_name       TEXT,
    artist_genres     TEXT
)

TABLE stream_counts (
    track_id          TEXT,
    track_name        TEXT,
    artist_names      TEXT,
    soundcharts_uuid  TEXT,
    stream_count      BIGINT,
    fetched_at        TIMESTAMPTZ
)

TABLE past_skips (
    played_at         TIMESTAMPTZ,
    track_id          TEXT,
    track_name        TEXT,
    artist_names      TEXT,
    album_name        TEXT,
    ms_played         INTEGER,
    platform          TEXT,
    conn_country      TEXT,
    reason_start      TEXT,
    reason_end        TEXT,
    shuffle           BOOLEAN,
    offline           BOOLEAN,
    incognito_mode    BOOLEAN
)

TABLE playlist_snapshots (
    playlist_id       TEXT,
    playlist_name     TEXT,
    snapshot_id       TEXT,
    last_updated      TIMESTAMPTZ
)"""

FORBIDDEN_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE",
    "REPLACE", "UPSERT", "MERGE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
    "CALL", "COPY", "VACUUM", "ANALYZE", "EXPLAIN", "SHOW", "SET", "RESET",
    "INTO", "pg_sleep", "pg_read_file", "pg_write_file", "lo_export",
    "lo_import", "LOAD", "IMPORT",
]


def is_safe_sql(sql: str) -> tuple[bool, str]:
    upper = sql.upper()
    # Strip string literals to avoid false positives on song titles
    stripped = re.sub(r"'[^']*'", "''", upper)
    for keyword in FORBIDDEN_KEYWORDS:
        pattern = r"\b" + re.escape(keyword.upper()) + r"\b"
        if re.search(pattern, stripped):
            return False, f"Query contains forbidden keyword: {keyword}"
    return True, ""


@app.route("/query", methods=["POST"])
def query():
    data = request.get_json(force=True)
    question = (data or {}).get("question", "").strip()
    if not question:
        return jsonify({"error": "No question provided"}), 400

    # Generate SQL via Anthropic API
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 500,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": question}],
            },
            timeout=30,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        return jsonify({"error": f"Anthropic API error: {e}"}), 400

    sql = resp.json()["content"][0]["text"].strip().rstrip(";")

    if not re.match(r"^\s*(SELECT|WITH)\b", sql, re.IGNORECASE):
        return jsonify({"error": "Only SELECT queries are allowed"}), 400

    safe, reason = is_safe_sql(sql)
    if not safe:
        return jsonify({"error": reason}), 400

    wrapped = f"SELECT * FROM ({sql}) q LIMIT 500"

    try:
        with engine.connect() as conn:
            result = conn.execute(text(wrapped))
            columns = list(result.keys())
            rows = [list(row) for row in result.fetchall()]
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    return jsonify({"sql": sql, "columns": columns, "rows": rows})


if __name__ == "__main__":
    app.run(port=5001, debug=True)
