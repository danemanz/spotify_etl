# Spotify ETL — Claude Project Notes

## Project overview

Personal Spotify ETL pipeline. Extracts listening history and playlist data from the
Spotify API, enriches it with stream counts via Soundcharts, and stores everything in
a local PostgreSQL database for analysis. Supports natural language querying via a
Flask + Anthropic API server.

---

## Critical Spotify API context

**Authentication:**
- OAuth2 with PKCE flow. Access token expires after 60 minutes.
- Refresh token is stored in `.env` as `SPOTIFY_REFRESH_TOKEN`.
- Always refresh the token at script start and every 500 requests in long loops.

**API limitations:**
- `GET /v1/me/player/recently-played` returns at most 50 tracks per call.
- Playlist tracks endpoint is paginated — must follow `next` links.
- No endpoint exposes full historical plays beyond the 50-track recent history;
  extended history must come from the Spotify data export JSON files.

**Extreme rate limiting behaviour:**
- Spotify imposes multi-hour bans (38,000–40,000 seconds) if requests are fired in
  rapid succession with no delay.
- Always use minimum 200 ms sleep between single track/artist calls.
- Always use minimum 500 ms sleep if making many calls in sequence.
- On 429 response always read the `Retry-After` header.
- If `Retry-After` > 300 seconds, stop the script immediately — do not sleep for
  hours; save progress and exit.
- Test with `GET /v1/me` before starting any long-running loop to verify you are not
  already rate limited.
- `genres` field on the artist object is deprecated and returns empty for new apps —
  do not attempt to fetch or store genres.
- Single track endpoint `GET /v1/tracks/{id}` works but is also subject to extreme
  rate limiting — treat it the same as artist calls.

---

## File structure

```
spotify-etl/
├── extract_plays.py         — pulls recent plays from GET /v1/me/player/recently-played,
│                              upserts into plays table. Runs every 30 min via Task Scheduler.
├── extract_playlists.py     — pulls all saved playlists and their tracks, upserts into
│                              playlists and playlist_tracks tables.
├── enrich_streams.py        — fetches Soundcharts stream counts for tracks missing them,
│                              upserts into stream_counts table.
├── enrich_plays.py          — backfills primary_artist_id and artist_ids for history rows
│                              using GET /v1/tracks/{id}, 200 ms delay, circuit breaker on
│                              Retry-After > 300 s, refreshes token every 500 requests.
├── load_history.py          — loads Spotify extended streaming history JSON files from the
│                              "Spotify Extended Streaming History" folder into plays and
│                              past_skips tables.
├── query_server.py          — Flask server on port 5001, receives plain-English questions,
│                              calls Anthropic API to generate SQL (claude-haiku-4-5-20251001),
│                              executes against Postgres, returns results. Has SQL safety checker
│                              blocking INSERT, UPDATE, DELETE, DROP and other dangerous keywords.
├── query_ui.html            — dark-theme browser UI for natural language queries against the
│                              database. Connects to query_server.py at localhost:5001.
├── start_query_server.bat   — batch file to start query_server.py.
├── migrations.sql           — schema migrations run manually in psql.
├── .env                     — credentials (never commit).
└── CLAUDE.md                — this file.
```

---

## Database schema

### TABLE plays
```sql
TABLE plays (
    played_at         TIMESTAMPTZ NOT NULL,
    track_id          TEXT NOT NULL,
    track_name        TEXT,
    album_name        TEXT,
    release_date      TEXT,         -- API only, null for history rows
    artist_ids        TEXT,         -- comma separated, null until enriched
    artist_names      TEXT,         -- comma separated all artists
    primary_artist_id TEXT,         -- null until enriched for history rows
    artist_name       TEXT,         -- primary artist name
    artist_genres     TEXT,         -- deprecated, always null
    duration_s        DOUBLE PRECISION,
    ms_played         INTEGER,      -- history only, null for API rows
    platform          TEXT,
    conn_country      TEXT,
    UNIQUE (played_at, track_id)
)
```

### TABLE past_skips
```sql
TABLE past_skips (
    played_at       TIMESTAMPTZ NOT NULL,
    track_id        TEXT,
    track_name      TEXT,
    artist_names    TEXT,
    album_name      TEXT,
    ms_played       INTEGER,
    platform        TEXT,
    conn_country    TEXT,
    reason_start    TEXT,
    reason_end      TEXT,
    shuffle         BOOLEAN,
    offline         BOOLEAN,
    incognito_mode  BOOLEAN,
    UNIQUE (played_at, track_id)
)
```

### TABLE stream_counts
Stores global Soundcharts stream counts keyed by track_id. One row per track.
Fetched once per track regardless of how many users have played it.

### TABLE playlists / playlist_tracks
Stores saved playlists and their track membership. Populated by extract_playlists.py.

---

## Architecture decisions

**Why plays and enrich_plays are separate:**
extract_plays.py runs every 30 minutes and must complete in seconds — no artist API
calls. enrich_plays.py runs once daily and backfills artist IDs for any rows missing
them. This prevents the 30-minute capture window from being blocked by slow API calls.

**Why past_skips is a separate table:**
Skipped tracks are filtered out of plays entirely. past_skips stores the full skip
history from the JSON import for separate analysis. Skipped rows are identified by
`skipped=True` in the JSON history files.

**Why stream counts are in a separate table:**
stream_counts is a global reference table — stream counts are the same regardless of
which user listened. Keeping it separate means it only needs to be fetched once per
track globally rather than per user.

**Natural language query tool:**
query_server.py + query_ui.html form a local NL query tool. The server handles both
the Anthropic API call and Postgres execution server-side so the API key never touches
the browser. Uses claude-haiku-4-5-20251001 for SQL generation (~$0.001 per query).
Has a SQL safety checker that blocks dangerous keywords using regex word boundaries
after stripping string literals.

**Soundcharts free tier strategy:**
1,000 requests = 500 tracks (2 requests per track). Create a new account every few
days to stay on the free tier. Credentials live in `.env` as `SOUNDCHARTS_APP_ID`
and `SOUNDCHARTS_API_KEY`.

---

## Next steps

- enrich_plays.py backfill — waiting out Spotify rate limit (~11 hours), then run
  overnight at 200 ms delay.
- Fix Windows Task Scheduler — currently hangs on all Python scripts even simple ones.
  Tried: full Python path, disabling Microsoft Store alias, cmd.exe wrapper, batch
  file. All hang. Scripts run fine manually in VS Code.
- Build nicheness score calculator using log-transformed stream_counts data (power law
  distribution).
- Add user_id column to plays, playlist_tracks, stream_counts for multi-user support
  (max 5 users, development mode limit).
- Build friend comparison dashboard.
- Download Spotify extended history for friends once multi-user is set up.
