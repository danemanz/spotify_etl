-- =============================================================
-- 2026-04-24  Rebuild plays table; create past_skips table
-- =============================================================

-- Drop and recreate plays with correct schema
DROP TABLE IF EXISTS plays;

CREATE TABLE plays (
    played_at         TIMESTAMPTZ NOT NULL,
    track_id          TEXT NOT NULL,
    track_name        TEXT,
    album_name        TEXT,
    release_date      TEXT,
    artist_ids        TEXT,
    artist_names      TEXT,
    primary_artist_id TEXT,
    artist_name       TEXT,
    artist_genres     TEXT,
    duration_s        DOUBLE PRECISION,
    ms_played         INTEGER,
    platform          TEXT,
    conn_country      TEXT,
    CONSTRAINT plays_unique UNIQUE (played_at, track_id)
);

-- Create past_skips table
CREATE TABLE IF NOT EXISTS past_skips (
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
    CONSTRAINT past_skips_unique UNIQUE (played_at, track_id)
);

-- =============================================================
-- 2026-04-24  Fix plays table: add artist_genres, drop stale columns
-- =============================================================

ALTER TABLE plays ADD COLUMN IF NOT EXISTS artist_genres TEXT;
ALTER TABLE plays DROP COLUMN IF EXISTS reason_start;
ALTER TABLE plays DROP COLUMN IF EXISTS reason_end;
ALTER TABLE plays DROP COLUMN IF EXISTS shuffle;
ALTER TABLE plays DROP COLUMN IF EXISTS skipped;
ALTER TABLE plays DROP COLUMN IF EXISTS offline;
ALTER TABLE plays DROP COLUMN IF EXISTS incognito_mode;
