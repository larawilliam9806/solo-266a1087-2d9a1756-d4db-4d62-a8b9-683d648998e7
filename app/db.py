"""SQLite metadata layer.

Four tables:

* ``files``         -- registry of every parquet file that belongs to a daily
                       partition.  ``state`` is ``active`` (visible to readers)
                       or ``retired`` (replaced by compaction, waiting for
                       in-flight readers to drain before the bytes are
                       unlinked).
* ``compactions``   -- one row per compaction job.  A partial unique index
                       guarantees at most one ``running`` job per day, which is
                       what lets us reject concurrent compactions for the same
                       day atomically (even across a process restart).
* ``snapshots``     -- one row per day snapshot.  ``released_at`` is NULL while
                       the snapshot is live; a released id is tombstoned and
                       serves no more data.
* ``snapshot_files`` -- the exact set of parquet paths pinned by a snapshot (plus
                       the per-file row count captured at creation time).
                       ``collect_garbage`` never unlinks a retired file while a
                       live snapshot still references it.

The compaction *switch* (retire inputs + register output + mark job done)
happens in a single SQLite transaction, so a crash can never leave the
registry half-switched.  Snapshot creation (snapshot row + pinned file set)
is likewise a single transaction, so a crash can never expose half a
snapshot.
"""

from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    day        TEXT NOT NULL,
    path       TEXT NOT NULL UNIQUE,
    kind       TEXT NOT NULL CHECK (kind IN ('upload', 'compacted')),
    state      TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    rows       INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_files_day_state ON files(day, state);

CREATE TABLE IF NOT EXISTS compactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    day         TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('running', 'done', 'failed')),
    input_paths TEXT NOT NULL,          -- JSON array, snapshot taken at job start
    input_rows  INTEGER NOT NULL,
    output_path TEXT,
    output_rows INTEGER,
    error       TEXT,
    created_at  TEXT NOT NULL,
    finished_at TEXT
);
-- At most one running compaction per day (enforced by the DB itself).
CREATE UNIQUE INDEX IF NOT EXISTS ux_compactions_one_running_per_day
    ON compactions(day) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS snapshots (
    id          TEXT PRIMARY KEY,
    day         TEXT NOT NULL,
    file_count  INTEGER NOT NULL,
    row_count   INTEGER NOT NULL,
    created_at  TEXT NOT NULL,
    released_at TEXT                    -- NULL = live, set = released
);
CREATE INDEX IF NOT EXISTS ix_snapshots_day ON snapshots(day);

-- The file set a snapshot pins.  Paths are copied (not joined through the
-- files registry) so later compaction/reclamation cannot change the set.
CREATE TABLE IF NOT EXISTS snapshot_files (
    snapshot_id TEXT NOT NULL,
    file_id     INTEGER NOT NULL,       -- files.id at creation, for stable order
    path        TEXT NOT NULL,
    rows        INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, path)
);
CREATE INDEX IF NOT EXISTS ix_snapshot_files_path ON snapshot_files(path);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """Open a short-lived connection with sane durability/concurrency pragmas."""
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # FULL so a committed upload / compaction switch survives a power cut.
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_schema(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
