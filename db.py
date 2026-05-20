import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "staking.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    hash         TEXT PRIMARY KEY,
    time_ms      INTEGER NOT NULL,
    user         TEXT NOT NULL,
    validator    TEXT NOT NULL,
    wei          INTEGER NOT NULL,
    is_undelegate INTEGER NOT NULL,
    error        TEXT,
    block        INTEGER
);
CREATE INDEX IF NOT EXISTS idx_events_user ON events(user);
CREATE INDEX IF NOT EXISTS idx_events_validator ON events(validator);

CREATE TABLE IF NOT EXISTS stakers (
    user         TEXT PRIMARY KEY,
    staked_wei   INTEGER NOT NULL,
    n_validators INTEGER NOT NULL,
    last_action_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stakers_staked ON stakers(staked_wei DESC);

CREATE TABLE IF NOT EXISTS staker_validators (
    user       TEXT NOT NULL,
    validator  TEXT NOT NULL,
    staked_wei INTEGER NOT NULL,
    PRIMARY KEY (user, validator)
);
CREATE INDEX IF NOT EXISTS idx_sv_validator ON staker_validators(validator);

CREATE TABLE IF NOT EXISTS validators (
    validator    TEXT PRIMARY KEY,
    signer       TEXT,
    name         TEXT,
    description  TEXT,
    stake_wei    INTEGER NOT NULL,
    commission   TEXT,
    is_active    INTEGER NOT NULL,
    is_jailed    INTEGER NOT NULL,
    apr          TEXT
);

CREATE TABLE IF NOT EXISTS unstaking_queue (
    user       TEXT NOT NULL,
    time_ms    INTEGER NOT NULL,
    wei        INTEGER NOT NULL,
    PRIMARY KEY (user, time_ms, wei)
);

-- Per-period snapshots of staker counts, reconstructed by replaying events.
-- threshold_hype = 0 is the total (>0); other rows count stakers with
-- net stake >= threshold_hype HYPE. total_staked_wei is the same for every
-- threshold in a period (it's a per-period figure, duplicated for easy querying).
CREATE TABLE IF NOT EXISTS staker_history (
    period_start_ms  INTEGER NOT NULL,
    threshold_hype   INTEGER NOT NULL,
    n_stakers        INTEGER NOT NULL,
    total_staked_wei INTEGER NOT NULL,
    PRIMARY KEY (period_start_ms, threshold_hype)
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init():
    conn = connect()
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()


def set_meta(conn: sqlite3.Connection, key: str, value: str):
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None
