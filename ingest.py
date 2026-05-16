"""Pull HypurrScan delegation events + Hyperliquid validators and rebuild SQLite tables.

The /allDelegations response can be 100MB+; we stream-parse it with ijson so peak
memory stays ~constant regardless of how many events accumulate over time.
"""
from __future__ import annotations

import time
from collections import defaultdict

import httpx
import ijson

import db

HYPURRSCAN = "https://api.hypurrscan.io"
HYPERLIQUID = "https://api.hyperliquid.xyz"
TIMEOUT = 120.0
EVENT_BATCH = 10_000

INSERT_EVENT_SQL = (
    "INSERT OR REPLACE INTO events"
    "(hash,time_ms,user,validator,wei,is_undelegate,error,block) "
    "VALUES(?,?,?,?,?,?,?,?)"
)


class _IterReader:
    """Adapt a bytes iterator (e.g. httpx.iter_bytes()) to a read(n) file-like for ijson."""

    def __init__(self, byte_iterator):
        self._it = iter(byte_iterator)
        self._buf = bytearray()

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            for chunk in self._it:
                self._buf.extend(chunk)
            data = bytes(self._buf)
            self._buf.clear()
            return data
        while len(self._buf) < size:
            try:
                self._buf.extend(next(self._it))
            except StopIteration:
                break
        out = bytes(self._buf[:size])
        del self._buf[:size]
        return out


def fetch_unstaking_queue() -> list[dict]:
    r = httpx.get(f"{HYPURRSCAN}/unstakingQueue", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_validator_summaries() -> list[dict]:
    r = httpx.post(
        f"{HYPERLIQUID}/info",
        json={"type": "validatorSummaries"},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def _event_row(e: dict) -> tuple:
    action = e.get("action") or {}
    block = e.get("block")
    return (
        e["hash"],
        int(e["time"]),
        e["user"],
        action.get("validator", ""),
        int(action.get("wei", 0)),
        1 if action.get("isUndelegate") else 0,
        e.get("error"),
        int(block) if block is not None else None,
    )


def stream_and_insert_events(conn) -> int:
    """Stream /allDelegations into the events table. Returns row count."""
    conn.execute("DELETE FROM events")
    batch: list[tuple] = []
    n = 0
    with httpx.stream("GET", f"{HYPURRSCAN}/allDelegations", timeout=TIMEOUT) as r:
        r.raise_for_status()
        reader = _IterReader(r.iter_bytes())
        for e in ijson.items(reader, "item"):
            batch.append(_event_row(e))
            if len(batch) >= EVENT_BATCH:
                conn.executemany(INSERT_EVENT_SQL, batch)
                n += len(batch)
                batch.clear()
    if batch:
        conn.executemany(INSERT_EVENT_SQL, batch)
        n += len(batch)
    return n


def rebuild_stakers(conn):
    """Replay events into per-(user,validator) net stake, then aggregate per user."""
    pair_stake: dict[tuple[str, str], int] = defaultdict(int)
    user_last: dict[str, int] = defaultdict(int)

    cur = conn.execute(
        "SELECT user, validator, wei, is_undelegate, time_ms FROM events "
        "WHERE error IS NULL ORDER BY time_ms ASC"
    )
    for user, validator, wei, is_undel, t in cur:
        pair_stake[(user, validator)] += -wei if is_undel else wei
        if t > user_last[user]:
            user_last[user] = t

    conn.execute("DELETE FROM staker_validators")
    conn.execute("DELETE FROM stakers")

    sv_rows = []
    user_totals: dict[str, int] = defaultdict(int)
    user_nvals: dict[str, int] = defaultdict(int)
    for (user, validator), staked in pair_stake.items():
        if staked <= 0:
            continue
        sv_rows.append((user, validator, staked))
        user_totals[user] += staked
        user_nvals[user] += 1

    conn.executemany(
        "INSERT INTO staker_validators(user,validator,staked_wei) VALUES(?,?,?)",
        sv_rows,
    )
    staker_rows = [
        (u, user_totals[u], user_nvals[u], user_last[u])
        for u in user_totals
    ]
    conn.executemany(
        "INSERT INTO stakers(user,staked_wei,n_validators,last_action_ms) VALUES(?,?,?,?)",
        staker_rows,
    )


def rebuild_validators(conn, vs: list[dict]):
    conn.execute("DELETE FROM validators")
    rows = []
    for v in vs:
        stats = dict(v.get("stats") or [])
        day = stats.get("day") or {}
        rows.append(
            (
                v["validator"],
                v.get("signer"),
                v.get("name"),
                v.get("description"),
                int(v.get("stake", 0)),
                v.get("commission"),
                1 if v.get("isActive") else 0,
                1 if v.get("isJailed") else 0,
                day.get("predictedApr"),
            )
        )
    conn.executemany(
        "INSERT INTO validators(validator,signer,name,description,stake_wei,commission,is_active,is_jailed,apr) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        rows,
    )


def rebuild_unstaking(conn, queue: list[dict]):
    conn.execute("DELETE FROM unstaking_queue")
    rows = [(q["user"], q["time"], int(q["wei"])) for q in queue]
    conn.executemany(
        "INSERT OR REPLACE INTO unstaking_queue(user,time_ms,wei) VALUES(?,?,?)",
        rows,
    )


def run() -> dict:
    db.init()
    started = time.time()

    print("Fetching /unstakingQueue …")
    queue = fetch_unstaking_queue()
    print(f"  {len(queue):,} queued unstakes")
    print("Fetching validatorSummaries …")
    validators = fetch_validator_summaries()
    print(f"  {len(validators):,} validators")

    conn = db.connect()
    try:
        conn.execute("BEGIN")
        print("Streaming /allDelegations …")
        n_events = stream_and_insert_events(conn)
        print(f"  {n_events:,} events")
        rebuild_stakers(conn)
        rebuild_unstaking(conn, queue)
        rebuild_validators(conn, validators)
        db.set_meta(conn, "last_refresh_ms", str(int(time.time() * 1000)))
        db.set_meta(conn, "n_events", str(n_events))
        conn.commit()
    finally:
        conn.close()

    elapsed = time.time() - started
    print(f"Done in {elapsed:.1f}s")
    return {
        "events": n_events,
        "validators": len(validators),
        "unstaking_queue": len(queue),
        "seconds": elapsed,
    }


if __name__ == "__main__":
    run()
