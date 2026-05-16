"""FastAPI backend for the HYPE staking dashboard."""
from __future__ import annotations

import asyncio
import csv
import io
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

import db
import ingest

WEI_PER_HYPE = 100_000_000  # HYPE has 8 decimals on the staking layer
BUCKETS = [
    (0, 1, "<1"),
    (1, 10, "1–10"),
    (10, 100, "10–100"),
    (100, 1_000, "100–1k"),
    (1_000, 10_000, "1k–10k"),
    (10_000, 100_000, "10k–100k"),
    (100_000, 1_000_000, "100k–1M"),
    (1_000_000, None, "≥1M"),
]

REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")
AUTO_REFRESH = os.environ.get("AUTO_REFRESH", "0") == "1"
REFRESH_INTERVAL_S = int(os.environ.get("REFRESH_INTERVAL_S", "3600"))

_ingest_lock = asyncio.Lock()


async def _run_ingest(label: str):
    async with _ingest_lock:
        print(f"[{label}] ingest starting")
        result = await asyncio.to_thread(ingest.run)
        print(f"[{label}] ingest done: {result}")
        return result


async def _periodic_refresh():
    while True:
        conn = db.connect()
        last = db.get_meta(conn, "last_refresh_ms")
        conn.close()
        if last:
            elapsed = (time.time() * 1000 - int(last)) / 1000
            wait = max(60.0, REFRESH_INTERVAL_S - elapsed)
        else:
            wait = float(REFRESH_INTERVAL_S)
        await asyncio.sleep(wait)
        try:
            await _run_ingest("scheduled")
        except Exception as e:
            print(f"[scheduled] ingest failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init()
    conn = db.connect()
    has_data = db.get_meta(conn, "last_refresh_ms") is not None
    conn.close()

    bg_tasks: list[asyncio.Task] = []
    if not has_data:
        print("Empty DB at startup — running initial ingest in background")
        async def initial():
            try:
                await _run_ingest("initial")
            except Exception as e:
                print(f"[initial] ingest failed: {e}")
        bg_tasks.append(asyncio.create_task(initial()))
    if AUTO_REFRESH:
        bg_tasks.append(asyncio.create_task(_periodic_refresh()))
    try:
        yield
    finally:
        for t in bg_tasks:
            t.cancel()


app = FastAPI(title="HYPE Staking Dashboard", lifespan=lifespan)
STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health():
    conn = db.connect()
    last = db.get_meta(conn, "last_refresh_ms")
    conn.close()
    body = {
        "ok": last is not None,
        "data_ready": last is not None,
        "last_refresh_ms": int(last) if last else None,
        "auto_refresh": AUTO_REFRESH,
        "interval_s": REFRESH_INTERVAL_S,
    }
    if last is None:
        return JSONResponse(body, status_code=503)
    return body


@app.post("/api/refresh")
async def refresh(authorization: str | None = Header(None)):
    if REFRESH_TOKEN:
        expected = f"Bearer {REFRESH_TOKEN}"
        if authorization != expected:
            raise HTTPException(status_code=401, detail="Admin token required")
    if _ingest_lock.locked():
        raise HTTPException(status_code=409, detail="Refresh already in progress")
    try:
        return await _run_ingest("manual")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Refresh failed: {e}") from e


@app.get("/api/stats")
def stats():
    conn = db.connect()
    try:
        last_refresh = db.get_meta(conn, "last_refresh_ms")
        if last_refresh is None:
            return JSONResponse(
                {"empty": True, "message": "No data yet — hit Refresh."},
                status_code=200,
            )

        totals = conn.execute(
            "SELECT COUNT(*) AS n_stakers, "
            "       COALESCE(SUM(staked_wei),0) AS total_wei "
            "FROM stakers"
        ).fetchone()

        n_events = conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"]
        n_failed = conn.execute(
            "SELECT COUNT(*) AS c FROM events WHERE error IS NOT NULL"
        ).fetchone()["c"]
        n_validators = conn.execute(
            "SELECT COUNT(*) AS c FROM validators WHERE is_active=1"
        ).fetchone()["c"]
        validator_stake = conn.execute(
            "SELECT COALESCE(SUM(stake_wei),0) AS s FROM validators"
        ).fetchone()["s"]
        unstaking = conn.execute(
            "SELECT COUNT(*) AS c, COALESCE(SUM(wei),0) AS s FROM unstaking_queue"
        ).fetchone()

        # Distribution buckets
        buckets = []
        for lo, hi, label in BUCKETS:
            lo_wei = lo * WEI_PER_HYPE
            if hi is None:
                row = conn.execute(
                    "SELECT COUNT(*) AS n, COALESCE(SUM(staked_wei),0) AS s "
                    "FROM stakers WHERE staked_wei >= ?",
                    (lo_wei,),
                ).fetchone()
            else:
                hi_wei = hi * WEI_PER_HYPE
                row = conn.execute(
                    "SELECT COUNT(*) AS n, COALESCE(SUM(staked_wei),0) AS s "
                    "FROM stakers WHERE staked_wei >= ? AND staked_wei < ?",
                    (lo_wei, hi_wei),
                ).fetchone()
            buckets.append(
                {
                    "label": label,
                    "min_hype": lo,
                    "max_hype": hi,
                    "n_stakers": row["n"],
                    "hype": row["s"] / WEI_PER_HYPE,
                }
            )

        # Quick thresholds used by stat cards
        def gte(n: int) -> int:
            return conn.execute(
                "SELECT COUNT(*) AS c FROM stakers WHERE staked_wei >= ?",
                (n * WEI_PER_HYPE,),
            ).fetchone()["c"]

        thresholds = {n: gte(n) for n in (100, 1_000, 10_000, 100_000, 1_000_000)}

        return {
            "empty": False,
            "last_refresh_ms": int(last_refresh),
            "n_stakers": totals["n_stakers"],
            "total_staked_hype": totals["total_wei"] / WEI_PER_HYPE,
            "n_events": n_events,
            "n_failed_events": n_failed,
            "n_active_validators": n_validators,
            "validator_total_hype": validator_stake / WEI_PER_HYPE,
            "unstaking_queue_count": unstaking["c"],
            "unstaking_queue_hype": unstaking["s"] / WEI_PER_HYPE,
            "buckets": buckets,
            "thresholds": thresholds,
        }
    finally:
        conn.close()


@app.get("/api/stakers")
def stakers(
    min_hype: float = Query(0, ge=0),
    max_hype: float | None = Query(None, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    sort: str = Query("staked_desc", regex="^(staked_desc|staked_asc|recent)$"),
):
    conn = db.connect()
    try:
        where = ["staked_wei >= ?"]
        params: list = [int(min_hype * WEI_PER_HYPE)]
        if max_hype is not None:
            where.append("staked_wei <= ?")
            params.append(int(max_hype * WEI_PER_HYPE))
        where_sql = " AND ".join(where)

        order = {
            "staked_desc": "staked_wei DESC",
            "staked_asc": "staked_wei ASC",
            "recent": "last_action_ms DESC",
        }[sort]

        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM stakers WHERE {where_sql}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT user, staked_wei, n_validators, last_action_ms "
            f"FROM stakers WHERE {where_sql} ORDER BY {order} LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "results": [
                {
                    "user": r["user"],
                    "staked_hype": r["staked_wei"] / WEI_PER_HYPE,
                    "n_validators": r["n_validators"],
                    "last_action_ms": r["last_action_ms"],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/api/staker/{address}")
def staker_detail(address: str):
    addr = address.lower()
    conn = db.connect()
    try:
        row = conn.execute(
            "SELECT user, staked_wei, n_validators, last_action_ms FROM stakers WHERE user=?",
            (addr,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Staker not found")

        per_validator = conn.execute(
            "SELECT sv.validator, sv.staked_wei, v.name "
            "FROM staker_validators sv LEFT JOIN validators v ON v.validator = sv.validator "
            "WHERE sv.user=? ORDER BY sv.staked_wei DESC",
            (addr,),
        ).fetchall()
        recent = conn.execute(
            "SELECT time_ms, validator, wei, is_undelegate, error, hash "
            "FROM events WHERE user=? ORDER BY time_ms DESC LIMIT 50",
            (addr,),
        ).fetchall()

        return {
            "user": row["user"],
            "staked_hype": row["staked_wei"] / WEI_PER_HYPE,
            "n_validators": row["n_validators"],
            "last_action_ms": row["last_action_ms"],
            "delegations": [
                {
                    "validator": r["validator"],
                    "name": r["name"],
                    "staked_hype": r["staked_wei"] / WEI_PER_HYPE,
                }
                for r in per_validator
            ],
            "recent_events": [
                {
                    "time_ms": r["time_ms"],
                    "validator": r["validator"],
                    "hype": r["wei"] / WEI_PER_HYPE,
                    "is_undelegate": bool(r["is_undelegate"]),
                    "error": r["error"],
                    "hash": r["hash"],
                }
                for r in recent
            ],
        }
    finally:
        conn.close()


@app.get("/api/validators")
def validators():
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT v.*, "
            "  (SELECT COUNT(*) FROM staker_validators sv WHERE sv.validator=v.validator) AS n_delegators "
            "FROM validators v ORDER BY stake_wei DESC"
        ).fetchall()
        return [
            {
                "validator": r["validator"],
                "name": r["name"],
                "description": r["description"],
                "stake_hype": r["stake_wei"] / WEI_PER_HYPE,
                "commission": r["commission"],
                "is_active": bool(r["is_active"]),
                "is_jailed": bool(r["is_jailed"]),
                "apr": r["apr"],
                "n_delegators": r["n_delegators"],
            }
            for r in rows
        ]
    finally:
        conn.close()


def _csv_response(filename: str, header: list[str], row_iter) -> Response:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(header)
    w.writerows(row_iter)
    return Response(
        content=out.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _iso(ms: int | None) -> str:
    if not ms:
        return ""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


@app.get("/api/export/stakers.csv")
def export_stakers(min_hype: float = Query(0, ge=0)):
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT user, staked_wei, n_validators, last_action_ms FROM stakers "
            "WHERE staked_wei >= ? ORDER BY staked_wei DESC",
            (int(min_hype * WEI_PER_HYPE),),
        ).fetchall()
    finally:
        conn.close()
    return _csv_response(
        "hype-stakers.csv",
        ["address", "staked_hype", "n_validators", "last_action_iso"],
        (
            [r["user"], f"{r['staked_wei'] / WEI_PER_HYPE:.8f}", r["n_validators"], _iso(r["last_action_ms"])]
            for r in rows
        ),
    )


@app.get("/api/export/validators.csv")
def export_validators():
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT v.*, "
            "  (SELECT COUNT(*) FROM staker_validators sv WHERE sv.validator=v.validator) AS n_delegators "
            "FROM validators v ORDER BY stake_wei DESC"
        ).fetchall()
    finally:
        conn.close()
    return _csv_response(
        "hype-validators.csv",
        ["validator", "name", "stake_hype", "commission", "apr", "n_delegators", "is_active", "is_jailed"],
        (
            [
                r["validator"], r["name"] or "",
                f"{r['stake_wei'] / WEI_PER_HYPE:.8f}",
                r["commission"] or "", r["apr"] or "",
                r["n_delegators"], bool(r["is_active"]), bool(r["is_jailed"]),
            ]
            for r in rows
        ),
    )
