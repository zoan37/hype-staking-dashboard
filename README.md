# HYPE Staking Dashboard

Live stats and per-staker breakdowns for HYPE staking on Hyperliquid.

**Live:** [hype-staking-dashboard.fly.dev](https://hype-staking-dashboard.fly.dev)

## What it shows

- Total HYPE staked, unique staker count, active validators
- Stake distribution histogram (buckets from <1 HYPE up to ≥1M HYPE)
- Threshold counts (≥100, ≥1k, ≥10k, ≥100k, ≥1M HYPE stakers)
- Filterable, sortable staker list (44k+ addresses)
- Validator table with commission, APR, delegator count, jailed/inactive status
- Pending unstaking queue size
- CSV exports for stakers and validators

## How it works

The Hyperliquid info API is user-centric for staking — there's no endpoint that lists all stakers. The dashboard fills that gap by:

1. Pulling all delegation events from [HypurrScan's](https://api.hypurrscan.io) `/allDelegations` (~490k events)
2. Streaming them into SQLite with `ijson` (constant memory regardless of dataset size)
3. Replaying delegate/undelegate pairs to derive each address's current stake
4. Combining with `validatorSummaries` from [api.hyperliquid.xyz](https://api.hyperliquid.xyz) for validator-side metadata

The replay reconstructs validator totals to within ~3% of `validatorSummaries` — there's some redelegation/reward mechanic not surfaced as events. Per-staker rankings and bucket counts are reliable.

## Stack

- Python 3.12 + FastAPI + SQLite (single file on a persistent volume)
- `ijson` streaming JSON parser
- Single-page static frontend (no framework, vanilla JS)
- Deployed on Fly.io (2 machines for zero-downtime rolling deploys)

## Local development

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# One-shot ingest into data/staking.db
.venv/bin/python ingest.py

# Serve the dashboard at http://127.0.0.1:8000
.venv/bin/uvicorn app:app --port 8000
```

## API

Endpoints used by the dashboard (no authentication, public):

| Path | What |
|---|---|
| `GET /api/stats` | Aggregates, buckets, thresholds |
| `GET /api/stakers?min_hype=&max_hype=&limit=&offset=&sort=` | Paginated staker list |
| `GET /api/staker/{address}` | Per-staker breakdown + recent events |
| `GET /api/validators` | Validator list with delegator counts |
| `GET /api/export/stakers.csv` | Full staker CSV dump |
| `GET /api/export/validators.csv` | Validator CSV dump |
| `GET /api/health` | Returns 503 until first ingest completes |

`POST /api/refresh` triggers an out-of-cycle ingest; requires `Authorization: Bearer $REFRESH_TOKEN`.

## Deployment notes

- Each Fly machine has its own SQLite volume and refreshes independently every hour (`AUTO_REFRESH=1`, `REFRESH_INTERVAL_S=3600`). They can drift by up to an hour; fine for staking data.
- Rolling deploys: Fly replaces machines one at a time. The health check (`/api/health`) gates traffic on ingest completion, so the new machine doesn't take traffic until its DB is populated.
- Memory: peak ~220MB during ingest (after the ijson refactor). 1GB machines have ~4× headroom.

## Data sources

- [HypurrScan](https://hypurrscan.io) — `/allDelegations`, `/unstakingQueue`
- [Hyperliquid info API](https://hyperliquid.gitbook.io/hyperliquid-docs) — `validatorSummaries`
