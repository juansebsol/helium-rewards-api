# HNT500 Index (Next.js + SQLite)

Daily-reconstituted Helium top-N basket index. Each calendar day uses **that day's** top earners from Nexus — not a fixed snapshot.

## Quick start

```bash
cd utils/HNT500-index-nextjs
cp .env.example .env
# fill NEXUS_API_KEY (and NEXUS_TOKEN if needed)
npm install
npm run dev
```

Open http://localhost:3001 — cron starts with the server. First build runs if `RUN_BUILD_ON_START=true`.

**Pages:** `/` overview · `/dashboard` index charts (alias `/index` via rewrite)

If you hit a Next.js manifest error after upgrading routes, delete `.next` and restart: `rm -rf .next && npm run dev`

## What it does

1. For each day in the last `INDEX_HISTORY_DAYS` (default 30), ending **yesterday**:
   - Fetch top `INDEX_SIZE` from `GET /api/nexus/helium/top-earners?day=...`
   - Sum `total_hnt` for all top-N that day (aggregate earnings)
   - Store `basket_hnt` (= sum), `basket_usd`, `hnt_price` in SQLite
2. Repeats on `CRON_SCHEDULE` (default every hour)

## API

| Route | Description |
|-------|-------------|
| `GET /api/index?days=30&n=500&weight_mode=earnings` | Index history JSON |
| `GET /api/health` | DB status, last build, cron config |
| `POST /api/rebuild` | Force rebuild (requires `REBUILD_SECRET` header `Authorization: Bearer …`) |

### Example

```bash
curl http://localhost:3001/api/index?days=7
```

## Scripts

| Command | Purpose |
|---------|---------|
| `npm run dev` | Dev server + cron (port 3001) |
| `npm run build && npm start` | Production |
| `npm run rebuild` | One-off CLI build (no server) |

## Env vars

See `.env.example`. Required: `NEXUS_API_KEY`.

| Variable | Default | Meaning |
|----------|---------|---------|
| `NEXUS_BASE_URL` | nexus.fractals.finance | Nexus host |
| `NEXUS_API_KEY` | — | Required |
| `NEXUS_TOKEN` | — | Optional header |
| `INDEX_SIZE` | 500 | Top N per day |
| `WEIGHT_MODE` | earnings | earnings \| equal \| rank |
| `INDEX_HISTORY_DAYS` | 30 | Days to store |
| `SQLITE_PATH` | ./data/hnt500.db | DB file |
| `CRON_SCHEDULE` | `0 */1 * * *` | Cron (every hour) |
| `RUN_BUILD_ON_START` | true | Build when server starts |
| `REBUILD_SECRET` | — | Enables POST /api/rebuild |
| `TOP_EARNERS_PAGE_DELAY_MS` | 400 | Nexus paging delay |
| `TOP_EARNERS_DAY_DELAY_MS` | 350 | Delay between days |

## SQLite schema

**`index_days`** — one row per (day, n, weight_mode): basket_hnt, basket_usd, hnt_price, constituent_count, updated_at

**`build_log`** — run history (status, days_built, errors)

DB file: `data/hnt500.db` (gitignored).

## Notes

- First build for 500×30 takes several minutes (Nexus paging).
- Use smaller `INDEX_SIZE` locally for faster tests.
- After code changes, run `npm run rebuild` for correct totals (sum of `total_hnt`).
- Dashboard shows **Devices loaded** and **Incomplete days** when Nexus pages fail.
- Wire `helium-index-dashboard.html` to `GET /api/index` instead of live Nexus loops when ready.
