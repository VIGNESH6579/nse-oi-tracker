# NSE OI Tracker PRO ? foundations

NSE OI Tracker is a FastAPI dashboard for public NSE F&O OI-spurts data and
option-chain analytics. This branch adds the production foundation for
server-owned signal history, SQLite event storage, scheduled lifecycle jobs,
and a dashboard migration path from browser-local history.

The current build keeps the existing F&O OI scanner as an optional research
mode and adds a separate Angel One stock-options scalping paper mode. CAS has
been removed from the product path: no CAS signals, context, API, or dashboard
UI are used.

It is analytical decision support, not investment advice. Current OI labels
are not a complete multi-factor trading signal.

## Run locally

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt pytest
Copy-Item .env.example .env
.\.venv\Scripts\uvicorn app.main:app --reload
```

Open `http://127.0.0.1:8000`. The SQLite database is created in `data/` by
default. Keep one Uvicorn worker because the app runs an in-process scheduler.

## Docker

```powershell
Copy-Item .env.example .env
docker compose up --build
```

The compose volume keeps SQLite history in `./data`. Do not use an ephemeral
PaaS filesystem if durable history is required.

## Render free-tier deployment

This repository includes a `render.yaml` Blueprint. After connecting the
repository in Render, it deploys the FastAPI web service and redeploys on each
commit to `main`. The health check is `/api/health` and Render supplies `PORT`.

Render's Free web service is appropriate for a live demo, not durable history:
it spins down after idle time and its local filesystem (including SQLite) is
lost on restart, redeploy, or spin-down. Use a paid persistent disk or a
durable external database before treating history as production data.

## Core API

- `GET /api/health` ? market status, version, persistence status, and last
  refresh metadata.
- `GET /api/oi-signals` ? current scanner output with freshness metadata.
- `GET /api/option-chain/{symbol}` ? read-only Angel near-ATM CE/PE ladder for one stock.
- `GET /api/history/today` ? visible, server-owned IST-day signal events and
  lifecycle status.
- `GET /api/analytics/today` ? server-calculated same-day outcomes.
- `GET /api/technical/{symbol}` ? daily NSE-bhavcopy EMA/ATR/volume/regime
  context. It is not an intraday VWAP or a standalone trading recommendation.

`/api/debug` is disabled unless `DEBUG_TOKEN` is configured; pass it through
the `X-Debug-Token` request header in a trusted environment.

The bundled dashboard is served from the API's own origin. If deploying a
separate dashboard, set `NSE_OI_CORS_ORIGINS` to its exact comma-separated
HTTPS origins; it is deliberately blank by default.

## Extended API

- `GET /api/scalp/live` ? current open paper scalps and recent events.
- `GET /api/scalp/history/today` ? persisted today-only scalp events with exit reason and hold time.
- `GET /api/market-overview` and `/api/market-regime` expose public NSE
  index/VIX/breadth/FII-DII context.
- `GET /api/news` returns public NSE corporate disclosures with event-risk
- `GET /api/participant-oi` returns the latest public NSE participant-wise
  OI report and preserves its end-of-day report date (it is not intraday).
- `GET /api/backtest` and `/api/backtest/export.csv` provide an auditable
  candidate-event export, not an options-strategy performance claim.

## Seed daily technical history

The app ingests the newly published bhavcopy at 18:10 IST each weekday. To
seed enough public NSE daily history for EMA50 and ATR validation immediately,

run this bounded, rate-limited command once (it never uses a paid API):

```powershell
.\.venv\Scripts\python -m collector.backfill --days 60 --max-downloads 60
```

The command skips dates already stored and only requests known weekday NSE
trading dates. It is safe to rerun after a network interruption.

## Verification

```powershell
.\.venv\Scripts\python -m pytest -q
```

See [architecture](docs/architecture.md) for data ownership, scheduler rules,
and the current signal-quality boundary. See [signal logic](docs/signal-logic.md),
[API reference](docs/api.md), [scheduler flow](docs/scheduler.md),
[deployment](docs/deployment.md), and [troubleshooting](docs/troubleshooting.md)
for operating guidance.



## Stock-option scalping paper mode

Default `SIGNAL_MODE=scalp_chain` is paper-only. It ranks the live F&O universe
by intraday percentage change and evaluates the top 20 gainers plus top 20
losers using timestamped Angel One WebSocket five-minute candles. It checks
opening-range breakout, VWAP side, momentum, and non-dead volume before making
a bounded on-demand near-ATM CE/PE chain request. A candidate must then pass
the option premium/OI confirmation. A quiet session with zero entries is valid;
the health endpoint should expose why candidates were rejected.

- Entry window: 09:30–15:00 IST.
- Maximum hold: 10 minutes.
- Hard exit/no new entries after 15:15 IST.
- Maximum concurrent paper positions: 3.
- No live orders or CAS.

The OI observation layer is separate from the scalp-entry engine. An OI
classification is not automatically a scalp entry. APIs:
`GET /api/scalp/live`, `GET /api/scalp/history/today`,
`GET /api/oi-signals`, and `GET /api/health`.
