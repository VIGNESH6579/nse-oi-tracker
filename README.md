# NSE Company News Monitor

A focused monitor for NSE F&O company announcements and financial results. It polls the official NSE corporate-announcements feed, filters to configured F&O symbols, stores filings in SQLite, classifies event type and cautious potential impact, and sends deduplicated alerts to ntfy.

## What it does
- Polls NSE corporate announcements; preserves filing timestamp and source attachment URL.
- Filters to the F&O universe supplied through `FNO_SYMBOLS` or the official NSE market-lot CSV.
- Classifies results, board meetings, orders/contracts, dividends, buybacks, acquisitions, fundraising, regulatory/legal and other filings.
- Labels potential impact as positive/negative/mixed/uncertain using transparent keyword heuristics. This is not a price forecast or trading recommendation.
- Stores events and notification state in SQLite.
- Sends new events to ntfy when `NTFY_TOPIC` is configured.
- Exposes a compact dashboard, JSON API and source health status.

## Run
```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Set `NTFY_TOPIC` to your topic name and optionally `NTFY_BASE_URL` / `NTFY_TOKEN`. Do not commit credentials. Set `FNO_SYMBOLS` to a comma-separated list if the NSE universe CSV is unavailable.

## API
- `GET /` dashboard
- `GET /api/events?limit=100&symbol=RELIANCE`
- `GET /api/health` source, scheduler and database status
- `POST /api/sync` trigger a poll manually

## Important limitations
NSE may return HTTP 403 due to its Akamai protection. The app reports this as a source error, backs off, and does not use public proxies or attempt to bypass access controls. Polling is best-effort, not guaranteed real-time. Render Free instances can sleep and their local filesystem is ephemeral; use a persistent disk or external database for durable production history. Notifications require a reachable ntfy server and valid topic configuration. The impact label is a rough filing-text classification, not a prediction of next-session price movement.
