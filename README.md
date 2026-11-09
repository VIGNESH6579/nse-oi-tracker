# NSE Company Event & Results Monitor

Tracks official NSE RSS announcements for F&O companies, separates upcoming board meetings, results-published filings and sudden corporate announcements, stores/deduplicates events, and sends ntfy alerts.

## Sources
- Primary filing/event feed: official NSE RSS feeds (announcements, board meetings, financial results).
- F&O universe: official NSE F&O market-lot CSV or explicit `FNO_SYMBOLS`.
- NSE's normal JSON API is not used as the only source; if it returns 403, RSS feed health is shown independently. No proxy or anti-bot bypass is used.

## Direction labels
Results are explicitly marked RESULT PUBLISHED when the exchange feed identifies a results filing. Until structured reported figures can be extracted and compared with prior periods and expectations, direction remains NEUTRAL / unclassified with an explanation. A filing title alone is not enough to call a result bullish or bearish. This build does not yet claim price-confirmed impact.

## Market data
No market-price feed is currently configured. A price reaction layer requires a valid market-data provider/API credential. The app must label quote source, timestamp, delay and freshness rather than imply live prices. Do not treat a company filing's direction label as a price prediction.

## Run
```bash
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Set `NTFY_TOPIC`, optionally `NTFY_TOKEN`, and if needed `FNO_SYMBOLS` (comma-separated). Keep secrets in Render environment variables, never in Git.

## API
- `GET /` dashboard
- `GET /api/events?limit=100&symbol=RELIANCE&status_filter=RESULT%20PUBLISHED`
- `GET /api/health`
- `POST /api/sync`

NSE RSS is an official exchange-published feed, but third-party feed-reader indexing suggests the archive announcements feed is active; actual access from this Render deployment must be verified. Render Free may sleep and local SQLite is ephemeral. For reliable continuous polling and durable history, use an always-on worker and persistent storage.
