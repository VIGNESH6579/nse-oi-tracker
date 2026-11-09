# NSE Company Event & Results Monitor

Tracks official NSE RSS announcements for F&O companies, separates upcoming board meetings, results-published filings and sudden corporate announcements, stores/deduplicates events, and sends ntfy alerts.

## Sources
- Primary filing/event feed: official NSE RSS feeds (announcements, board meetings, financial results).
- F&O universe: official NSE F&O market-lot CSV or explicit `FNO_SYMBOLS`.
- NSE's normal JSON API is not used as the only source; if it returns 403, RSS feed health is shown independently. No proxy or anti-bot bypass is used.

## Direction labels
Results are explicitly marked RESULT PUBLISHED when the exchange feed identifies a results filing. Until structured reported figures can be extracted and compared with prior periods and expectations, direction remains NEUTRAL / unclassified with an explanation. A filing title alone is not enough to call a result bullish or bearish. This build does not yet claim price-confirmed impact.

## Market data (read-only Angel One SmartAPI)
- Angel One is used only for NSE cash-equity/index market quotes: LTP, percentage change, OHLC and traded volume.
- The app uses the SmartAPI login and market quote endpoint only. It does not contain order placement, order modification/cancellation, GTT, holdings, or position actions.
- The quote response may contain additional exchange fields; the application discards OI and depth and does not store or analyze them.
- Quotes are fetched on the event-poll cadence (default 5 minutes), with NIFTY 50 as a comparison benchmark. The displayed timestamp is when the quote was fetched, not a guarantee of exchange tick time.
- Configure `ANGEL_ONE_API_KEY`, `ANGEL_ONE_CLIENT_CODE`, `ANGEL_ONE_PASSWORD`, and `ANGEL_ONE_TOTP_SECRET` as Render secrets. Optional `ANGEL_SYMBOL_TOKENS` is a JSON object mapping NSE symbols to Angel tokens; if omitted, the adapter loads the public instrument master and filters NSE cash-equity/index instruments.
- `ANGEL_MARKET_DATA_ENABLED=0` disables the quote layer.
- Price movement is context only, not proof that an announcement caused the move. Relative change is stock % change minus NIFTY 50 % change.

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
