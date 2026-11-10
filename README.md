# NSE Company Event & Results Monitor

Tracks official NSE and BSE RSS filings for NSE F&O companies, separates upcoming board meetings, results-published filings and fresh corporate announcements, stores/deduplicates events, and sends ntfy alerts. This is a news-only application; no broker or market-price integration is included.

## Sources
- Primary filing/event feed: official NSE RSS feeds (announcements, board meetings, financial results).
- Independent fallback: official BSE RSS feeds for announcements, board meetings and financial results. Events are matched back to the NSE F&O ticker universe using company names and known aliases.
- Structured NSE board-meetings and event-calendar APIs are attempted as supplements, but Render currently receives HTTP 403 from these endpoints; BSE RSS is used as the independent fallback.
- F&O universe: official NSE F&O market-lot CSV or explicit `FNO_SYMBOLS`.
- NSE's normal JSON API is not used as the only source; if it returns 403, RSS feed health is shown independently. No proxy or anti-bot bypass is used.

## Direction labels
Results are marked RESULT PUBLISHED only for actual result filings, not board-meeting agendas, earnings-call transcripts, or parent-company references. When BSE iXBRL is available, current-period revenue/PAT/EPS/EBITDA are extracted. YoY impact is assigned only when the same document contains a matching previous-year quarter; otherwise impact stays NOT ASSESSED. Headlines are never used to invent figures.

## Run
```bash
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Set `NTFY_TOPIC`, optionally `NTFY_TOKEN`, and if needed `FNO_SYMBOLS` (comma-separated). To enable manual sync, set a long random `SYNC_TOKEN` in Render and send it only in the `X-Sync-Token` header. Keep secrets in Render environment variables, never in Git.

## API
- `GET /` dashboard
- `GET /api/events?limit=100&symbol=RELIANCE&status_filter=RESULT%20PUBLISHED`
- `GET /api/health` (liveness and scanner diagnostics)
- `GET /api/ready` (scanner readiness; 503 when no valid F&O scan is available)
- `POST /api/sync` is disabled unless `SYNC_TOKEN` is configured; when enabled it requires the `X-Sync-Token` header.

NSE and BSE RSS feeds are exchange-published sources. Actual reachability and item counts are shown independently in the dashboard's Data status panel. Render Free may sleep and local SQLite is ephemeral. For reliable continuous polling and durable history, use an always-on worker and persistent storage.
