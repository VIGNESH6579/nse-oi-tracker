# API reference

All routes return JSON except the root dashboard and the CSV export. Times and
daily partitions use Asia/Kolkata (IST).

## Runtime and scanner

- `GET /api/health`: version, market/holiday state, last scanner metadata,
  daily-bar freshness, and database readiness.
- `GET /api/sources`: declared public-source coverage. `NOT_CONFIGURED`
  sources are intentionally not inferred or substituted.
- `GET /api/oi-signals`: cached all-F&O price/OI candidates. Optional query
  parameters: `refresh`, `signal`, `tier`, `sector`, `min_strength`.
- `GET /api/category/{category}`: one of `long_buildup`, `short_buildup`,
  `short_covering`, or `long_unwinding`.
- `GET /api/signal/{symbol}`: on-demand futures observation or structured
  upstream error.

## Market context

- `GET /api/market-overview?refresh=false`: public NSE indices, India VIX,
  breadth, and FII/DII cash activity.
- `GET /api/market-regime?refresh=false`: daily/live context classification;
  never a directional order.
- `GET /api/market-intelligence`: compact cache-backed public-data briefing.
- `GET /api/participant-oi?refresh=false`: latest stored NSE participant-wise
  OI end-of-day report. `is_intraday` is always false.
- `GET /api/news?symbol=&limit=100&refresh=false`: normalized public NSE
  corporate announcements, classified for event volatility only.

## Technical and option chain

- `GET /api/technical/{symbol}`: persisted daily bhavcopy indicators.
- `GET /api/option-chain/{symbol}?strikes=1`: read-only Angel near-ATM stock CE/PE ladder.
- `GET /api/scalp/live`: current open paper scalps and recent events.
- `GET /api/scalp/history/today`: persisted paper scalp events for the current IST day.

## History and audit exports

- `GET /api/history/today?limit=1000`: visible IST-day event history.
- `GET /api/analytics/today`: current-day lifecycle metrics plus sector/stock
  candidate breakdowns.
- `GET /api/backtest?from_date=YYYY-MM-DD&to_date=YYYY-MM-DD`: up to 366
  days of stored candidate-event outcome summaries.
- `GET /api/backtest/export.csv?from_date=YYYY-MM-DD&to_date=YYYY-MM-DD`:
  underlying audit records as CSV.

`/api/debug` is disabled unless `DEBUG_TOKEN` is configured and supplied in
the `X-Debug-Token` header. It should not be used as an ordinary dashboard API.
