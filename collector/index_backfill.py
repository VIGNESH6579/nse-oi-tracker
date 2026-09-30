"""Bounded public NSE index-history backfill with a safe Angel-to-NSE fallback."""
from datetime import timedelta
import logging
import time

from app.config import INDICES
from app.nse_fetcher import fetch_index_history
from utils.time import now_ist

logger = logging.getLogger(__name__)

INDEX_TYPES = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
    "FINNIFTY": "NIFTY FINANCIAL SERVICES",
    "MIDCPNIFTY": "NIFTY MIDCAP SELECT",
}


def backfill_index_bars(repository, *, days=60, max_downloads=60, angel_client=None):
    end = now_ist().date() - timedelta(days=1)
    start = end - timedelta(days=days * 2)
    stored = 0
    sources = set()

    for symbol in INDEX_TYPES:
        rows = []
        source = "nse_index_history"
        try:
            if angel_client is not None:
                source = "angel_one_daily_index"
                candles = angel_client.daily_candles(symbol, days=days, exchange="NSE")
                for candle in candles:
                    trade_date = str(candle.get("time") or "")[:10]
                    if trade_date and start.isoformat() <= trade_date <= end.isoformat():
                        rows.append({
                            "trade_date": trade_date,
                            "open": candle["open"],
                            "high": candle["high"],
                            "low": candle["low"],
                            "close": candle["close"],
                        })
            else:
                rows = fetch_index_history(INDEX_TYPES[symbol], start, end)
        except Exception as exc:
            # Angel historical data is rate-limited independently of the live
            # stream. Do not retry it here; immediately use the public NSE
            # history path for this index instead.
            logger.warning(
                "Angel index history unavailable for %s (%s); falling back to NSE",
                symbol,
                type(exc).__name__,
            )
            source = "nse_index_history"
            if angel_client is not None:
                try:
                    rows = fetch_index_history(INDEX_TYPES[symbol], start, end)
                except Exception:
                    logger.exception("NSE index history fallback failed for %s", symbol)

        sources.add(source)
        stored += repository.upsert_daily_index_bars(
            [{**row, "symbol": symbol, "source": source} for row in rows]
        )
        time.sleep(0.25)

    return {
        "stored": stored,
        "symbols": len(INDICES),
        "source": ",".join(sorted(sources)) if sources else "none",
    }
