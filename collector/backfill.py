"""Bounded, resumable backfill for public NSE daily bhavcopy archives."""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, timedelta

from collector.bhavcopy import collect_equity_bhavcopy
from database.repository import SignalRepository
from app.market_calendar import is_trading_holiday
from app.nse_fetcher import fetch_equity_history
from integrations.nse_bhavcopy_mcp import fetch_symbol_history_batch
from config.settings import get_settings
from utils.time import now_ist

logger = logging.getLogger(__name__)


def default_backfill_end_date() -> date:
    """Return yesterday in IST for the CLI's completed-session boundary."""
    return now_ist().date() - timedelta(days=1)


def recent_nse_trading_dates(end_date: date, count: int) -> list[date]:
    """Return at most `count` known weekday NSE dates, newest first."""
    if count <= 0:
        return []
    dates: list[date] = []
    cursor = end_date
    while len(dates) < count:
        holiday = is_trading_holiday(cursor)
        if cursor.weekday() < 5 and holiday is not True:
            # Unknown future years are deliberately omitted: no unverified
            # archives are requested just to satisfy a history count.
            if holiday is not None:
                dates.append(cursor)
        cursor -= timedelta(days=1)
    return dates


def bundled_fno_symbols() -> set[str]:
    """Return the real F&O stock universe (name kept for backward compatibility).

    It used to read symbols from the committed seed, which holds every NSE
    equity, so the filter removed nothing. See collector/universe.py.
    """
    from collector.universe import fno_symbols
    return fno_symbols()


def backfill_recent_bhavcopies(
    repository: SignalRepository,
    *,
    end_date: date,
    required_days: int = 60,
    max_downloads: int = 60,
    delay_seconds: float | None = None,
    symbols: set[str] | None = None,
) -> dict[str, int]:
    """Fetch missing dates newest-first, paced at no more than ten files/minute."""
    if required_days <= 0 or max_downloads <= 0:
        return {"requested": 0, "downloaded": 0, "stored": 0, "skipped": 0, "failed": 0}
    candidates = recent_nse_trading_dates(end_date, required_days)
    if symbols and hasattr(repository, "daily_equity_missing_dates_for_symbols"):
        missing_dates = repository.daily_equity_missing_dates_for_symbols(symbols, candidates)
        missing = [candidate for candidate in candidates if candidate.isoformat() in set(missing_dates)]
    else:
        existing = repository.daily_equity_trade_dates()
        missing = [candidate for candidate in candidates if candidate.isoformat() not in existing]
    if delay_seconds is None:
        delay_seconds = 60.0 / get_settings().backfill_max_per_min
    downloaded = stored = failed = 0
    consecutive_empty = 0
    for index, candidate in enumerate(missing[:max_downloads], start=1):
        try:
            bars = collect_equity_bhavcopy(candidate)
            if not bars:
                failed += 1
                consecutive_empty += 1
                logger.warning("Bhavcopy returned no usable rows date=%s", candidate)
                if consecutive_empty >= 3:
                    logger.warning("Bhavcopy bulk archive circuit breaker opened after %d consecutive empty dates", consecutive_empty)
                    break
            else:
                consecutive_empty = 0
                if symbols:
                    bars = [bar for bar in bars if str(bar.get("symbol") or "").upper() in symbols]
                downloaded += 1
                stored += repository.upsert_daily_equity_bars(bars)
        except Exception:
            failed += 1
            logger.warning("Bhavcopy download failed date=%s; retrying with backoff", candidate, exc_info=True)
            time.sleep(min(30.0, max(1.0, delay_seconds) * (2 ** min(failed, 4))))
        if index % 10 == 0 or index == min(len(missing), max_downloads):
            logger.info("Bhavcopy backfill progress batch=%d requested=%d stored=%d failed=%d", index, len(candidates), stored, failed)
        if delay_seconds > 0 and index < min(len(missing), max_downloads):
            time.sleep(delay_seconds)
    return {
        "requested": len(candidates),
        "downloaded": downloaded,
        "stored": stored,
        "skipped": len(candidates) - len(missing),
        "failed": failed,
    }



def backfill_symbol_history(
    repository: SignalRepository,
    symbols: set[str],
    *,
    end_date: date,
    max_symbols: int = 12,
    calendar_days: int = 100,
    delay_seconds: float | None = None,
) -> dict[str, int]:
    """Fill daily history through official NSE Bhavcopy MCP, with REST fallback."""
    wanted = sorted({str(s).upper() for s in symbols if s})[:max_symbols]
    if not wanted:
        return {"requested": 0, "downloaded": 0, "stored": 0, "failed": 0}
    if delay_seconds is None:
        delay_seconds = 60.0 / get_settings().backfill_max_per_min
    downloaded = stored = failed = 0

    try:
        batch = fetch_symbol_history_batch(wanted, end_date, delay_seconds=delay_seconds)
    except Exception as exc:
        batch = {}
        logger.warning("NSE Bhavcopy MCP unavailable; using historical REST fallback: %s", str(exc)[:160])

    unresolved: list[str] = []
    for symbol in wanted:
        rows = batch.get(symbol, [])
        if rows:
            stored += repository.upsert_daily_equity_bars(rows)
            downloaded += 1
        else:
            unresolved.append(symbol)

    start_date = end_date - timedelta(days=calendar_days)
    for index, symbol in enumerate(unresolved):
        try:
            rows = fetch_equity_history(symbol, start_date, end_date)
            if not rows:
                failed += 1
                logger.warning("NSE per-security history returned no rows symbol=%s", symbol)
            else:
                stored += repository.upsert_daily_equity_bars(rows)
                downloaded += 1
        except Exception:
            failed += 1
            logger.warning("NSE per-security history failed symbol=%s", symbol, exc_info=True)
        if delay_seconds > 0 and index + 1 < len(unresolved):
            time.sleep(delay_seconds)
    return {"requested": len(wanted), "downloaded": downloaded, "stored": stored, "failed": failed}

def backfill_symbol_gaps(
    repository: SignalRepository,
    symbols: set[str],
    *,
    end_date: date,
    dates: int = 20,
    delay_seconds: float | None = None,
) -> dict[str, int]:
    """Re-download recent bhavcopies but keep ONLY rows for ``symbols``.

    Used for F&O names the OI feed reports that Angel's master names differently
    (renames/demergers), so they get ATR/volume history without re-storing everything.
    """
    wanted = {str(s).upper() for s in symbols if s}
    if not wanted or dates <= 0:
        return {"requested": 0, "downloaded": 0, "stored": 0, "failed": 0}
    if delay_seconds is None:
        delay_seconds = 60.0 / get_settings().backfill_max_per_min
    candidates = recent_nse_trading_dates(end_date, dates)
    downloaded = stored = failed = 0
    for index, candidate in enumerate(candidates, start=1):
        try:
            rows = [bar for bar in collect_equity_bhavcopy(candidate) if str(bar.get("symbol") or "").upper() in wanted]
            stored += repository.upsert_daily_equity_bars(rows)
            downloaded += 1
        except Exception:
            failed += 1
            logger.warning("Gap-fill download failed date=%s", candidate, exc_info=True)
        if delay_seconds > 0 and index < len(candidates):
            time.sleep(delay_seconds)
    return {"requested": len(candidates), "downloaded": downloaded, "stored": stored, "failed": failed}


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill free public NSE EQ bhavcopy bars.")
    parser.add_argument("--days", type=int, default=60, help="Recent NSE trading days required (default: 60)")
    parser.add_argument("--max-downloads", type=int, default=60, help="Strict cap for this run (default: 60)")
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=default_backfill_end_date(),
    )
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = backfill_recent_bhavcopies(
        SignalRepository(get_settings().database_path),
        end_date=arguments.end_date,
        required_days=arguments.days,
        max_downloads=arguments.max_downloads,
        symbols=bundled_fno_symbols(),
    )
    print(result)


if __name__ == "__main__":
    main()
