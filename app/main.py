# main.py ? FastAPI production app for NSE F&O OI Scanner
# Version 4.2.0 ? holiday-aware market status, native-price-change fix,
#                  MEDIUM-tier signals restored, gated /api/debug

import logging
import asyncio
import threading
import time
import csv
import io
import os
import gc
import ctypes
try:
    import resource
except ImportError:
    resource = None
from datetime import date, datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from fastapi.middleware.cors import CORSMiddleware

import app.oi_analyzer as oi_engine

from app.market_calendar import (
    get_market_status,
    has_holiday_calendar_for_year,
    holiday_calendar_metadata,
    refresh_holiday_calendar,
    MARKET_STATUS_OPEN,
    MARKET_STATUS_LABELS,
)
from app.cache import cache
from app.oi_analyzer import (
    scan_all_fno_realtime,
    last_scan_data_status,
    SIGNAL_META,
    sample_field_usage,
)
from app.nse_fetcher import (
    fetch_all_fno_oi_change,
    fetch_market_indices,
    test_nse_connectivity,
)
from analytics.technical import technical_context
from analytics.intraday import observe as observe_intraday
from analytics.intraday import candle_vwap
from collector.bhavcopy import collect_equity_bhavcopy
from collector.backfill import backfill_recent_bhavcopies, recent_nse_trading_dates, bundled_fno_symbols
from collector.index_backfill import backfill_index_bars
from app.database_backup import restore_latest_backup, restore_bundled_seed, upload_database_snapshot, last_snapshot_age_s, last_snapshot_info
from collector.universe import universe_source, cached_universe_size, cached_universe, add_extra_symbols
from collector.backfill import backfill_symbol_gaps
import app.self_test as self_test
from app.market_calendar import is_trading_holiday
from collector.fno_ban import refresh_ban_list, banned_symbols, ban_info
from analytics.intraday_confirm import summarize_candles, average_daily_volume, bias_from_candles, bias_from_quote, quote_context
from signal_engine.confirmation import evaluate_gate, ENTRY_SIGNALS, INDEX_SYMBOLS
from signal_engine.quality import apply_daily_technical_context, apply_intraday_observation_context
from analytics.market_overview import normalize_market_overview
from analytics.backtest import summarize_candidate_backtest
from alerts.dispatcher import dispatch_candidate_alert
from analytics.sectors import attach_sector, known_sectors
from analytics.cas import confidence_analysis
from analytics.traps import trap_risk
from analytics.sources import public_source_inventory
from integrations.angel_one_market_data import AngelOneMarketData
from config.settings import get_settings
from database.repository import SignalRepository
from utils.time import IST, now_ist, ist_trade_date

APP_VERSION = "4.4.0"
settings = get_settings()
repository = SignalRepository(settings.database_path)
angel_market_data = AngelOneMarketData.from_environment()

# Set this in Render's environment variables to lock down /api/debug in
# production. Left unset, /api/debug stays open (dev convenience) but says
# so loudly in its own response ? "production standard" means the open-by-
# default state is visible, not silently assumed safe.
DEBUG_TOKEN = settings.debug_token

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class _HealthAccessFilter(logging.Filter):
    """Drop /api/health access lines (Render probes it every 5 s) so real logs stay visible."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            return "/api/health" not in record.getMessage()
        except Exception:
            return True


logging.getLogger("uvicorn.access").addFilter(_HealthAccessFilter())
STALE_SIGNAL_GRACE_SECONDS = 180
_last_good_signals: list[dict] = []
_last_good_signals_at = 0.0
_last_refresh_at_ist: str | None = None
_last_refresh_was_stale = False
_last_snapshot_id: int | None = None
_last_refresh_completed_monotonic = 0.0
_started_at = now_ist()
_startup_state = "STARTING"
_startup_ready_at_ist: str | None = None
_startup_error: str | None = None
_scheduler_started_at_ist: str | None = None
_scheduler_heartbeat_at_ist: str | None = None
MIN_REFRESH_INTERVAL_SECONDS = 45.0
_vwap_cache: dict[tuple[str, str], tuple[float, dict]] = {}
_last_vwap_request_at = 0.0
_refresh_lock = asyncio.Lock()
_backfill_lock = asyncio.Lock()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = PROJECT_ROOT / "static"


# ?? Helpers ???????????????????????????????????????????????????????????????????

def is_market_open() -> bool:
    """
    Backward-compatible boolean. Used everywhere that only needs a yes/no.
    This used to ONLY check weekday + clock window, so an NSE trading
    holiday falling on a weekday was reported as "market open" and the
    poller scanned a dead market. It now defers to market_calendar, which
    knows about holidays too.
    """
    return get_market_status() == MARKET_STATUS_OPEN


def expected_latest_bhavcopy_date(now: datetime | None = None) -> date:
    """Return the latest NSE date whose daily bhavcopy should be available.

    NSE's daily file is published after the trading day. Before 18:30 IST,
    absence of today's file is normal, so health checks expect the previous
    known trading date. After that cutoff, today's file is expected.
    """
    observed_at = (now or now_ist()).astimezone(IST)
    candidate = observed_at.date()
    if (observed_at.hour, observed_at.minute) < (18, 30):
        candidate -= timedelta(days=1)
    return recent_nse_trading_dates(candidate, 1)[0]


def bhavcopy_backfill_required(
    summary: dict[str, object], now: datetime | None = None
) -> bool:
    """Report stale or empty daily equity history as requiring backfill."""
    bars = int(summary.get("bars") or 0)
    latest_trade_date = summary.get("latest_trade_date")
    if bars == 0 or not latest_trade_date:
        return True
    return str(latest_trade_date) < expected_latest_bhavcopy_date(now).isoformat()


def _refresh_signals() -> list[dict]:
    """Fetch, persist, and cache one signal scan in a worker thread."""
    global _last_good_signals, _last_good_signals_at
    global _last_refresh_at_ist, _last_refresh_was_stale, _last_snapshot_id
    signals = scan_all_fno_realtime()
    if signals and angel_market_data is not None:
        try:
            quotes = angel_market_data.full_quotes(
                [str(signal.get("symbol") or "") for signal in signals]
            )
            enriched = []
            for signal in signals:
                symbol = str(signal.get("symbol") or "").upper()
                quote = quotes.get(symbol)
                if quote and quote.get("ltp", 0) > 0:
                    enriched.append({
                        **signal,
                        "nse_ltp": signal.get("ltp"),
                        "ltp": round(float(quote["ltp"]), 2),
                        "volume": quote.get("volume", signal.get("volume", 0)),
                        "angel_quote": quote,
                        "realtime_source": "angel_one_read_only",
                    })
                else:
                    enriched.append(signal)
            signals = enriched
        except Exception:
            logger.exception("Read-only Angel One quote overlay failed; retaining NSE data")
    signals = [attach_sector(signal) for signal in signals]
    # The public NSE live endpoint is polled every minute. Where it supplies
    # cumulative volume, retain a session VWAP from those real observations.
    # This is deliberately labelled observation-based; it is not fabricated
    # 5-minute OHLCV and never turns a candidate into an order recommendation.
    session_date = now_ist().date()
    enriched_intraday = []
    candle_calls = 0
    vwap_deadline = time.monotonic() + 8.0
    for signal in signals:
        symbol = str(signal.get("symbol") or "")
        context = observe_intraday(symbol, float(signal.get("ltp") or 0), float(signal.get("volume") or 0), session_date)
        # Every buildup candidate needs candles for the gate (VWAP, opening range, volume pace),
        # not only confidence>=70 ones; the per-scan budget below bounds the Angel call rate.
        _now = now_ist()
        qctx = quote_context(signal.get("angel_quote") or {}, oi_engine.oi_window.opening_range(symbol), _now.hour * 60 + _now.minute)
        if qctx and qctx["or_complete"] and str(signal.get("signal") or "NEUTRAL") in ENTRY_SIGNALS:
            # Everything the gate needs is already here: no historical-candle call (Angel answers many with 403).
            _data_quality["quote_ctx"] = _data_quality.get("quote_ctx", 0) + 1
            enriched_intraday.append({**signal, "intraday_context": qctx})
            continue
        if angel_market_data is not None and str(signal.get("signal") or "NEUTRAL") in ENTRY_SIGNALS:
            try:
                global _last_vwap_request_at
                cache_key = (symbol.upper(), "FIVE_MINUTE")
                cached = _vwap_cache.get(cache_key)
                if cached and time.monotonic() - cached[0] < float(os.getenv("VWAP_CACHE_S", "300")):
                    context = dict(cached[1])
                    context["vwap_age_s"] = round(time.monotonic() - cached[0], 2)
                    enriched_intraday.append({**signal, "intraday_context": context})
                    continue
                if candle_calls >= int(os.getenv("MAX_CANDLE_SYMBOLS", "8")):
                    enriched_intraday.append({**signal, "intraday_context": context})
                    continue
                candle_calls += 1
                wait_s = 0.5 - (time.monotonic() - _last_vwap_request_at)
                if wait_s > 0:
                    time.sleep(min(wait_s, 0.5))
                if time.monotonic() <= vwap_deadline:
                    candles = angel_market_data.intraday_candles(symbol, interval="FIVE_MINUTE", exchange="NSE", days=1)
                    _last_vwap_request_at = time.monotonic()
                else:
                    candles = []
                summary = summarize_candles(candles)
                broker_vwap = candle_vwap(candles)
                _data_quality["candle_ok" if summary.get("available") else "candle_empty"] += 1
                if summary.get("available"):
                    # Indices have no volume: fall back to the time-weighted average (TWAP).
                    context = {
                        **summary,
                        "vwap": broker_vwap if broker_vwap is not None else summary.get("twap"),
                        "vwap_kind": "vwap" if broker_vwap is not None else "twap",
                        "available": True,
                        "source": "angel_one_5m_ohlcv",
                        "data_frequency": "FIVE_MINUTE",
                        "candle_count": len(candles),
                        "vwap_age_s": 0.0,
                    }
                    _vwap_cache[cache_key] = (time.monotonic(), dict(context))
            except Exception as exc:
                _data_quality["candle_fail"] += 1
                stale = _vwap_cache.get((symbol.upper(), "FIVE_MINUTE"))
                if stale and time.monotonic() - stale[0] < float(os.getenv("VWAP_STALE_MAX_S", "900")):
                    context = {**stale[1], "vwap_age_s": round(time.monotonic() - stale[0], 2), "stale_candles": True}
                if qctx and not context.get("stale_candles"):
                    context = qctx                       # exchange VWAP + volume beat an observation VWAP
                log = logger.info if type(exc).__name__ == "AngelUnavailable" else logger.warning
                log("Angel candles unavailable for %s (%s); using %s", symbol, str(exc)[:100],
                    "last good candles" if context.get("stale_candles") else "observation VWAP")
        enriched_intraday.append({**signal, "intraday_context": context})
    signals = enriched_intraday
    if signals:
        bars_by_symbol: dict = {}
        try:
            symbols = [str(signal.get("symbol") or "") for signal in signals]
            bars_by_symbol = _merge_bars(repository.daily_equity_bars_for_symbols(symbols),
                                         repository.daily_index_bars_for_symbols(symbols))
            signals = [
                apply_daily_technical_context(
                    signal,
                    technical_context(bars_by_symbol.get(str(signal.get("symbol") or ""), [])),
                )
                for signal in signals
            ]
            signals = [
                apply_intraday_observation_context(signal, signal.get("intraday_context"))
                for signal in signals
            ]
            signals = [{
                **signal,
                "trap_context": trap_risk(signal, signal.get("technical_context")),
            } for signal in signals]
        except Exception:
            logger.exception("Could not attach daily technical context to scanner results")
        try:
            signals = _apply_confirmation_gate(signals, bars_by_symbol)
        except Exception:
            logger.exception("Confirmation gate failed; failing closed")
            signals = [{**signal, "actionable": False, "trade_recommendation": "NO_TRADE",
                        "confirmation_gate": "FAILED", "missing_confirmations": ["gate_error"]} for signal in signals]
        try:
            market_context = cache.get("market-overview")
            if market_context is None:
                # VIX/index regime only, refreshed at most once per cache TTL. News/announcements
                # and FII/DII cash flow were removed from the scan: no intraday signal value.
                market_context = normalize_market_overview(fetch_market_indices(), [])
                cache.set("market-overview", market_context, ttl=settings.cache_ttl_seconds)
            signals = [{**signal, "cas_context": confidence_analysis(signal.get("technical_context"), None, market_context)}
                       for signal in signals]
        except Exception:
            logger.exception("Could not attach public VIX/regime/CAS context to scanner results")
    monotonic_now = time.monotonic()
    captured_at = now_ist()
    served_stale = False
    if signals:
        _last_good_signals = signals
        _last_good_signals_at = monotonic_now
    elif _last_good_signals and monotonic_now - _last_good_signals_at <= STALE_SIGNAL_GRACE_SECONDS:
        logger.warning("Empty scan received; serving last valid signals during grace period")
        signals = _last_good_signals
        served_stale = True

    if not served_stale:
        try:
            write = repository.record_scan(signals, captured_at)
            _last_snapshot_id = write.snapshot_id
            repository.update_open_events(signals, captured_at)
        except Exception:
            logger.exception("Could not persist scan history")

        # Alert delivery is opt-in. Per-symbol/day/channel dedup prevents a
        # frequent scanner refresh from becoming a notification storm.
        if settings.alert_webhook_url or settings.ntfy_topic_url or settings.telegram_bot_token:
            for signal in signals:
                if int(signal.get("confidence") or 0) < settings.alert_min_confidence:
                    continue
                for channel, endpoint in (
                    ("webhook", settings.alert_webhook_url),
                    ("ntfy", settings.ntfy_topic_url),
                    ("telegram", settings.telegram_bot_token),
                ):
                    if not endpoint:
                        continue
                    key = f"candidate:{ist_trade_date(captured_at)}:{signal.get('symbol')}:{channel}"
                    if not repository.reserve_alert(key, channel, captured_at):
                        continue
                    deliveries = dispatch_candidate_alert(
                        signal,
                        webhook_url=endpoint if channel == "webhook" else None,
                        ntfy_topic_url=endpoint if channel == "ntfy" else None,
                        telegram_bot_token=endpoint if channel == "telegram" else None,
                        telegram_chat_id=settings.telegram_chat_id if channel == "telegram" else None,
                    )
                    for delivery in deliveries:
                        repository.complete_alert(
                            key, delivered=delivery.delivered, detail=delivery.detail,
                            observed_at=captured_at,
                        )

    _last_refresh_at_ist = captured_at.isoformat()
    _last_refresh_was_stale = served_stale
    cache.set("all_signals", signals, ttl=settings.cache_ttl_seconds)
    h = sum(1 for s in signals if s.get("confidence_tier") == "HIGH")
    m = sum(1 for s in signals if s.get("confidence_tier") == "MEDIUM")
    logger.info(f"Scan complete ? {len(signals)} signals ({h} HIGH, {m} MEDIUM)")
    return signals


def _release_scan_memory() -> None:
    """Return freed scan-cycle memory to the OS where glibc supports it."""
    gc.collect()
    try:
        ctypes.CDLL(None).malloc_trim(0)
    except (AttributeError, OSError):
        pass


def _refresh_signals_and_release_memory() -> list[dict]:
    try:
        return _refresh_signals()
    finally:
        _release_scan_memory()


async def refresh_signals() -> list[dict]:
    """Serialize all refresh callers to avoid upstream request stampedes."""
    global _last_refresh_completed_monotonic
    async with _refresh_lock:
        now = time.monotonic()
        if _last_refresh_completed_monotonic and now - _last_refresh_completed_monotonic < MIN_REFRESH_INTERVAL_SECONDS:
            cached = cache.get("all_signals")
            if cached is not None:
                logger.info("Skipping duplicate signal refresh inside %.0fs guard", MIN_REFRESH_INTERVAL_SECONDS)
                return cached
        result = await asyncio.to_thread(_refresh_signals_and_release_memory)
        _last_refresh_completed_monotonic = time.monotonic()
        return result


async def scheduled_refresh() -> None:
    """Refresh only during an exchange-open session."""
    global _scheduler_heartbeat_at_ist
    _scheduler_heartbeat_at_ist = now_ist().isoformat()
    if not is_market_open():
        return
    try:
        await refresh_signals()
    except Exception:
        logger.exception("Scheduled signal refresh failed")


async def scheduled_history_rollover() -> None:
    """Archive prior-day UI history at 00:05 IST and retain a local archive."""
    now = now_ist()
    try:
        archived = await asyncio.to_thread(
            repository.archive_previous_history,
            ist_trade_date(now),
            settings.history_retention_days,
        )
        logger.info("Daily history rollover archived %s event(s)", archived)
    except Exception:
        logger.exception("Daily history rollover failed")


async def scheduled_market_close() -> None:
    """Grade and close unresolved current-day signal events after close.

    Before grading, one last public-feed poll refreshes tracked prices for
    unresolved symbols, so the WIN/LOSS/FLAT verdict reflects the ~15:30
    market instead of the last time a symbol happened to be published.
    """
    try:
        observed_at = now_ist()
        trade_date = ist_trade_date(observed_at)
        symbols = await asyncio.to_thread(repository.unresolved_event_symbols, trade_date)
        # A delayed/manual invocation may run after the event's calendar day.
        # Catch up the newest unresolved session instead of silently leaving it
        # open; the normal 15:31 scheduler still uses the current IST date.
        if not symbols:
            pending_date = await asyncio.to_thread(repository.latest_unresolved_event_trade_date)
            if pending_date and pending_date != trade_date:
                trade_date = pending_date
                observed_at = datetime.fromisoformat(
                    f"{trade_date}T15:31:00"
                ).replace(tzinfo=IST)
                symbols = await asyncio.to_thread(
                    repository.unresolved_event_symbols, trade_date
                )
        refreshed = 0
        if symbols:
            try:
                rows = await asyncio.to_thread(fetch_all_fno_oi_change)
                wanted = set(symbols)
                final_prices = {
                    str(row.get("symbol") or "").upper(): float(row.get("underlyingValue")
                        or row.get("lastPrice") or row.get("ltp") or 0)
                    for row in rows
                    if str(row.get("symbol") or "").upper() in wanted
                    and float(row.get("underlyingValue") or row.get("lastPrice")
                              or row.get("ltp") or 0) > 0
                }
                refreshed = await asyncio.to_thread(
                    repository.refresh_event_prices, final_prices, observed_at,
                )
            except Exception:
                logger.warning("Final-price refresh before close failed; grading with tracked prices")
        expired = await asyncio.to_thread(lambda: repository.expire_open_events(observed_at, result_source="live_exit"))
        logger.info(
            "Market-close processing expired %s event(s) (refreshed %s price(s))",
            expired, refreshed,
        )
    except Exception:
        logger.exception("Market-close processing failed")


async def scheduled_holiday_calendar_refresh() -> None:
    """Refresh NSE's public F&O calendar; bundled dates stay as fallback."""
    result = await asyncio.to_thread(refresh_holiday_calendar)
    if result["updated"]:
        logger.info("NSE holiday calendar refreshed for %s", result["years"])
    else:
        logger.warning("NSE holiday calendar refresh failed; using bundled dates")


async def ingest_daily_bhavcopy(trade_date: str | None = None) -> int:
    """Collect one public NSE daily equity file and persist normalized bars."""
    target_date = datetime.fromisoformat(trade_date).date() if trade_date else now_ist().date()
    try:
        bars = await asyncio.to_thread(collect_equity_bhavcopy, target_date)
        universe = await asyncio.to_thread(bundled_fno_symbols)
        if not universe:
            logger.warning("Daily ingestion skipped for %s: F&O universe unavailable (fail-closed)", target_date.isoformat())
            return 0
        bars = [bar for bar in bars if str(bar.get("symbol") or "").upper() in universe]
        stored = await asyncio.to_thread(repository.upsert_daily_equity_bars, bars)
        logger.info("Stored %s daily NSE equity bars for %s", stored, target_date.isoformat())
        return stored
    except Exception:
        logger.exception("Daily NSE bhavcopy ingestion failed for %s", target_date.isoformat())
        return 0


async def ingest_daily_index_bars() -> int:
    """Append the latest public NSE daily OHLC rows for the four F&O indices."""
    try:
        result = await asyncio.to_thread(backfill_index_bars, repository, days=2, max_downloads=2, angel_client=angel_market_data)
        return int(result.get("stored", 0))
    except Exception:
        logger.exception("Daily NSE index-bar ingestion failed")
        return 0


async def scheduled_bhavcopy_ingestion() -> None:
    """Ingest daily data, re-grade estimated exits with official closes, snapshot."""
    stored = await ingest_daily_bhavcopy()
    await ingest_daily_index_bars()
    if stored:
        try:
            # Regrade the date actually ingested. This also makes delayed or
            # manually replayed ingestion repair the latest stored session.
            trade_date = repository.latest_daily_equity_trade_date() or ist_trade_date(now_ist())
            regraded = await asyncio.to_thread(repository.regrade_with_bhavcopy_close, trade_date)
            if regraded:
                logger.info("Bhavcopy re-grade updated %s day-end result(s) for %s", regraded, trade_date)
        except Exception:
            logger.exception("Bhavcopy close re-grade failed")
    await asyncio.to_thread(upload_database_snapshot, settings.database_path)


def _backfill_end_date() -> date:
    """Use the latest expected published bhavcopy date as the backfill boundary."""
    return expected_latest_bhavcopy_date()


async def run_backfill(*, required_days: int = 60, max_downloads: int = 60) -> dict[str, object]:
    started = time.monotonic()
    started_at = now_ist().isoformat()
    symbols = await asyncio.to_thread(bundled_fno_symbols)
    if not symbols:
        # Fail closed: never fall back to storing all ~2,600 equities.
        logger.warning("Backfill skipped: F&O universe unavailable")
        return {"requested": 0, "downloaded": 0, "stored": 0, "skipped": 0, "failed": 0,
                "skipped_reason": "fno_universe_unavailable",
                "started_at_ist": started_at, "duration_seconds": 0.0,
                "daily_equity_data": repository.daily_equity_bar_summary()}
    async with _backfill_lock:
        result = await asyncio.to_thread(
            backfill_recent_bhavcopies,
            repository,
            end_date=_backfill_end_date(),
            required_days=required_days,
            max_downloads=max_downloads,
            symbols=symbols,
        )
    result.update({
        "started_at_ist": started_at,
        "duration_seconds": round(time.monotonic() - started, 2),
        "daily_equity_data": repository.daily_equity_bar_summary(),
    })
    return result


async def automatic_startup_backfill() -> None:
    if not settings.startup_backfill:
        return
    summary = repository.daily_equity_bar_summary()
    if not bhavcopy_backfill_required(summary):
        return
    logger.info("Daily bhavcopy is missing recent dates; automatic bounded backfill is starting. Monitor /api/health.")
    current = now_ist()
    minutes = current.hour * 60 + current.minute
    in_market_hours = current.weekday() < 5 and 540 <= minutes <= 945
    # During market hours fetch only the newest 16 dates (enough for ATR14);
    # the rest is topped up after the close to protect the 512 MB instance.
    limit = min(16, settings.backfill_target_days) if in_market_hours else settings.backfill_target_days
    try:
        result = await run_backfill(required_days=settings.backfill_target_days, max_downloads=limit)
        logger.info("Automatic bhavcopy backfill finished (market_hours=%s cap=%d): %s", in_market_hours, limit, result)
    except Exception:
        logger.exception("Automatic bhavcopy backfill failed")


async def scheduled_durable_snapshot() -> None:
    """Persist today's working database during market hours."""
    memory_watchdog()
    maybe_fill_feed_gaps()
    current = now_ist()
    minutes = current.hour * 60 + current.minute
    if current.weekday() < 5 and 540 <= minutes <= 945:
        await asyncio.to_thread(upload_database_snapshot, settings.database_path)


async def startup_universe_maintenance() -> None:
    """Load the real F&O universe and drop non-F&O bars (fail-closed, background)."""
    try:
        symbols = await asyncio.to_thread(bundled_fno_symbols)
        if not symbols:
            logger.warning("Bar purge skipped: F&O universe unavailable")
            return
        result = await asyncio.to_thread(repository.purge_non_fno_bars, symbols)
        logger.info("F&O universe applied source=%s symbols=%d purge=%s", universe_source(), len(symbols), result)
    except Exception:
        logger.exception("F&O universe maintenance failed")


async def scheduled_backfill_topup() -> None:
    """After the close: top up missing history to the full target (cheap when complete)."""
    if not settings.startup_backfill:
        return
    try:
        result = await run_backfill(required_days=settings.backfill_target_days, max_downloads=settings.backfill_target_days)
        logger.info("Post-close bhavcopy top-up finished: %s", result)
    except Exception:
        logger.exception("Post-close bhavcopy top-up failed")


_gap_fill: dict = {"running": False, "last_start": None, "symbols": [], "result": None, "gave_up": set()}


def maybe_fill_feed_gaps() -> bool:
    """Give bars to F&O names the OI feed reports but the Angel-derived universe lacks.

    Renamed/demerged stocks (e.g. TMPV/TMCV) would otherwise fail the gate forever with
    ``no_daily_bars``. Runs in a background thread, at most once per 20 minutes.
    """
    if not render_startup_backfill_enabled():
        return False
    last = _gap_fill["last_start"]
    if _gap_fill["running"] or _backfill_lock.locked() or (last is not None and time.monotonic() - last < 1200):
        return False
    universe = cached_universe()
    if not universe:
        return False
    extras = set(oi_engine._feed_symbols) - universe
    missing = set(repository.symbols_missing_bars(universe | extras, 15)) - _gap_fill["gave_up"]
    if not missing:
        return False
    _gap_fill.update(running=True, last_start=time.monotonic(), symbols=sorted(missing)[:20])

    def worker() -> None:
        try:
            add_extra_symbols(extras)
            _gap_fill["result"] = backfill_symbol_gaps(repository, missing, end_date=_backfill_end_date(), dates=20)
            _gap_fill["gave_up"].update(repository.symbols_missing_bars(missing, 15))     # no bhavcopy rows exist: stop retrying
            logger.info("Feed gap-fill finished symbols=%s result=%s gave_up=%s", sorted(missing)[:20], _gap_fill["result"], sorted(_gap_fill["gave_up"]))
        except Exception:
            logger.exception("Feed gap-fill failed")
        finally:
            _gap_fill["running"] = False

    threading.Thread(target=worker, daemon=True, name="feed-gap-fill").start()
    return True


def _current_rss_mb() -> float | None:
    try:
        with open("/proc/self/statm") as handle:
            return round(int(handle.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 1048576, 2)
    except Exception:
        return None


def memory_watchdog() -> float | None:
    """Warn (and collect garbage) when RSS nears Render Free's 512 MB limit."""
    rss = _current_rss_mb()
    limit = float(os.getenv("MEMORY_WARN_MB", "430"))
    if rss is not None and rss > limit:
        logger.warning("MEMORY_HIGH rss_mb=%.1f warn_mb=%.0f; running gc", rss, limit)
        gc.collect()
    return rss


_data_quality: dict[str, int] = {"candle_ok": 0, "candle_empty": 0, "candle_fail": 0, "quote_ctx": 0}
_gate_stats: dict = {"passed": 0, "failed": 0, "top_missing": {}, "at": None}
_bias_cache: tuple[float, str] = (0.0, "UNKNOWN")


def _market_bias_cached() -> str:
    """Nifty 5-minute regime (BULL/BEAR/NEUTRAL/UNKNOWN), cached 90 s; soft input only."""
    global _bias_cache
    if time.monotonic() - _bias_cache[0] < 90:
        return _bias_cache[1]
    bias = "UNKNOWN"
    if angel_market_data is not None:
        try:
            quotes = angel_market_data.full_quotes(["NIFTY"])
            bias = bias_from_quote(next(iter(quotes.values()))) if quotes else "UNKNOWN"
        except Exception:
            bias = "UNKNOWN"
    if angel_market_data is not None and bias == "UNKNOWN":
        try:
            bias = bias_from_candles(angel_market_data.intraday_candles("NIFTY", interval="FIVE_MINUTE", exchange="NSE", days=1))
        except Exception as exc:
            logger.warning("Nifty regime unavailable; treating as UNKNOWN (%s)", str(exc)[:120])
    _bias_cache = (time.monotonic(), bias)
    return bias


def _merge_bars(equity: dict, index: dict) -> dict:
    """Combine equity and index bars WITHOUT letting empty index lists erase stock history.

    daily_index_bars_for_symbols() returns ``{symbol: []}`` for every requested symbol, so a
    plain dict.update() blanked every stock's bars (ATR/EMA/volume were never available).
    """
    merged = dict(equity)
    for symbol, bars in index.items():
        if bars:
            merged[symbol] = bars
        else:
            merged.setdefault(symbol, [])
    return merged


def _apply_confirmation_gate(signals: list[dict], bars_by_symbol: dict) -> list[dict]:
    """Attach the transparent confirmation gate to every candidate (fail closed)."""
    now = now_ist()
    today = now.date().isoformat()
    banned = banned_symbols()
    bias = _market_bias_cached()
    out, missing_counts, failed_symbols = [], {}, {}
    for signal in signals:
        symbol = str(signal.get("symbol") or "")
        tech = signal.get("technical_context") or {}
        bars = [bar for bar in bars_by_symbol.get(symbol, []) if str(bar.get("trade_date")) != today]
        gate = evaluate_gate(
            signal, oi_ctx=signal.get("oi_window") or {}, intraday=signal.get("intraday_context"),
            atr14=tech.get("atr14"), avg_volume=average_daily_volume(bars),
            prev_close=float(bars[-1]["close"]) if bars else None,
            ema20=tech.get("ema20"), ema50=tech.get("ema50"),
            banned=symbol.upper() in banned, market_bias=bias, now=now,
            has_bars=bool(bars), is_index=symbol.upper() in INDEX_SYMBOLS,
        )
        if gate["missing_confirmations"]:
            failed_symbols[symbol] = gate["missing_confirmations"][:4]
        for reason in gate["missing_confirmations"]:
            missing_counts[reason] = missing_counts.get(reason, 0) + 1
        merged = {**signal, **gate}
        try:
            # The exact entry/SL/targets that will be tracked, so the table and the tracked trade agree.
            _payload, plan = repository._event_payload(merged, now)
            merged["plan"] = {key: plan[key] for key in ("entry", "stop_loss", "target_1", "target_2", "risk_reward", "source")}
        except Exception:
            logger.debug("Could not attach trade plan to %s", symbol, exc_info=True)
        out.append(merged)
    passed = sum(1 for item in out if item.get("actionable"))
    _gate_stats.update(passed=passed, failed=len(out) - passed, top_missing=dict(sorted(missing_counts.items(), key=lambda kv: -kv[1])[:6]), failed_symbols=dict(list(failed_symbols.items())[:10]), at=now.isoformat(timespec="seconds"))
    return out


async def scheduled_ban_refresh() -> None:
    """Daily F&O ban list; hard 90 s cap so a hung fetch is always reported."""
    try:
        await asyncio.wait_for(asyncio.to_thread(refresh_ban_list), timeout=90)
    except asyncio.TimeoutError:
        logger.warning("FNO_BAN_LIST_UNAVAILABLE: refresh timed out after 90 s")
    except Exception:
        logger.exception("F&O ban list refresh failed")


_gap_cache: tuple[float, list[str]] = (0.0, [])


def _universe_gap() -> list[str]:
    """F&O symbols lacking enough daily bars (cached 5 min so /api/health stays fast)."""
    global _gap_cache
    if time.monotonic() - _gap_cache[0] < 60:
        return _gap_cache[1]
    universe = cached_universe()
    missing = repository.symbols_missing_bars(universe, 15) if universe else []
    _gap_cache = (time.monotonic(), missing)
    return missing


def _self_test_probes(stage: str):
    return self_test.build_probes(
        stage, angel=angel_market_data, repository=repository, universe=cached_universe, ban_info=ban_info,
        scan_stats=lambda: oi_engine._last_scan_stats, window_depth=oi_engine.oi_window.depth,
    )


async def scheduled_self_test(stage: str) -> None:
    """09:05 (pre-open) and 09:35 (post-open) IST: prove every data source works, loudly."""
    if is_trading_holiday(now_ist().date()) is True:
        return
    if angel_market_data is None:
        logger.warning("SELF_TEST stage=%s skipped: Angel One not configured", stage)
        return
    try:
        await asyncio.to_thread(self_test.run_stage, stage, _self_test_probes(stage), now_ist())
    except Exception:
        logger.exception("Self-test crashed (stage=%s)", stage)


async def scheduled_trade_monitor() -> None:
    """Every 30 s in market hours: check EVERY open paper trade against live prices.

    Independent of the signal list and of dashboard visits, so target/stop/breakeven are
    caught even after a symbol drops off the published signals.
    """
    global _scheduler_heartbeat_at_ist
    now = now_ist()
    _scheduler_heartbeat_at_ist = now.isoformat()
    minutes = now.hour * 60 + now.minute
    if now.weekday() >= 5 or not (9 * 60 + 15 <= minutes <= 15 * 60 + 15):
        return
    try:
        symbols = await asyncio.to_thread(repository.unresolved_event_symbols, now.date().isoformat())
        if not symbols:
            return
        prices: dict[str, float] = {}
        if angel_market_data is not None:
            try:
                quotes = await asyncio.to_thread(angel_market_data.full_quotes, list(symbols))
                prices = {s: float(q.get("ltp") or 0) for s, q in quotes.items() if float(q.get("ltp") or 0) > 0}
            except Exception as exc:
                logger.info("Trade monitor: Angel quotes unavailable (%s); using scan prices", str(exc)[:80])
        for symbol in symbols:
            if symbol not in prices:
                last = oi_engine.oi_window.last_price(symbol, now)
                if last:
                    prices[symbol] = last
        closed = await asyncio.to_thread(repository.update_open_events, [], now, prices)
        if closed:
            logger.info("Trade monitor closed %d paper trade(s)", closed)
    except Exception:
        logger.exception("Trade monitor failed")


def render_startup_backfill_enabled() -> bool:
    """Only run automatic backfill on Render; local/dev uses the HTTP trigger."""
    return bool(os.getenv("RENDER") or os.getenv("RENDER_SERVICE_ID"))


# ?? Background poller ?????????????????????????????????????????????????????????

async def background_poller():
    """Re-scan all F&O stocks every 60s during market hours."""
    # First scan: wait 15s for session to fully initialise, then scan immediately
    await asyncio.sleep(15)
    if is_market_open():
        logger.info("Market open ? initial scan?")
        try:
            await asyncio.to_thread(_refresh_signals_and_release_memory)
        except Exception as e:
            logger.error(f"Initial scan error: {e}")

    while True:
        await asyncio.sleep(settings.poll_interval_seconds)
        if is_market_open():
            logger.info("Polling ? scanning F&O stocks?")
            try:
                await asyncio.to_thread(_refresh_signals_and_release_memory)
            except Exception as e:
                logger.error(f"Poll scan error: {e}")


# ?? Lifecycle ?????????????????????????????????????????????????????????????????

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _startup_state, _startup_ready_at_ist, _startup_error
    global _scheduler_started_at_ist, _scheduler_heartbeat_at_ist
    logger.info(f"NSE OI Tracker v{APP_VERSION} starting")
    _startup_state = "STARTING"
    _startup_error = None
    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(
        scheduled_refresh,
        IntervalTrigger(seconds=settings.poll_interval_seconds, timezone=IST),
        id="nse-signal-refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        scheduled_history_rollover,
        CronTrigger(hour=0, minute=5, timezone=IST),
        id="daily-history-rollover",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        scheduled_market_close,
        CronTrigger(day_of_week="mon-fri", hour=15, minute=15, timezone=IST),
        id="market-close-expiry",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        scheduled_holiday_calendar_refresh,
        CronTrigger(day_of_week="sun", hour=7, minute=0, timezone=IST),
        id="nse-holiday-calendar-refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        scheduled_bhavcopy_ingestion,
        CronTrigger(day_of_week="mon-fri", hour=18, minute=10, timezone=IST),
        id="nse-daily-bhavcopy-ingestion",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        scheduled_durable_snapshot,
        IntervalTrigger(minutes=settings.snapshot_every_min, timezone=IST),
        id="durable-database-snapshot",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    for _stage, _hour, _minute in (("pre_open", 9, 5), ("post_open", 9, 35)):
        scheduler.add_job(
            scheduled_self_test, CronTrigger(day_of_week="mon-fri", hour=_hour, minute=_minute, timezone=IST),
            args=[_stage], id=f"self-test-{_stage}", replace_existing=True, max_instances=1, coalesce=True,
        )
    scheduler.add_job(
        scheduled_trade_monitor, IntervalTrigger(seconds=30, timezone=IST), id="trade-monitor",
        replace_existing=True, max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        scheduled_ban_refresh,
        CronTrigger(day_of_week="mon-fri", hour=8, minute=50, timezone=IST),
        id="fno-ban-refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        scheduled_backfill_topup,
        CronTrigger(day_of_week="mon-fri", hour=16, minute=5, timezone=IST),
        id="post-close-bhavcopy-topup",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    app.state.scheduler = scheduler
    _scheduler_started_at_ist = now_ist().isoformat()
    _scheduler_heartbeat_at_ist = _scheduler_started_at_ist
    gc.freeze()
    # Restore analytics before any market-data backfill; the restore is optional
    # and bounded, so an unavailable bucket never blocks application startup.
    if repository.daily_equity_bar_summary().get("bars", 0) == 0:
        await asyncio.to_thread(restore_latest_backup, settings.database_path)
    if repository.daily_equity_bar_summary().get("bars", 0) == 0:
        await asyncio.to_thread(restore_bundled_seed, settings.database_path)
    if render_startup_backfill_enabled():
        # Network work at startup only on Render (never in tests/dev).
        asyncio.create_task(startup_universe_maintenance())
        asyncio.create_task(scheduled_ban_refresh())
    # Render Free has an ephemeral filesystem; run one bounded backfill without
    # blocking health/startup. The deployment setting controls whether it runs.
    if bhavcopy_backfill_required(repository.daily_equity_bar_summary()):
        if settings.startup_backfill and render_startup_backfill_enabled():
            asyncio.create_task(automatic_startup_backfill())
        else:
            logger.warning("Daily bhavcopy history is empty or stale and automatic backfill is disabled.")
    if repository.daily_index_bar_summary().get("bars", 0) == 0 and os.getenv("NSE_OI_INDEX_BACKFILL", "0").lower() not in {"0", "false", "no"}:
        asyncio.create_task(asyncio.to_thread(backfill_index_bars, repository, days=60, max_downloads=60, angel_client=angel_market_data))
    # A newly deployed year is unknown until NSE's public calendar loads.
    # Await only in that case: normal startup stays local and fast.
    if not has_holiday_calendar_for_year(now_ist().year):
        await scheduled_holiday_calendar_refresh()
    # Preserve an already-populated cache (important for warm restarts and
    # deterministic API tests); production still performs the initial refresh
    # whenever no current signal snapshot exists.
    if cache.get("all_signals") is None:
        await scheduled_refresh()
    _startup_state = "READY"
    _startup_ready_at_ist = now_ist().isoformat()
    logger.info("NSE OI Tracker startup READY scheduler=%s", _scheduler_started_at_ist)
    yield
    _startup_state = "STOPPING"
    try:
        await asyncio.wait_for(asyncio.to_thread(upload_database_snapshot, settings.database_path), timeout=10)
    except Exception:
        logger.warning("Shutdown snapshot skipped", exc_info=True)
    scheduler.shutdown(wait=False)


# ?? FastAPI app ???????????????????????????????????????????????????????????????

app = FastAPI(
    title="NSE F&O OI Scanner",
    description="Real-time OI signal scanner",
    version=APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    # The shipped UI is same-origin. An external dashboard must be opted in
    # through NSE_OI_CORS_ORIGINS rather than using a browser-wide wildcard.
    allow_origins=list(settings.cors_origins),
    allow_methods=["GET", "HEAD"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ?? Routes ????????????????????????????????????????????????????????????????????

@app.get("/", include_in_schema=False)
async def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.api_route("/api/health", methods=["GET", "HEAD"])
async def health():
    """Health check ? GET and HEAD supported (UptimeRobot uses HEAD)."""
    now = now_ist()
    persisted_snapshot = repository.latest_snapshot_metadata()
    effective_last_scan_at = _last_refresh_at_ist or (persisted_snapshot or {}).get("captured_at_ist")
    effective_snapshot_id = _last_snapshot_id or (persisted_snapshot or {}).get("id")
    status = get_market_status(now)
    daily_equity_data = repository.daily_equity_bar_summary()
    daily_index_data = repository.daily_index_bar_summary()
    scan_age = max(0.0, time.monotonic() - _last_refresh_completed_monotonic) if _last_refresh_completed_monotonic else None
    coverage = repository.daily_history_coverage(symbols=cached_universe() or None)
    angel_state = angel_market_data.health() if angel_market_data is not None else {"state": "disabled", "last_error_code": "", "retry_at": None}
    return {
        "status":        "ok",
        "time_ist":      now.strftime("%Y-%m-%d %H:%M:%S IST"),
        "market_open":   status == MARKET_STATUS_OPEN,
        "market_status": status,
        "market_status_label": MARKET_STATUS_LABELS[status],
        "version":       APP_VERSION,
        "build_sha":     os.getenv("RENDER_GIT_COMMIT", "unknown"),
        "started_at":    _started_at.isoformat(),
        "uptime_s":      round(max(0.0, (now - _started_at).total_seconds()), 2),
        "data_source":   oi_engine._last_scan_stats.get("oi_source") or "no_scan_yet",
        "price_source":  "angel_one_quotes" if angel_market_data else "nse_oi_feed",
        "last_scan_at":  effective_last_scan_at,
        "scan_age_s":    round(scan_age, 2) if scan_age is not None else None,
        "atr_coverage_pct": coverage["atr_coverage_pct"],
        "history_ready_pct": coverage["history_ready_pct"],
        "snapshot_age_s": last_snapshot_age_s(),
        "snapshot_backend": "github" if os.getenv("NSE_OI_BACKUP_GITHUB_REPO") and os.getenv("NSE_OI_BACKUP_GITHUB_TOKEN") else "url" if os.getenv("NSE_OI_BACKUP_URL") else "none",
        "angel":         angel_state,
        "database":       "ready",
        "memory_rss_mb":  round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2) if resource else 0.0,
        "memory_rss_peak_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2) if resource else 0.0,
        "memory_rss_current_mb": _current_rss_mb(),
        "data_quality": {
            "rows_last_scan": oi_engine._last_scan_stats.get("rows"),
            "candidates_last_scan": oi_engine._last_scan_stats.get("candidates"),
            "oi_window": oi_engine.oi_window.depth(),
            **_data_quality,
            "market_bias": _bias_cache[1],
            **ban_info(),
            "gate_last_scan": _gate_stats,
            "self_test": self_test.latest(),
            "gap_fill": {"running": _gap_fill["running"], "symbols": _gap_fill["symbols"], "result": _gap_fill["result"]},
            "universe_missing_bars": _universe_gap()[:40],
        },
        "readiness": self_test.readiness() if self_test.readiness() != "UNTESTED" else _startup_state,
        "startup_state": _startup_state,
        "startup_ready_at_ist": _startup_ready_at_ist,
        "startup_error": _startup_error,
        "scheduler_started_at_ist": _scheduler_started_at_ist,
        "scheduler_heartbeat_at_ist": _scheduler_heartbeat_at_ist,
        "fno_universe_source": universe_source(),
        "fno_universe_size": cached_universe_size(),
        **last_snapshot_info(),
        "last_refresh_at_ist": _last_refresh_at_ist or effective_last_scan_at,
        "last_refresh_was_stale": _last_refresh_was_stale,
        "last_snapshot_id": effective_snapshot_id,
        "holiday_calendar": holiday_calendar_metadata(),
        "daily_equity_data": daily_equity_data,
        "bhavcopy_backfill_required": bhavcopy_backfill_required(daily_equity_data, now),
        "daily_index_data": daily_index_data,
        "index_backfill_required": daily_index_data.get("bars", 0) == 0,
    }


@app.post("/api/admin/backfill")
async def admin_backfill(
    days: int = Query(60, ge=1, le=120),
    max_downloads: int = Query(60, ge=1, le=120),
    x_debug_token: str = Header(default=""),
):
    """Run one bounded daily-bar backfill without requiring Render Shell access."""
    if not DEBUG_TOKEN:
        raise HTTPException(status_code=404, detail="Backfill endpoint is disabled")
    if x_debug_token != DEBUG_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Debug-Token header")
    if _backfill_lock.locked():
        raise HTTPException(status_code=409, detail="Backfill is already running")
    res = await run_backfill(required_days=days, max_downloads=max_downloads)
    try:
        await ingest_daily_index_bars()
    except Exception:
        logger.exception("Index ingestion during admin backfill failed")
    return res


@app.post("/api/admin/self-test")
async def admin_self_test(stage: str = Query("pre_open"), x_debug_token: str = Header(default="")):
    """Run a self-test stage now (same auth as the other admin endpoint)."""
    if not DEBUG_TOKEN or x_debug_token != DEBUG_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Debug-Token header")
    if stage not in ("pre_open", "post_open"):
        raise HTTPException(status_code=400, detail="stage must be pre_open or post_open")
    if angel_market_data is None:
        raise HTTPException(status_code=409, detail="Angel One is not configured")
    return await asyncio.to_thread(self_test.run_stage, stage, _self_test_probes(stage), now_ist())


@app.get("/api/sources")
async def sources():
    """Declared public-data coverage; unavailable sources are never implied live."""
    return {
        "sources": public_source_inventory(),
        "angel_one_market_data": {
            "configured": angel_market_data is not None,
            "mode": "read_only_quotes_and_candles" if angel_market_data else "disabled",
            "order_execution": False,
            "health": angel_market_data.health() if angel_market_data else {"state": "disabled", "last_error_code": "", "retry_at": None},
        },
        "policy": "Only public/free sources are used. A NOT_CONFIGURED source is not silently substituted or inferred.",
    }


@app.get("/api/oi-signals")
async def oi_signals(
    refresh:      bool  = Query(False, description="Force fresh NSE fetch"),
    signal:       str   = Query("",    description="Filter by signal type"),
    tier:         str   = Query("",    description="Filter by tier: HIGH | MEDIUM (MEDIUM is diagnostic only)"),
    sector:       str   = Query("",    description="Curated display sector; unknown symbols are Unclassified"),
    min_strength: float = Query(0,     description="Min strength score"),
):
    """
    Scan ALL NSE F&O stocks. Returns only high-confidence, liquid signals.

    Medium and low-confidence classifications are retained for diagnostics but
    are intentionally excluded from the live signal list to reduce noise.

    Data source: /api/live-analysis-oi-spurts-underlyings (confirmed working)
    Signal classification: price direction ? OI direction ? 4 signal types
    """
    if refresh:
        cache.delete("all_signals")

    cached = cache.get("all_signals")

    # Only a missing cache needs a blocking scan. An empty list is a valid,
    # successful quiet-market result; refreshing it on every request made the
    # dashboard wait for another full NSE round-trip whenever no signal passed.
    if cached is None and is_market_open():
        logger.info("Signal cache missing during market hours - fresh scan")
        cached = await refresh_signals()

    if cached is None:
        cached = []

    results = list(cached)

    # Optional filters
    if signal:
        results = [r for r in results if r["signal"] == signal.upper()]
    if tier:
        results = [r for r in results if r["confidence_tier"] == tier.upper()]
    if sector:
        results = [r for r in results if str(r.get("sector") or "Unclassified").casefold() == sector.strip().casefold()]
    if min_strength > 0:
        results = [r for r in results if r["strength"] >= min_strength]

    # Count per signal type
    counts = {}
    for r in cached:
        counts[r["signal"]] = counts.get(r["signal"], 0) + 1

    high   = sum(1 for r in cached if r.get("confidence_tier") == "HIGH")
    medium = sum(1 for r in cached if r.get("confidence_tier") == "MEDIUM")
    status = get_market_status()
    active_source = (
        "angel_one_read_only_overlay"
        if any(r.get("realtime_source") == "angel_one_read_only" for r in cached)
        else "nse_public_feed"
    )
    data_status = last_scan_data_status()

    return {
        "market_open":       status == MARKET_STATUS_OPEN,
        "market_status":     status,
        "market_status_label": MARKET_STATUS_LABELS[status],
        "total_fno_active":  len(cached),
        "high_confidence":   high,
        "medium_confidence": medium,
        "filtered_count":    len(results),
        "signal_counts":     counts,
        "signal_meta":       SIGNAL_META,
        "available_sectors": known_sectors(),
        "signals":           results,
        "data_status":       data_status,
        "data_warning": (
            "NSE upstream returned no OI rows; no signal can be computed right now."
            if data_status == "UNAVAILABLE" and not results else None
        ),
        "primary_market_data_source": active_source,
        "oi_source": oi_engine._last_scan_stats.get("oi_source") or "no_scan_yet",
        "angel_one_configured": angel_market_data is not None,
        "nse_public_feed_is_fallback": active_source == "nse_public_feed" and angel_market_data is not None,
        "refresh_supported": True,
        "timestamp":         now_ist().strftime("%H:%M:%S"),
        "last_refresh_at_ist": _last_refresh_at_ist,
        "is_stale":          _last_refresh_was_stale,
        "snapshot_id":       _last_snapshot_id,
    }


@app.get("/api/history/today")
async def today_history(
    limit: int = Query(1000, ge=1, le=5000, description="Maximum number of current-day events"),
):
    """Return server-owned signal events visible for the current IST date."""
    trade_date = ist_trade_date()
    events, _total = await asyncio.to_thread(repository.history_for_date, trade_date, limit=limit)
    events = [
        event for event in events
        if (event.get("tier") or "TRADE") in {"TRADE", "CANDIDATE"}
        and int(event.get("confidence") or 0) >= 75
    ]
    performance = await asyncio.to_thread(repository.performance_for_date, trade_date)
    watch_performance = await asyncio.to_thread(repository.performance_for_date, trade_date, "WATCH")
    return {
        "trade_date": trade_date,
        "visible_history_scope": "today_ist",
        "reset_policy": "Previous-day events are archived at 00:05 IST.",
        "total_events": len(events),
        "events": events,
        "performance": performance,
        "watch_performance": watch_performance,
        "timestamp": now_ist().strftime("%Y-%m-%d %H:%M:%S IST"),
    }


@app.get("/api/analytics/today")
async def today_analytics():
    """Return server-calculated daily performance for durable signal events."""
    trade_date = ist_trade_date()
    events = await asyncio.to_thread(repository.backtest_events, trade_date, trade_date)
    return {
        "trade_date": trade_date,
        "metrics": await asyncio.to_thread(repository.performance_for_date, trade_date),
        "watch_metrics": await asyncio.to_thread(repository.performance_for_date, trade_date, "WATCH"),
        "breakdowns": summarize_candidate_backtest(events),
        "timestamp": now_ist().strftime("%Y-%m-%d %H:%M:%S IST"),
    }


def _validated_backtest_dates(start_date: str, end_date: str) -> tuple[str, str]:
    try:
        start = datetime.fromisoformat(start_date).date()
        end = datetime.fromisoformat(end_date).date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Dates must use YYYY-MM-DD") from exc
    if start > end:
        raise HTTPException(status_code=400, detail="from_date must be on or before to_date")
    if (end - start).days > 366:
        raise HTTPException(status_code=400, detail="Backtest range is limited to 366 days")
    return start.isoformat(), end.isoformat()


@app.get("/api/backtest")
async def backtest(
    from_date: str = Query(..., description="IST start date, YYYY-MM-DD"),
    to_date: str = Query(..., description="IST end date, YYYY-MM-DD"),
):
    """Analyze stored candidate events; does not claim strategy performance."""
    start, end = _validated_backtest_dates(from_date, to_date)
    events = await asyncio.to_thread(repository.backtest_events, start, end)
    return {
        "from_date": start,
        "to_date": end,
        "metrics": summarize_candidate_backtest(events),
    }


@app.get("/api/backtest/export.csv")
async def backtest_export(
    from_date: str = Query(..., description="IST start date, YYYY-MM-DD"),
    to_date: str = Query(..., description="IST end date, YYYY-MM-DD"),
):
    """Export the underlying stored candidate-event rows for audit/recalculation."""
    start, end = _validated_backtest_dates(from_date, to_date)
    events = await asyncio.to_thread(repository.backtest_events, start, end)
    fields = [
        "captured_at_ist", "trade_date", "symbol", "signal", "direction", "confidence",
        "entry", "stop_loss", "target_1", "target_2", "risk_reward", "risk_source",
        "current_price", "exit_price", "max_target_hit", "status", "result", "sector",
    ]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(events)
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="nse-oi-candidates-{start}-to-{end}.csv"'},
    )


@app.get("/api/market-overview")
async def market_overview(refresh: bool = Query(False)):
    """Cached NSE indices, VIX and breadth (context only)."""
    cache_key = "market-overview"
    if refresh:
        cache.delete(cache_key)
    cached = cache.get(cache_key)
    if cached is not None:
        return {"cached": True, **cached}
    indices = await asyncio.to_thread(fetch_market_indices)
    result = normalize_market_overview(indices, [])
    cache.set(cache_key, result, ttl=settings.cache_ttl_seconds)
    return {"cached": False, **result}


@app.get("/api/debug")
async def debug(x_debug_token: str = Header(default="")):
    """
    Diagnostic endpoint ? NSE connectivity + raw sample data.

    Gated by DEBUG_TOKEN env var. This endpoint exposes raw upstream
    payloads and internal field-mapping state; leaving it wide open on a
    public deployment is fine for personal debugging but not something to
    call "production standard". Set DEBUG_TOKEN in Render's env vars to
    lock it down ? until you do, it stays open and says so explicitly.
    """
    if not DEBUG_TOKEN:
        raise HTTPException(status_code=404, detail="Diagnostic endpoint is disabled")
    if x_debug_token != DEBUG_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Debug-Token header")

    conn   = await asyncio.to_thread(test_nse_connectivity)
    cached = cache.get("all_signals") or []

    # Get raw rows to inspect field names
    raw_rows = await asyncio.to_thread(fetch_all_fno_oi_change)
    sample   = raw_rows[:3] if raw_rows else []

    return {
        "timestamp":        now_ist().strftime("%Y-%m-%d %H:%M:%S IST"),
        "market_status":    get_market_status(),
        "version":          APP_VERSION,
        "auth_protected":   bool(DEBUG_TOKEN),
        "cached_signals":   len(cached),
        "raw_rows_count":   len(raw_rows),
        "nse_endpoints":    conn,
        "sample_row":       sample[0] if sample else {},   # ? shows real field names
        "sample_rows":      sample,
        "price_sources":    {r.get("symbol"): r.get("price_source") for r in sample},
        "oi_field_usage":   sample_field_usage(),
        "cas_time_ist":     oi_engine._last_cas_time_ist,
    }
