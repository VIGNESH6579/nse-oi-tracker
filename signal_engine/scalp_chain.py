"""Angel One stock-option scalping paper engine.

This module is deliberately separate from the legacy F&O OI swing scanner.
It has no order-placement code and no regime-as-entry dependency.
"""
from __future__ import annotations

import json
import logging
import os
import statistics
import time
import uuid
from datetime import datetime, time as dt_time
from typing import Any

from collector.fno_ban import banned_symbols, ban_info
from collector.universe import cached_universe, universe_source
from utils.time import now_ist
from integrations.angel_one_market_data import AngelOneMarketData, AngelUnavailable
from integrations.angel_one_stream import AngelOneMarketStream

logger = logging.getLogger(__name__)

MODE = "scalp_chain"
ENTRY_START = os.getenv("SCALP_ENTRY_START", os.getenv("ENTRY_START", "09:30"))
ENTRY_END = os.getenv("SCALP_ENTRY_END", os.getenv("ENTRY_END", "15:00"))
# Retained only to tolerate older environment files; duration is not an exit rule.
MAX_HOLD_MINUTES = int(os.getenv("SCALP_MAX_HOLD_MINUTES", os.getenv("MAX_HOLD_MINUTES", "10")))
TRAIL_DRAWDOWN_PCT = max(1.0, float(os.getenv("SCALP_TRAIL_DRAWDOWN_PCT", "12")))
ABSOLUTE_EXIT = os.getenv("SCALP_ABSOLUTE_EXIT", "15:15")
MAX_CONCURRENT = int(os.getenv("SCALP_MAX_OPEN", os.getenv("SCALP_MAX_CONCURRENT", "3")))
OPTION_STOP_PCT = float(os.getenv("SCALP_OPTION_STOP_PCT", "30"))
OPTION_TARGET_PCT = float(os.getenv("SCALP_OPTION_TARGET_PCT", "20"))
CHAIN_TTL_S = float(os.getenv("SCALP_CHAIN_TTL_S", "20"))
HIST_CANDLE_TTL_S = float(os.getenv("SCALP_HIST_CANDLE_TTL_S", "300"))
HIST_CANDLE_MAX_CALLS_PER_REFRESH = max(0, int(os.getenv("SCALP_HIST_CANDLE_MAX_CALLS_PER_REFRESH", "2")))
TOP_N = int(os.getenv("SCALP_CHAIN_TOP_N", os.getenv("SCALP_TOP_N", "5")))
RANK_TOP_N = int(os.getenv("SCALP_RANK_TOP_N", "20"))
MIN_OPTION_OI = int(os.getenv("SCALP_MIN_OPTION_OI", "10000"))
MIN_OPTION_VOLUME = int(os.getenv("SCALP_MIN_OPTION_VOLUME", "100"))
MOMENTUM_MIN_PCT = float(os.getenv("SCALP_MOMENTUM_MIN_PCT", "0.30"))
# Quality gates are intentionally conservative: fewer entries are preferable to
# breakouts without confirmed participation or option-side follow-through.
MIN_RELATIVE_VOLUME = max(1.0, float(os.getenv("SCALP_MIN_RELATIVE_VOLUME", "1.20")))
MIN_CANDLE_CLOSE_LOCATION = min(0.90, max(0.60, float(os.getenv("SCALP_MIN_CLOSE_LOCATION", "0.65"))))
MIN_BREAKOUT_BUFFER_PCT = max(0.0, float(os.getenv("SCALP_MIN_BREAKOUT_BUFFER_PCT", "0.05")))
MIN_PREMIUM_CHANGE_PCT = max(0.0, float(os.getenv("SCALP_MIN_PREMIUM_CHANGE_PCT", "0.25")))

def _clock(value: str) -> tuple[int, int]:
    hour, minute = value.strip().split(":", 1)
    return int(hour), int(minute)


def _in_window(now: datetime, start: str, end: str) -> bool:
    current = now.hour * 60 + now.minute
    sh, sm = _clock(start)
    eh, em = _clock(end)
    return sh * 60 + sm <= current <= eh * 60 + em


def _before(now: datetime, value: str) -> bool:
    h, m = _clock(value)
    return now.hour * 60 + now.minute < h * 60 + m


def _minutes_between(start_iso: str, end: datetime) -> float:
    start = datetime.fromisoformat(start_iso)
    return max(0.0, (end - start).total_seconds() / 60.0)


def _json_rows(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, separators=(",", ":"), default=str)


class ScalpChainEngine:
    def __init__(self, market_data: AngelOneMarketData | None, stream: AngelOneMarketStream, repository: Any) -> None:
        self.market_data = market_data
        self.stream = stream
        self.repository = repository
        self.universe: list[str] = []  # No hard-coded/configured stock-list fallback.
        self._chain_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._historical_candle_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._historical_fetches_this_refresh = 0
        self._historical_fetch_count = 0
        self._historical_fetch_failures = 0
        self._previous_chain: dict[str, dict[str, Any]] = {}
        self._open: dict[str, dict[str, Any]] = {}
        self._last_run_at = 0.0
        self._last_error = ""
        self._last_ban_warning_at = 0.0
        self._last_context_rejection = ""
        self._last_monitor_at_ist: str | None = None
        self._unpriced_close_count = 0
        self._last_exit_pnl_status: str | None = None
        self._last_run_stats: dict[str, Any] = {
            "candidates": 0, "chain_checked": 0, "chain_unavailable": 0,
            "confirmation_failed": 0, "entries": 0, "ban_list_unavailable": False,
            "top_rejections": {}, "at": None,
        }
        self._load_open_events()

    def health(self) -> dict[str, Any]:
        cache_now = time.monotonic()
        fresh_chain_cache = [
            stamp for stamp, _ in self._chain_cache.values()
            if 0 <= cache_now - stamp < CHAIN_TTL_S
        ]
        oldest_chain_age = (
            round(max(0.0, cache_now - min(stamp for stamp, _ in self._chain_cache.values())), 2)
            if self._chain_cache else None
        )
        return {
            "mode": MODE,
            "paper_only": True,
            "universe_size": len(cached_universe()),
            "fixed_universe_size": 0,
            "universe_source": universe_source() or ("unavailable" if not cached_universe() else "dynamic_registry"),
            "top_n": TOP_N,
            "rank_top_n": RANK_TOP_N,
            "ranking_basis": "live_intraday_pct_change",
            "open_scalps": len(self._open),
            "chain_cache_symbols": len(self._chain_cache),
            "chain_cache_fresh_symbols": len(fresh_chain_cache),
            "chain_cache_ttl_s": CHAIN_TTL_S,
            "chain_cache_oldest_age_s": oldest_chain_age,
            "chain_ok": bool(fresh_chain_cache),
            "historical_candle_cache_symbols": len(self._historical_candle_cache),
            "historical_fetch_count": self._historical_fetch_count,
            "historical_fetch_failures": self._historical_fetch_failures,
            "historical_fetches_this_refresh": self._historical_fetches_this_refresh,
            "last_error": self._last_error,
            "last_monitor_at_ist": self._last_monitor_at_ist,
            "unpriced_close_count": self._unpriced_close_count,
            "last_exit_pnl_status": self._last_exit_pnl_status,
            "stop_loss_pct": OPTION_STOP_PCT,
            "target_pct": OPTION_TARGET_PCT,
            "max_hold_minutes": None,
            "time_exit_enabled": False,
            "trailing_drawdown_pct": TRAIL_DRAWDOWN_PCT,
            "exit_policy": "momentum_weakness_or_protective_stop_or_1515_hard_flat",
            "entry_window": f"{ENTRY_START}-{ENTRY_END} IST",
            "last_run": self._last_run_stats,
        }

    def _load_open_events(self) -> None:
        try:
            rows = self.repository.open_scalps(now_ist().date().isoformat())
            for row in rows:
                self._open[str(row["symbol"]).upper()] = {
                    "id": int(row["id"]),
                    "symbol": str(row["symbol"]).upper(),
                    "direction": row["direction"],
                    "strike": float(row["strike"]),
                    "option_type": row["option_type"],
                    "expiry": row["expiry"],
                    "entry_time_ist": row["entry_time_ist"],
                    "underlying_entry": float(row["underlying_entry"]),
                    "entry_option_ltp": float(row["entry_option_ltp"]),
                    "chain_snapshot_id": row["chain_snapshot_id"],
                }
        except Exception:
            logger.exception("Could not restore open paper scalps")

    def reload_open_positions(self) -> None:
        """Re-read today's persisted paper positions after a database restore."""
        self._open.clear()
        self._load_open_events()

    def _chain(self, symbol: str, *, spot: float, force_refresh: bool = False) -> dict[str, Any] | None:
        if self.market_data is None or not self.market_data.configured:
            self._last_error = "angel_not_configured"
            return None
        if spot <= 0:
            self._last_error = "underlying_spot_unavailable"
            return None
        key = symbol.upper()
        cached = self._chain_cache.get(key)
        if not force_refresh and cached and time.monotonic() - cached[0] < CHAIN_TTL_S:
            return cached[1]
        try:
            chain = self.market_data.option_chain_near_atm(key, spot, strikes_each_side=max(0, min(2, int(os.getenv("SCALP_STRIKES_SIDE", "1")))))
            if not chain.get("ok"):
                self._last_error = str(chain.get("reason") or "chain_unavailable")
                return None
            rows: list[dict[str, Any]] = []
            for bucket in chain.get("chain") or []:
                strike = float(bucket.get("strike") or 0)
                for side in ("CE", "PE"):
                    leg = bucket.get("ce" if side == "CE" else "pe")
                    if not leg:
                        continue
                    rows.append({
                        "strike": strike,
                        "option_type": side,
                        "token": leg.get("token"),
                        "symbol": leg.get("symbol"),
                        "ltp": float(leg.get("ltp") or 0),
                        "oi": float(leg.get("oi") or 0),
                        "volume": float(leg.get("volume") or 0),
                        "trading_symbol": leg.get("trading_symbol"),
                    })
            if not rows:
                self._last_error = "chain_no_option_rows"
                return None
            chain["rows"] = rows
            self._chain_cache[key] = (time.monotonic(), chain)
            return chain
        except AngelUnavailable as exc:
            self._last_error = str(exc)[:120]
            return None
        except Exception as exc:
            self._last_error = type(exc).__name__
            logger.warning("Scalp option chain unavailable for %s: %s", key, type(exc).__name__)
            return None

    def _historical_context_candles(self, symbol: str) -> list[dict[str, Any]]:
        """Fetch today's real Angel 5-minute OHLCV as a bounded fallback for the WS opening range."""
        key = symbol.upper()
        now_mono = time.monotonic()
        cached = self._historical_candle_cache.get(key)
        if cached and now_mono - cached[0] < HIST_CANDLE_TTL_S:
            return cached[1]
        if self._historical_fetches_this_refresh >= HIST_CANDLE_MAX_CALLS_PER_REFRESH:
            return []
        if self.market_data is None or not self.market_data.configured:
            return []

        self._historical_fetches_this_refresh += 1
        try:
            raw = self.market_data.intraday_candles(
                key, interval="FIVE_MINUTE", exchange="NSE", days=1
            )
            candles = []
            for row in raw or []:
                candle = dict(row)
                candle["data_frequency"] = "FIVE_MINUTE"
                candle["source"] = "angel_one_historical_5m"
                candle["candle_source"] = "angel_one_historical_5m"
                candles.append(candle)
            self._historical_fetch_count += 1
            self._historical_candle_cache[key] = (time.monotonic(), candles)
            while len(self._historical_candle_cache) > 80:
                oldest = next(iter(self._historical_candle_cache))
                self._historical_candle_cache.pop(oldest, None)
            if candles:
                logger.info("SCALP_HISTORICAL_CANDLES symbol=%s bars=%d source=angel_one_historical_5m",
                            key, len(candles))
            else:
                logger.info("SCALP_HISTORICAL_CANDLES_EMPTY symbol=%s", key)
            return candles
        except Exception as exc:
            # AngelUnavailable often means the shared client is already in its
            # provider-wide cooldown. Do not count that as another HTTP failure.
            # Negative-cache briefly (not the full success TTL) so a recovered
            # provider can be retried without hammering it every scan.
            message = str(exc).lower()
            provider_cooldown = isinstance(exc, AngelUnavailable)
            if not provider_cooldown and (
                "403" in message or "429" in message or "rate" in message or "access rate" in message
            ):
                failure_reason = "angel_historical_rate_limited"
                self._historical_fetch_failures += 1
            elif provider_cooldown:
                failure_reason = "angel_historical_cooldown"
            else:
                failure_reason = "angel_historical_request_failed"
                self._historical_fetch_failures += 1
            logger.warning("SCALP_HISTORICAL_CANDLES_FAILED symbol=%s reason=%s error=%s", key, failure_reason, type(exc).__name__)
            negative_ttl = max(30.0, float(os.getenv("SCALP_HIST_FAILURE_TTL_S", "60")))
            # Cache entries use the normal TTL. Backdate the negative entry so it
            # expires after negative_ttl rather than holding an empty result for
            # the full five-minute successful-candle TTL.
            cache_stamp = time.monotonic() - max(0.0, HIST_CANDLE_TTL_S - negative_ttl)
            self._historical_candle_cache[key] = (cache_stamp, [])
            return []

    @staticmethod
    def _parse_context_candles(candles: list[dict[str, Any]], today) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
        for raw in candles:
            candle = dict(raw)
            stamp_value = candle.get("time")
            if not stamp_value and candle.get("timestamp_ms"):
                try:
                    candle["time"] = datetime.fromtimestamp(
                        float(candle["timestamp_ms"]) / 1000.0,
                        tz=now_ist().tzinfo,
                    ).isoformat(timespec="seconds")
                    stamp_value = candle["time"]
                except (TypeError, ValueError, OSError):
                    stamp_value = None
            try:
                stamp = datetime.fromisoformat(str(stamp_value))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=now_ist().tzinfo)
                stamp = stamp.astimezone(now_ist().tzinfo)
                candle["_parsed_time"] = stamp
                if stamp.date() != today:
                    continue
                op, hi, lo, close = (float(candle[k]) for k in ("open", "high", "low", "close"))
                if min(op, hi, lo, close) <= 0 or hi < max(op, close, lo) or lo > min(op, close, hi):
                    continue
                if candle.get("data_frequency") != "FIVE_MINUTE":
                    continue
                parsed.append(candle)
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
        parsed.sort(key=lambda x: x["_parsed_time"])
        return parsed

    def _underlying_context(self, symbol: str) -> dict[str, Any] | None:
        """Use fresh real 5-minute bars; repair missing WS opening ranges with bounded Angel history."""
        candles = self.stream.recent_candles_for_symbol(symbol, limit=80)
        today = now_ist().date()
        parsed = self._parse_context_candles(candles, today)

        def has_opening_range(items: list[dict[str, Any]]) -> bool:
            return sum(
                1 for candle in items
                if 9 * 60 + 15 <= candle["_parsed_time"].hour * 60 + candle["_parsed_time"].minute < 9 * 60 + 30
            ) >= 2

        # Render cold starts and WS reconnections may subscribe after 09:30.
        # The local tick-built buffer then cannot contain the opening range. Fetch
        # only a bounded number of real historical bars, cache them, and never
        # fabricate an opening range from a later candle.
        if len(parsed) < 4 or not has_opening_range(parsed):
            historical = self._historical_context_candles(symbol)
            if historical:
                parsed_historical = self._parse_context_candles(historical, today)
                merged = {c["_parsed_time"]: c for c in parsed_historical}
                merged.update({c["_parsed_time"]: c for c in parsed})
                parsed = sorted(merged.values(), key=lambda x: x["_parsed_time"])

        if len(parsed) < 4:
            self._last_context_rejection = "insufficient_valid_today_5m_candles"
            return None
        # Do not mislabel a rate-budget deferral as a broken opening range. It is
        # operationally different from a successful historical response missing
        # the 09:15-09:25 bars, and should be visible as such in scan diagnostics.
        if not has_opening_range(parsed):
            cached_history = self._historical_candle_cache.get(symbol.upper())
            if cached_history is None and self._historical_fetches_this_refresh >= HIST_CANDLE_MAX_CALLS_PER_REFRESH:
                self._last_context_rejection = "historical_fetch_budget_deferred"
                return None
            if cached_history is not None and not cached_history[1]:
                self._last_context_rejection = "historical_history_unavailable"
                return None
        latest = parsed[-1]
        latest_stamp = latest["_parsed_time"]
        try:
            observed_at = float(latest.get("observed_at") or latest_stamp.timestamp())
        except (TypeError, ValueError):
            observed_at = latest_stamp.timestamp()
        candle_age_s = max(0.0, now_ist().timestamp() - observed_at)
        if candle_age_s > 360:
            self._last_context_rejection = "stale_5m_candles"
            return None
        opening = [
            candle for candle in parsed
            if 9 * 60 + 15 <= candle["_parsed_time"].hour * 60 + candle["_parsed_time"].minute < 9 * 60 + 30
        ]
        if len(opening) < 2:
            self._last_context_rejection = "opening_range_incomplete"
            return None
        or_high = max(float(c["high"]) for c in opening)
        or_low = min(float(c["low"]) for c in opening)
        recent = parsed[-1]
        prev = parsed[-2]
        close = float(recent["close"])
        prev_close = float(prev["close"])
        momentum = ((close - prev_close) / prev_close * 100.0) if prev_close else 0.0
        volumes = [
            float(c.get("volume") or 0) for c in parsed[:-1]
            if float(c.get("volume") or 0) > 0 and c.get("volume_valid") is not False
        ]
        last_volume = float(recent.get("volume") or 0)
        median_volume = statistics.median(volumes[-5:]) if len(volumes) >= 3 else 0.0
        relative_volume = last_volume / median_volume if median_volume > 0 else 0.0
        # No "volume OK" fallback when historical volume is missing/zero. A
        # missing-volume candle is missing evidence, not positive confirmation.
        volume_ok = (
            len(volumes) >= 3
            and median_volume > 0
            and last_volume > 0
            and recent.get("volume_valid") is not False
            and relative_volume >= MIN_RELATIVE_VOLUME
        )
        total_volume = sum(max(0.0, float(c.get("volume") or 0)) for c in parsed)
        vwap = (
            sum(float(c["close"]) * max(0.0, float(c.get("volume") or 0)) for c in parsed) / total_volume
            if total_volume > 0 else sum(float(c["close"]) for c in parsed) / len(parsed)
        )
        candle_range = max(0.0, float(recent.get("high") or close) - float(recent.get("low") or close))
        candle_open = float(recent.get("open") or close)
        close_location = (close - float(recent.get("low") or close)) / candle_range if candle_range > 0 else 0.5
        candle_body_pct = abs(close - candle_open) / prev_close * 100.0 if prev_close > 0 else 0.0
        opening_range_width = max(0.0, or_high - or_low)
        breakout_buffer = max(close * MIN_BREAKOUT_BUFFER_PCT / 100.0, opening_range_width * 0.10)
        breakout_distance_pct = 0.0
        buy_breakout = close > or_high + breakout_buffer
        sell_breakout = close < or_low - breakout_buffer
        if buy_breakout and or_high > 0:
            breakout_distance_pct = (close - or_high) / or_high * 100.0
        elif sell_breakout and or_low > 0:
            breakout_distance_pct = (or_low - close) / or_low * 100.0
        candle_shape_buy = close > candle_open and close_location >= MIN_CANDLE_CLOSE_LOCATION and candle_body_pct >= 0.08
        candle_shape_sell = close < candle_open and close_location <= (1.0 - MIN_CANDLE_CLOSE_LOCATION) and candle_body_pct >= 0.08
        if (
            buy_breakout and close > vwap and momentum >= MOMENTUM_MIN_PCT
            and volume_ok and candle_shape_buy
        ):
            direction = "BUY"
        elif (
            sell_breakout and close < vwap and momentum <= -MOMENTUM_MIN_PCT
            and volume_ok and candle_shape_sell
        ):
            direction = "SELL"
        else:
            direction = None
            if not volume_ok:
                self._last_context_rejection = "volume_not_confirmed"
            elif (buy_breakout and not candle_shape_buy) or (sell_breakout and not candle_shape_sell):
                self._last_context_rejection = "breakout_candle_weak"
            elif (close > or_high and close <= vwap) or (close < or_low and close >= vwap):
                self._last_context_rejection = "wrong_side_of_vwap"
            elif abs(momentum) < MOMENTUM_MIN_PCT:
                self._last_context_rejection = "momentum_below_threshold"
            elif or_low <= close <= or_high:
                self._last_context_rejection = "inside_opening_range"
            else:
                self._last_context_rejection = "directional_setup_not_confirmed"
        if direction:
            self._last_context_rejection = ""
        return {
            "direction": direction,
            "ltp": close,
            "vwap": vwap,
            "or_high": or_high,
            "or_low": or_low,
            "momentum_pct": momentum,
            "last_volume": last_volume,
            "median_volume": median_volume,
            "relative_volume": round(relative_volume, 3),
            "volume_ok": volume_ok,
            "candle_close_location": round(close_location, 4),
            "candle_body_pct": round(candle_body_pct, 4),
            "breakout_distance_pct": round(breakout_distance_pct, 4),
            "candle_count": len(parsed),
            "candle_age_s": round(candle_age_s, 2),
            "candle_source": latest.get("candle_source") or latest.get("source") or "angel_one_websocket_v2",
            "data_frequency": "FIVE_MINUTE",
        }

    def _chain_confirmation(self, symbol: str, direction: str, chain: dict[str, Any]) -> dict[str, Any] | None:
        rows = chain.get("rows") or []
        if not rows:
            return None
        option_type = "CE" if direction == "BUY" else "PE"
        target = [r for r in rows if r.get("option_type") == option_type and float(r.get("strike") or 0) == float(chain["atm_strike"])]
        if not target:
            target = [r for r in rows if r.get("option_type") == option_type]
        if not target:
            return None
        option = min(target, key=lambda r: abs(float(r.get("strike") or 0) - float(chain["atm_strike"])))
        if float(option.get("oi") or 0) < MIN_OPTION_OI or float(option.get("volume") or 0) < MIN_OPTION_VOLUME:
            return None
        previous = self._previous_chain.get(symbol, {})
        prev_by_key = {(r.get("strike"), r.get("option_type")): r for r in previous.get("rows", [])}
        prev = prev_by_key.get((option.get("strike"), option.get("option_type")))
        if not prev:
            return None
        current_premium = float(option.get("ltp") or 0)
        previous_premium = float(prev.get("ltp") or 0)
        if current_premium <= 0 or previous_premium <= 0:
            return None
        premium_change = current_premium - previous_premium
        premium_change_pct = premium_change / previous_premium * 100.0
        opposite_type = "PE" if option_type == "CE" else "CE"
        opposite = next((r for r in rows if r.get("option_type") == opposite_type and float(r.get("strike") or 0) == float(option.get("strike") or 0)), None)
        previous_opposite = prev_by_key.get((option.get("strike"), opposite_type))
        oi_delta = float(option.get("oi") or 0) - float(prev.get("oi") or 0)
        opposite_oi_delta = (
            float(opposite.get("oi") or 0) - float(previous_opposite.get("oi") or 0)
            if opposite and previous_opposite else None
        )
        # A falling directional OI value must never pass merely because the
        # opposite side's OI fell faster. Require actual directional OI addition,
        # premium follow-through, and a material (not one-tick) premium increase.
        if (
            premium_change_pct < MIN_PREMIUM_CHANGE_PCT
            or opposite_oi_delta is None
            or oi_delta <= 0
            or oi_delta <= opposite_oi_delta
        ):
            return None
        return {
            "option": option,
            "premium_change_pct": round(premium_change_pct, 4),
            "previous_option_ltp": previous_premium,
            "oi_delta": oi_delta,
            "opposite_oi_delta": opposite_oi_delta,
            "option_oi": float(option.get("oi") or 0),
            "option_volume": float(option.get("volume") or 0),
            "rule": "CHAIN_PREMIUM_RISING_AND_DIRECTIONAL_OI",
        }

    def probe_chain(self, symbol: str = "RELIANCE") -> tuple[bool, str]:
        """One bounded live chain probe used by readiness/self-test."""
        try:
            self.stream.ensure_symbols([symbol])
            tick = self.stream.latest_tick_for_symbol(symbol, max_age_s=15.0)
            spot = float((tick or {}).get("ltp") or 0)
            if spot <= 0 and self.market_data is not None:
                quote = self.market_data.full_quotes([symbol]).get(symbol) or {}
                spot = float(quote.get("ltp") or 0)
            chain = self._chain(symbol, spot=spot)
            if not chain:
                return False, f"{symbol} chain unavailable"
            rows = chain.get("rows") or []
            ce = sum(1 for row in rows if row.get("option_type") == "CE")
            pe = sum(1 for row in rows if row.get("option_type") == "PE")
            return ce > 0 and pe > 0, f"{symbol} expiry={chain.get('expiry')} atm={chain.get('atm_strike')} CE={ce} PE={pe}"
        except Exception as exc:
            return False, f"{type(exc).__name__}: {str(exc)[:120]}"

    def _ranked_universe(self) -> tuple[list[str], int, int, list[str], list[str]]:
        """Build the live scalp universe from current F&O gainers and losers."""
        symbols = sorted(cached_universe())
        if not symbols:
            self._last_error = "dynamic_fno_universe_unavailable"
            return [], 0, 0, [], []
        try:
            self.stream.ensure_symbols(symbols)
        except Exception:
            logger.exception("Could not seed dynamic scalp subscriptions")
            return [], 0, 0, [], []
        ranked: list[tuple[str, float]] = []
        for symbol in symbols:
            if symbol in self._open:
                continue
            tick = self.stream.latest_tick_for_symbol(symbol, max_age_s=15.0)
            if not tick:
                continue
            try:
                ltp = float(tick.get("ltp") or 0)
                prev_close = float(tick.get("close") or 0)
            except (TypeError, ValueError):
                continue
            if ltp <= 0 or prev_close <= 0:
                continue
            pct = (ltp - prev_close) / prev_close * 100.0
            if pct == pct:
                ranked.append((symbol, pct))
        gainers = sorted((x for x in ranked if x[1] > 0), key=lambda x: x[1], reverse=True)[:RANK_TOP_N]
        losers = sorted((x for x in ranked if x[1] < 0), key=lambda x: x[1])[:RANK_TOP_N]
        selected = {symbol: pct for symbol, pct in gainers}
        selected.update({symbol: pct for symbol, pct in losers})
        # Evaluate the strongest movers from BOTH sides first. This matters because
        # historical candles are deliberately rate-limited; a gainers-first walk
        # could consume the entire history budget before checking any losers.
        ordered = [symbol for symbol, _ in sorted(selected.items(), key=lambda item: abs(item[1]), reverse=True)]
        return ordered, len(gainers), len(losers), [s for s, _ in gainers], [s for s, _ in losers]

    def _candidate_contexts(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        symbols, gainers, losers, gainer_symbols, loser_symbols = self._ranked_universe()
        candidates: list[dict[str, Any]] = []
        early_rejections: dict[str, int] = {}
        early_rejected_symbols: dict[str, str] = {}
        for symbol in symbols:
            self._last_context_rejection = ""
            ctx = self._underlying_context(symbol)
            if ctx and ctx.get("direction"):
                tick = self.stream.latest_tick_for_symbol(symbol, max_age_s=15.0) or {}
                prev_close = float(tick.get("close") or 0)
                ltp = float(tick.get("ltp") or ctx.get("ltp") or 0)
                day_change_pct = ((ltp - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0
                candidates.append({"symbol": symbol, **ctx, "day_change_pct": day_change_pct})
            else:
                reason = self._last_context_rejection or "directional_setup_not_confirmed"
                early_rejections[reason] = early_rejections.get(reason, 0) + 1
                if len(early_rejected_symbols) < 20:
                    early_rejected_symbols[symbol] = reason
        candidates.sort(key=lambda x: abs(float(x.get("day_change_pct") or 0)), reverse=True)
        return candidates[:TOP_N], {
            "ranked_universe": len(symbols),
            "gainers": gainers,
            "losers": losers,
            "gainer_symbols": gainer_symbols,
            "loser_symbols": loser_symbols,
            "early_rejections": early_rejections,
            "early_rejected_symbols": early_rejected_symbols,
        }

    def _enter(self, candidate: dict[str, Any], confirmation: dict[str, Any], chain: dict[str, Any]) -> None:
        symbol = candidate["symbol"]
        option = confirmation["option"]
        now = now_ist()
        event = {
            "trade_date": now.date().isoformat(),
            "mode": MODE,
            "symbol": symbol,
            "direction": candidate["direction"],
            "strike": float(option["strike"]),
            "option_type": option["option_type"],
            "expiry": chain["expiry"],
            "entry_time_ist": now.isoformat(),
            "underlying_entry": float(candidate["ltp"]),
            "entry_option_ltp": float(option["ltp"]),
            # Persist the exact evidence that authorized this entry so future
            # winner-vs-loser analysis is based on facts, not reconstructed guesses.
            "signal_quality": {
                "version": "scalp-quality-v2",
                "underlying": {
                    key: candidate.get(key) for key in (
                        "vwap", "or_high", "or_low", "momentum_pct", "last_volume",
                        "median_volume", "relative_volume", "volume_ok",
                        "candle_close_location", "candle_body_pct", "breakout_distance_pct",
                        "candle_count", "candle_age_s", "candle_source", "data_frequency",
                        "day_change_pct",
                    )
                },
                "option": {
                    "premium_change_pct": confirmation.get("premium_change_pct"),
                    "previous_option_ltp": confirmation.get("previous_option_ltp"),
                    "directional_oi_delta": confirmation.get("oi_delta"),
                    "opposite_oi_delta": confirmation.get("opposite_oi_delta"),
                    "oi": confirmation.get("option_oi"),
                    "volume": confirmation.get("option_volume"),
                    "confirmation_rule": confirmation.get("rule"),
                },
                "spread_check": "unavailable_from_current_quote_payload",
            },
            "rule_ids": [
                "TIME_WINDOW",
                "UNDERLYING_OR_BREAK",
                "UNDERLYING_VWAP_SIDE",
                "UNDERLYING_VOLUME_NOT_DEAD",
                confirmation["rule"],
                "LIQUID_OPTION",
                "MAX_CONCURRENT",
                "FO_BAN_CLEAR",
            ],
            "chain_snapshot_id": uuid.uuid4().hex,
            "chain_snapshot": chain,
            "paper_only": True,
        }
        event_id = self.repository.save_scalp_entry(event)
        self._open[symbol] = {**event, "id": event_id}
        logger.info(
            "SCALP_ENTRY PAPER symbol=%s direction=%s option=%s strike=%s underlying=%.2f option_ltp=%.2f",
            symbol, event["direction"], event["option_type"], event["strike"],
            event["underlying_entry"], event["entry_option_ltp"],
        )

    def _exit_reason(
        self,
        position: dict[str, Any],
        underlying: dict[str, Any] | None,
        option_ltp: float,
        now: datetime,
    ) -> str | None:
        # There is intentionally no elapsed-time exit: keep a paper position while
        # momentum persists. The protective premium stop and 15:15 hard-flat remain
        # absolute safety rules.
        if not _before(now, ABSOLUTE_EXIT):
            return "CLOCK"
        entry = float(position["entry_option_ltp"])
        change_pct = (option_ltp - entry) / entry * 100.0 if entry > 0 else 0.0
        if change_pct <= -OPTION_STOP_PCT:
            return "OPTION_STOP"

        peak = max(float(position.get("_peak_option_ltp") or entry), float(option_ltp or 0))
        position["_peak_option_ltp"] = peak
        peak_change_pct = (peak - entry) / entry * 100.0 if entry > 0 else 0.0
        if OPTION_TARGET_PCT > 0 and peak_change_pct >= OPTION_TARGET_PCT:
            position["_trailing_armed"] = True

        # A missing/stale underlying context is NOT evidence that momentum weakened.
        # In that case keep monitoring the exact option quote and protective stop.
        weak = False
        if underlying is not None:
            direction = str(position.get("direction") or "").upper()
            try:
                spot = float(underlying.get("ltp") or 0)
                vwap = float(underlying.get("vwap") or 0)
                momentum = float(underlying.get("momentum_pct") or 0)
                or_level = float(underlying.get("or_high") if direction == "BUY" else underlying.get("or_low") or 0)
                if direction == "BUY":
                    weak = momentum <= 0 or (vwap > 0 and spot <= vwap) or (or_level > 0 and spot < or_level)
                elif direction == "SELL":
                    weak = momentum >= 0 or (vwap > 0 and spot >= vwap) or (or_level > 0 and spot > or_level)
            except (TypeError, ValueError):
                weak = False
            position["_weak_signal_count"] = int(position.get("_weak_signal_count") or 0) + 1 if weak else 0
            if int(position.get("_weak_signal_count") or 0) >= 2:
                return "MOMENTUM_WEAK"

        if position.get("_trailing_armed") and peak > 0:
            drawdown_from_peak_pct = (peak - option_ltp) / peak * 100.0
            if drawdown_from_peak_pct >= TRAIL_DRAWDOWN_PCT and int(position.get("_weak_signal_count") or 0) >= 1:
                return "TRAILING_MOMENTUM_WEAK"
        return None

    def _close_paper_position(
        self,
        symbol: str,
        position: dict[str, Any],
        now: datetime,
        reason: str,
        *,
        underlying: dict[str, Any] | None,
        tick: dict[str, Any] | None,
        option_ltp: float | None,
        quote_note: str | None = None,
    ) -> None:
        """Close the paper position without inventing an exit price or P&L."""
        hold = _minutes_between(position["entry_time_ist"], now)
        entry_ltp = float(position["entry_option_ltp"])
        valid_exit = None
        try:
            candidate = float(option_ltp) if option_ltp is not None else 0.0
            if candidate > 0:
                valid_exit = candidate
        except (TypeError, ValueError):
            valid_exit = None

        underlying_exit = None
        if underlying is not None:
            try:
                candidate_underlying = float(underlying.get("ltp") or 0)
                if candidate_underlying > 0:
                    underlying_exit = candidate_underlying
            except (TypeError, ValueError):
                pass
        if underlying_exit is None and tick:
            try:
                candidate_underlying = float(tick.get("ltp") or 0)
                if candidate_underlying > 0:
                    underlying_exit = candidate_underlying
            except (TypeError, ValueError):
                pass

        payload: dict[str, Any] = {
            "pnl_status": "QUOTED_PAPER_EXIT" if valid_exit is not None else "UNPRICED",
            "exit_quote_source": "angel_one_option_chain" if valid_exit is not None else None,
            "exit_quote_unavailable": valid_exit is None,
            "exit_quote_note": quote_note if valid_exit is None else None,
            "pnl_is_realized": False,
        }
        if valid_exit is not None and entry_ltp > 0:
            payload["pnl_premium_pct"] = round((valid_exit - entry_ltp) / entry_ltp * 100.0, 4)
        else:
            payload["pnl_note"] = (
                "No fresh positive quote for the exact contract at the paper exit; "
                "exit premium and P&L are intentionally unknown."
            )

        self.repository.close_scalp(
            int(position["id"]),
            exit_time_ist=now.isoformat(),
            underlying_exit=underlying_exit,
            exit_option_ltp=valid_exit,
            hold_minutes=hold,
            exit_reason=reason,
            payload=payload,
        )
        self._last_exit_pnl_status = str(payload["pnl_status"])
        if valid_exit is None:
            self._unpriced_close_count += 1
        logger.info(
            "SCALP_EXIT PAPER symbol=%s direction=%s option=%s strike=%s reason=%s "
            "hold_minutes=%.2f option_ltp=%s pnl_status=%s",
            symbol, position.get("direction"), position.get("option_type"),
            position.get("strike"), reason, hold,
            f"{valid_exit:.2f}" if valid_exit is not None else "UNPRICED",
            payload["pnl_status"],
        )
        self._open.pop(symbol, None)

    def _manage_open(self) -> None:
        if not self._open:
            return
        now = now_ist()
        self._last_monitor_at_ist = now.isoformat()
        quote_window = _in_window(now, "09:15", "15:30")
        market_data_ready = bool(self.market_data and self.market_data.configured)

        for symbol, position in list(self._open.items()):
            # Never close merely because a timer elapsed. Only the hard-flat clock
            # is time-based; all earlier exits are momentum/risk driven.
            due_reason = "CLOCK" if not _before(now, ABSOLUTE_EXIT) else None

            underlying = None
            if due_reason is None:
                try:
                    underlying = self._underlying_context(symbol)
                except Exception as exc:
                    logger.warning("SCALP_MONITOR_CONTEXT_FAILED symbol=%s error=%s", symbol, type(exc).__name__)

            tick: dict[str, Any] = {}
            try:
                tick = self.stream.latest_tick_for_symbol(symbol, max_age_s=15.0) or {}
            except Exception as exc:
                logger.warning("SCALP_MONITOR_TICK_FAILED symbol=%s error=%s", symbol, type(exc).__name__)

            spot = 0.0
            try:
                spot = float((underlying or {}).get("ltp") or tick.get("ltp") or 0)
            except (TypeError, ValueError):
                spot = 0.0

            chain = None
            # Option marks must come from a current-session query. Never use a cached
            # chain or an after-hours value as the supposed hard-flat exit price.
            if market_data_ready and quote_window and spot > 0:
                chain = self._chain(symbol, spot=spot, force_refresh=due_reason is not None)

            option_ltp: float | None = None
            if chain and str(chain.get("expiry") or "").replace(" ", "").upper() == str(position.get("expiry") or "").replace(" ", "").upper():
                row = next(
                    (
                        r for r in chain.get("rows", [])
                        if r.get("option_type") == position["option_type"]
                        and float(r.get("strike") or 0) == float(position["strike"])
                    ),
                    None,
                )
                try:
                    candidate = float((row or {}).get("ltp") or 0)
                    if candidate > 0:
                        option_ltp = candidate
                except (TypeError, ValueError):
                    option_ltp = None

            if option_ltp is not None:
                entry_ltp = float(position["entry_option_ltp"])
                position["_last_option_ltp"] = option_ltp
                position["_last_option_quote_at_ist"] = now.isoformat()
                position["_last_pnl_pct"] = round((option_ltp - entry_ltp) / entry_ltp * 100.0, 3) if entry_ltp else None

            if due_reason is not None:
                quote_note = None
                if not quote_window:
                    quote_note = "exit deadline was handled outside the regular market quote window"
                elif not market_data_ready:
                    quote_note = "Angel One market data was unavailable"
                elif spot <= 0:
                    quote_note = "no fresh underlying spot was available to query the option chain"
                elif chain is None:
                    quote_note = f"option chain unavailable: {self._last_error or 'no response'}"
                elif option_ltp is None:
                    quote_note = "exact strike, option side, and expiry had no positive quote"
                self._close_paper_position(
                    symbol, position, now, due_reason, underlying=underlying,
                    tick=tick, option_ltp=option_ltp, quote_note=quote_note,
                )
                continue

            # If the quote is unavailable, leave the paper position open for another
            # monitoring cycle; do not substitute the entry price. A fresh option mark
            # still allows premium stop/target checks if candle context is missing.
            if option_ltp is None:
                continue
            reason = self._exit_reason(position, underlying, option_ltp, now)
            if reason:
                self._close_paper_position(
                    symbol, position, now, reason, underlying=underlying,
                    tick=tick, option_ltp=option_ltp,
                )

    def refresh(self) -> None:
        now = now_ist()
        self._historical_fetches_this_refresh = 0
        # Open positions must be monitored before any entry-window or feed-readiness
        # early return so momentum, protective stops and 15:15 hard-flat remain active.
        if self._open:
            self._manage_open()
        if not _before(now, ABSOLUTE_EXIT):
            return
        if not _in_window(now, "09:15", "15:15"):
            return
        if not self.market_data or not self.market_data.configured:
            self._last_error = "angel_not_configured"
            return
        if not _in_window(now, ENTRY_START, ENTRY_END):
            return
        if len(self._open) >= MAX_CONCURRENT:
            return
        # Clear an earlier transient error before this eligible scan; any new
        # failure below will set it again so the dashboard never shows stale errors.
        self._last_error = ""
        ban_status = ban_info()
        stats: dict[str, Any] = {
            "candidates": 0, "chain_checked": 0, "chain_unavailable": 0,
            "confirmation_failed": 0, "entries": 0,
            "ban_list_unavailable": not bool(ban_status.get("ban_list_ok")),
            "top_rejections": {}, "at": now.isoformat(timespec="seconds"),
            "ranked_universe": 0, "gainers": 0, "losers": 0, "gainer_symbols": [], "loser_symbols": [],
        }
        if not bool(ban_status.get("ban_list_ok")):
            # Fail closed for new entries when the exchange ban list is unknown.
            # Existing paper positions were already monitored above and remain managed.
            self._last_error = "fo_ban_list_unavailable"
            stats["top_rejections"] = {"fo_ban_list_unavailable": 1}
            self._last_run_stats = stats
            self._last_run_at = time.monotonic()
            if time.monotonic() - self._last_ban_warning_at >= 300.0:
                logger.warning("SCALP_BAN_LIST_UNAVAILABLE: blocking new paper entries")
                self._last_ban_warning_at = time.monotonic()
            return
        blocked = {str(x).upper() for x in banned_symbols()}

        candidates, ranking_stats = self._candidate_contexts()
        stats.update(ranking_stats)
        stats["candidates"] = len(candidates)
        rejection_counts: dict[str, int] = dict(ranking_stats.get("early_rejections") or {})
        for candidate in candidates:
            if candidate["symbol"] in blocked:
                rejection_counts["fo_ban_period"] = rejection_counts.get("fo_ban_period", 0) + 1
                continue
            if len(self._open) >= MAX_CONCURRENT:
                rejection_counts["max_concurrent"] = rejection_counts.get("max_concurrent", 0) + 1
                break
            stats["chain_checked"] += 1
            chain = self._chain(candidate["symbol"], spot=float(candidate["ltp"]))
            if not chain:
                stats["chain_unavailable"] += 1
                rejection_counts["option_chain_unavailable"] = rejection_counts.get("option_chain_unavailable", 0) + 1
                continue
            confirmation = self._chain_confirmation(candidate["symbol"], candidate["direction"], chain)
            self._previous_chain[candidate["symbol"]] = chain
            if confirmation:
                self._enter(candidate, confirmation, chain)
                stats["entries"] += 1
            else:
                stats["confirmation_failed"] += 1
                rejection_counts["option_chain_confirmation"] = rejection_counts.get("option_chain_confirmation", 0) + 1

        stats["top_rejections"] = dict(sorted(rejection_counts.items(), key=lambda kv: -kv[1])[:8])
        self._last_run_stats = stats
        self._last_run_at = time.monotonic()
        logger.info(
            "SCALP_REFRESH ranked=%d gainers=%d losers=%d candidates=%d chain_checked=%d "
            "chain_unavailable=%d confirmation_failed=%d entries=%d early_rejections=%s "
            "early_rejected_symbols=%s rejections=%s",
            stats["ranked_universe"], stats["gainers"], stats["losers"], stats["candidates"],
            stats["chain_checked"], stats["chain_unavailable"], stats["confirmation_failed"],
            stats["entries"], stats.get("early_rejections", {}),
            stats.get("early_rejected_symbols", {}), stats["top_rejections"],
        )

    def live(self) -> dict[str, Any]:
        now = now_ist()
        open_rows = []
        for position in self._open.values():
            hold = _minutes_between(position["entry_time_ist"], now)
            entry_ltp = float(position["entry_option_ltp"])
            stop_ltp = round(entry_ltp * (1.0 - OPTION_STOP_PCT / 100.0), 2) if entry_ltp > 0 else None
            target_ltp = round(entry_ltp * (1.0 + OPTION_TARGET_PCT / 100.0), 2) if entry_ltp > 0 else None
            open_rows.append({
                **position,
                "hold_minutes": round(hold, 2),
                "stop_loss": stop_ltp,
                "target": target_ltp,
                "stop_loss_option_ltp": stop_ltp,
                "target_option_ltp": target_ltp,
                "target_behavior": "trailing_activation_not_automatic_exit",
                "trailing_armed": bool(position.get("_trailing_armed")),
                "peak_option_ltp": position.get("_peak_option_ltp", entry_ltp),
                "weak_signal_count": int(position.get("_weak_signal_count") or 0),
                "current_option_ltp": position.get("_last_option_ltp"),
                "current_option_quote_at_ist": position.get("_last_option_quote_at_ist"),
                "pnl_pct": position.get("_last_pnl_pct"),
                "pnl_status": "QUOTED" if position.get("_last_option_ltp") is not None else "WAITING_FOR_QUOTE",
                "risk_reward_ratio": round(OPTION_TARGET_PCT / OPTION_STOP_PCT, 3) if OPTION_STOP_PCT > 0 else None,
                "status": "OPEN",
            })
        recent = self.repository.scalp_events_for_date(now.date().isoformat())[:30]
        return {
            "mode": MODE,
            "paper_only": True,
            "open": open_rows,
            "recent": recent,
            "updated_at_ist": now.isoformat(),
        }
