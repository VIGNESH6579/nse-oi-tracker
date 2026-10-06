"""Angel One stock-option scalping paper engine.

This module is deliberately separate from the legacy F&O OI swing scanner.
It has no order-placement code and no CAS/regime-entry dependency.
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

from collector.fno_ban import banned_symbols
from utils.time import now_ist
from integrations.angel_one_market_data import AngelOneMarketData, AngelUnavailable
from integrations.angel_one_stream import AngelOneMarketStream

logger = logging.getLogger(__name__)

MODE = "scalp_chain"
ENTRY_START = os.getenv("ENTRY_START", "09:30")
ENTRY_END = os.getenv("ENTRY_END", "14:30")
MAX_HOLD_MINUTES = int(os.getenv("MAX_HOLD_MINUTES", "10"))
ABSOLUTE_EXIT = os.getenv("SCALP_ABSOLUTE_EXIT", "15:15")
MAX_CONCURRENT = int(os.getenv("SCALP_MAX_CONCURRENT", "3"))
OPTION_STOP_PCT = float(os.getenv("SCALP_OPTION_STOP_PCT", "30"))
OPTION_TARGET_PCT = float(os.getenv("SCALP_OPTION_TARGET_PCT", "20"))
CHAIN_TTL_S = float(os.getenv("SCALP_CHAIN_TTL_S", "20"))
TOP_N = int(os.getenv("SCALP_TOP_N", "10"))
MIN_OPTION_OI = int(os.getenv("SCALP_MIN_OPTION_OI", "10000"))
MIN_OPTION_VOLUME = int(os.getenv("SCALP_MIN_OPTION_VOLUME", "100"))
MOMENTUM_MIN_PCT = float(os.getenv("SCALP_MOMENTUM_MIN_PCT", "0.20"))

DEFAULT_UNIVERSE = (
    "RELIANCE,TCS,INFY,HDFCBANK,ICICIBANK,SBIN,AXISBANK,KOTAKBANK,"
    "LT,ITC,ADANIENT,ADANIPORTS,BAJFINANCE,MARUTI,M&M,AXISBANK,"
    "SUNPHARMA,DRREDDY,APOLLOHOSP,TATASTEEL,JSWSTEEL,HINDALCO,"
    "TATAMOTORS,EICHERMOT,HEROMOTOCO,BAJAJFINSV,INDUSINDBK,"
    "BHARTIARTL,TECHM,WIPRO"
)


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
        self.universe = self._load_universe()
        self._chain_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._previous_chain: dict[str, dict[str, Any]] = {}
        self._open: dict[str, dict[str, Any]] = {}
        self._last_run_at = 0.0
        self._last_error = ""
        self._load_open_events()

    def _load_universe(self) -> list[str]:
        raw = os.getenv("SCALP_UNIVERSE", DEFAULT_UNIVERSE)
        result = []
        for item in raw.split(","):
            symbol = item.strip().upper().removesuffix("-EQ")
            if symbol and symbol not in result:
                result.append(symbol)
        return result[:50]

    def health(self) -> dict[str, Any]:
        return {
            "mode": MODE,
            "paper_only": True,
            "universe_size": len(self.universe),
            "top_n": TOP_N,
            "open_scalps": len(self._open),
            "chain_cache_symbols": len(self._chain_cache),
            "chain_ok": bool(self._chain_cache),
            "last_error": self._last_error,
            "max_hold_minutes": MAX_HOLD_MINUTES,
            "entry_window": f"{ENTRY_START}-{ENTRY_END} IST",
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

    def _chain(self, symbol: str, *, spot: float) -> dict[str, Any] | None:
        if self.market_data is None or not self.market_data.configured:
            self._last_error = "angel_not_configured"
            return None
        key = symbol.upper()
        cached = self._chain_cache.get(key)
        if cached and time.monotonic() - cached[0] < CHAIN_TTL_S:
            return cached[1]
        try:
            chain = self.market_data.stock_option_chain_snapshot(key, strikes_each_side=1, spot=spot)
            if not chain.get("ok"):
                self._last_error = str(chain.get("reason") or "chain_unavailable")
                return None
            self._chain_cache[key] = (time.monotonic(), chain)
            return chain
        except AngelUnavailable as exc:
            self._last_error = str(exc)[:120]
            return None
        except Exception as exc:
            self._last_error = type(exc).__name__
            logger.warning("Scalp option chain unavailable for %s: %s", key, type(exc).__name__)
            return None

    def _underlying_context(self, symbol: str) -> dict[str, Any] | None:
        candles = self.stream.recent_candles_for_symbol(symbol, limit=12)
        if len(candles) < 4:
            return None
        today = now_ist().date()
        parsed = []
        for candle in candles:
            try:
                stamp = datetime.fromisoformat(str(candle["time"]))
                if stamp.date() != today:
                    continue
                parsed.append(candle)
            except Exception:
                continue
        if len(parsed) < 4:
            return None
        parsed.sort(key=lambda x: str(x.get("time") or ""))
        opening = [c for c in parsed if "09:15" <= str(c.get("time"))[11:16] < "09:30"]
        if len(opening) < 2:
            return None
        or_high = max(float(c["high"]) for c in opening)
        or_low = min(float(c["low"]) for c in opening)
        recent = parsed[-1]
        prev = parsed[-2]
        close = float(recent["close"])
        prev_close = float(prev["close"])
        momentum = ((close - prev_close) / prev_close * 100.0) if prev_close else 0.0
        volumes = [float(c.get("volume") or 0) for c in parsed[:-1] if float(c.get("volume") or 0) > 0]
        last_volume = float(recent.get("volume") or 0)
        median_volume = statistics.median(volumes[-5:]) if volumes else 0.0
        volume_ok = last_volume > 0 and (median_volume <= 0 or last_volume >= median_volume * 0.5)
        total_volume = sum(max(0.0, float(c.get("volume") or 0)) for c in parsed)
        vwap = (
            sum(float(c["close"]) * max(0.0, float(c.get("volume") or 0)) for c in parsed) / total_volume
            if total_volume > 0 else sum(float(c["close"]) for c in parsed) / len(parsed)
        )
        if close > or_high and close > vwap and momentum >= MOMENTUM_MIN_PCT and volume_ok:
            direction = "BUY"
        elif close < or_low and close < vwap and momentum <= -MOMENTUM_MIN_PCT and volume_ok:
            direction = "SELL"
        else:
            direction = None
        return {
            "direction": direction,
            "ltp": close,
            "vwap": vwap,
            "or_high": or_high,
            "or_low": or_low,
            "momentum_pct": momentum,
            "last_volume": last_volume,
            "median_volume": median_volume,
            "volume_ok": volume_ok,
            "candle_count": len(parsed),
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
        premium_change = float(option["ltp"] or 0) - float(prev.get("ltp") or 0)
        premium_change_pct = premium_change / float(prev.get("ltp") or 1) * 100.0
        opposite_type = "PE" if option_type == "CE" else "CE"
        opposite = next((r for r in rows if r.get("option_type") == opposite_type and float(r.get("strike") or 0) == float(option.get("strike") or 0)), None)
        previous_opposite = prev_by_key.get((option.get("strike"), opposite_type))
        oi_delta = float(option.get("oi") or 0) - float(prev.get("oi") or 0)
        opposite_oi_delta = (
            float(opposite.get("oi") or 0) - float(previous_opposite.get("oi") or 0)
            if opposite and previous_opposite else None
        )
        if premium_change_pct <= 0 or opposite_oi_delta is None or oi_delta <= opposite_oi_delta:
            return None
        return {
            "option": option,
            "premium_change_pct": premium_change_pct,
            "oi_delta": oi_delta,
            "opposite_oi_delta": opposite_oi_delta,
            "rule": "CHAIN_PREMIUM_RISING_AND_DIRECTIONAL_OI",
        }

    def _candidate_contexts(self) -> list[dict[str, Any]]:
        try:
            self.stream.ensure_symbols(self.universe)
        except Exception:
            logger.exception("Could not seed scalp underlying subscriptions")
            return []
        candidates = []
        for symbol in self.universe:
            if symbol in self._open:
                continue
            ctx = self._underlying_context(symbol)
            if ctx and ctx.get("direction"):
                candidates.append({"symbol": symbol, **ctx})
        candidates.sort(key=lambda x: abs(float(x.get("momentum_pct") or 0)), reverse=True)
        return candidates[:TOP_N]

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

    def _exit_reason(self, position: dict[str, Any], underlying: dict[str, Any], option_ltp: float, now: datetime) -> str | None:
        hold = _minutes_between(position["entry_time_ist"], now)
        if hold >= MAX_HOLD_MINUTES:
            return "TIME"
        if not _before(now, ABSOLUTE_EXIT):
            return "CLOCK"
        direction = position["direction"]
        if direction == "BUY" and (float(underlying["ltp"]) <= float(underlying["vwap"]) or float(underlying["ltp"]) < float(underlying["or_high"])):
            return "UNDERLYING_REVERSE"
        if direction == "SELL" and (float(underlying["ltp"]) >= float(underlying["vwap"]) or float(underlying["ltp"]) > float(underlying["or_low"])):
            return "UNDERLYING_REVERSE"
        entry = float(position["entry_option_ltp"])
        change_pct = (option_ltp - entry) / entry * 100.0 if entry else 0.0
        if change_pct <= -OPTION_STOP_PCT:
            return "OPTION_STOP"
        if OPTION_TARGET_PCT > 0 and change_pct >= OPTION_TARGET_PCT:
            return "OPTION_TARGET"
        return None

    def _manage_open(self) -> None:
        if not self._open:
            return
        now = now_ist()
        for symbol, position in list(self._open.items()):
            underlying = self._underlying_context(symbol)
            if not underlying:
                continue
            chain = self._chain(symbol, spot=float(underlying["ltp"]))
            if not chain:
                continue
            option_type = position["option_type"]
            row = next((r for r in chain.get("rows", []) if r.get("option_type") == option_type and float(r.get("strike") or 0) == float(position["strike"])), None)
            if not row:
                continue
            reason = self._exit_reason(position, underlying, float(row["ltp"]), now)
            if not reason:
                continue
            hold = _minutes_between(position["entry_time_ist"], now)
            self.repository.close_scalp(
                int(position["id"]),
                exit_time_ist=now.isoformat(),
                underlying_exit=float(underlying["ltp"]),
                exit_option_ltp=float(row["ltp"]),
                hold_minutes=hold,
                exit_reason=reason,
                payload={"pnl_premium_pct": ((float(row["ltp"]) - float(position["entry_option_ltp"])) / float(position["entry_option_ltp"]) * 100.0) if position["entry_option_ltp"] else 0.0},
            )
            logger.info(
                "SCALP_EXIT PAPER symbol=%s direction=%s option=%s strike=%s reason=%s hold_minutes=%.2f option_ltp=%.2f",
                symbol, position["direction"], option_type, position["strike"], reason, hold, float(row["ltp"]),
            )
            self._open.pop(symbol, None)

    def refresh(self) -> None:
        now = now_ist()
        if not self.market_data or not self.market_data.configured:
            self._last_error = "angel_not_configured"
            return
        if not _in_window(now, "09:15", "15:15"):
            return
        self._manage_open()
        if not _in_window(now, ENTRY_START, ENTRY_END):
            return
        if len(self._open) >= MAX_CONCURRENT:
            return
        blocked = {str(x).upper() for x in banned_symbols()}
        for candidate in self._candidate_contexts():
            if candidate["symbol"] in blocked:
                continue
            if len(self._open) >= MAX_CONCURRENT:
                break
            chain = self._chain(candidate["symbol"], spot=float(candidate["ltp"]))
            if not chain:
                continue
            confirmation = self._chain_confirmation(candidate["symbol"], candidate["direction"], chain)
            self._previous_chain[candidate["symbol"]] = chain
            if confirmation:
                self._enter(candidate, confirmation, chain)
        self._last_run_at = time.monotonic()

    def live(self) -> dict[str, Any]:
        now = now_ist()
        open_rows = []
        for position in self._open.values():
            hold = _minutes_between(position["entry_time_ist"], now)
            open_rows.append({
                **position,
                "hold_minutes": round(hold, 2),
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
