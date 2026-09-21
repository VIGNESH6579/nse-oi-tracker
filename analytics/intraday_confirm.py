"""Pure intraday helpers: session VWAP, opening range, volume pace, extension."""

from __future__ import annotations

from typing import Any

from analytics.intraday import candle_vwap

SESSION_START_MIN = 9 * 60 + 15          # 09:15 IST
OR_END_MIN = 9 * 60 + 30                 # opening range = 09:15-09:30
# Approximate cumulative share of the day's volume traded by N minutes after the
# open (U-shaped NSE profile). It is a documented approximation, not exact data.
VOLUME_CURVE = [(0, 0.0), (15, 0.12), (30, 0.20), (60, 0.32), (120, 0.47), (180, 0.58),
                (240, 0.70), (300, 0.82), (345, 0.92), (375, 1.0)]


def candle_minute(candle: dict) -> int | None:
    stamp = str(candle.get("time") or "")
    try:
        return int(stamp[11:13]) * 60 + int(stamp[14:16])
    except ValueError:
        return None


def expected_volume_fraction(minutes_since_open: float) -> float:
    if minutes_since_open <= 0:
        return 0.0
    for (x0, y0), (x1, y1) in zip(VOLUME_CURVE, VOLUME_CURVE[1:]):
        if minutes_since_open <= x1:
            return y0 + (y1 - y0) * (minutes_since_open - x0) / (x1 - x0)
    return 1.0


def summarize_candles(candles: list[dict]) -> dict[str, Any]:
    rows = [c for c in candles if candle_minute(c) is not None]
    rows.sort(key=lambda c: candle_minute(c))
    if not rows:
        return {"available": False}
    opening = [c for c in rows if SESSION_START_MIN <= candle_minute(c) < OR_END_MIN]
    last_minute = candle_minute(rows[-1])
    return {
        "available": True,
        "vwap": candle_vwap(rows),
        "day_open": float(rows[0]["open"]),
        "day_high": max(float(c["high"]) for c in rows),
        "day_low": min(float(c["low"]) for c in rows),
        "last_close": float(rows[-1]["close"]),
        "twap": round(sum((float(c["high"]) + float(c["low"]) + float(c["close"])) / 3 for c in rows) / len(rows), 4),
        "session_volume": sum(float(c.get("volume") or 0) for c in rows),
        "or_high": max((float(c["high"]) for c in opening), default=None),
        "or_low": min((float(c["low"]) for c in opening), default=None),
        "or_complete": last_minute >= OR_END_MIN and len(opening) >= 2,
        "last_minute": last_minute,
        "candle_count": len(rows),
    }


def quote_context(quote: dict, opening_range: dict | None, now_minute: int) -> dict[str, Any] | None:
    """Intraday context from a live Angel quote: exchange VWAP (avgPrice), session volume and
    day range, plus an opening range observed by our own scans. Needs no candle API call."""
    avg, day_open = float(quote.get("avg_price") or 0), float(quote.get("open") or 0)
    if avg <= 0 or day_open <= 0:
        return None
    covered = bool(opening_range) and float(opening_range.get("span_min") or 0) >= 8 and now_minute >= OR_END_MIN
    return {
        "available": True, "vwap": avg, "vwap_kind": "exchange_avg_price", "day_open": day_open,
        "day_high": float(quote.get("high") or 0), "day_low": float(quote.get("low") or 0),
        "last_close": float(quote.get("ltp") or 0), "session_volume": float(quote.get("volume") or 0),
        "or_high": opening_range["high"] if opening_range else None,
        "or_low": opening_range["low"] if opening_range else None,
        "or_complete": covered, "or_source": "observed_scans",
        "last_minute": now_minute - 5,                      # gate adds 5 back: minutes since the open
        "source": "angel_quote", "data_frequency": "scan", "vwap_age_s": 0.0, "candle_count": 0,
    }


def average_daily_volume(bars: list[dict], period: int = 20) -> float | None:
    volumes = [float(b.get("volume") or 0) for b in bars[-period:] if float(b.get("volume") or 0) > 0]
    return sum(volumes) / len(volumes) if len(volumes) >= max(5, period // 2) else None


def relative_volume(session_volume: float, avg_daily_volume: float | None, minutes_since_open: float) -> float | None:
    fraction = expected_volume_fraction(minutes_since_open)
    if not avg_daily_volume or fraction < 0.12:
        return None
    return round(session_volume / (avg_daily_volume * fraction), 3)


def bias_from_candles(candles: list[dict]) -> str:
    """Index regime from 5-minute candles: BULL / BEAR / NEUTRAL / UNKNOWN."""
    rows = [c for c in candles if candle_minute(c) is not None]
    if len(rows) < 6:
        return "UNKNOWN"
    closes = [float(c["close"]) for c in rows]
    day_open, last = float(rows[0]["open"]), closes[-1]
    sma = sum(closes[-20:]) / len(closes[-20:])
    if last > day_open * 1.001 and last > sma:
        return "BULL"
    if last < day_open * 0.999 and last < sma:
        return "BEAR"
    return "NEUTRAL"
