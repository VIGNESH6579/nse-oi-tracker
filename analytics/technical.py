"""Pure technical context computed from chronological NSE daily candles."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def _number(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _ordered(candles: Iterable[Mapping[str, Any]]) -> list[dict[str, float | str]]:
    """Normalize daily bars in ascending trade-date order."""
    rows = [
        {
            "trade_date": str(candle.get("trade_date") or ""),
            "open": _number(candle.get("open")),
            "high": _number(candle.get("high")),
            "low": _number(candle.get("low")),
            "close": _number(candle.get("close")),
            "volume": _number(candle.get("volume")),
        }
        for candle in candles
    ]
    return sorted((row for row in rows if row["close"] > 0), key=lambda row: row["trade_date"])


def ema(values: Iterable[float], period: int) -> float | None:
    """Latest exponential moving average using a standard SMA seed."""
    numbers = [float(value) for value in values]
    if period <= 0 or len(numbers) < period:
        return None
    result = sum(numbers[:period]) / period
    multiplier = 2 / (period + 1)
    for value in numbers[period:]:
        result = (value - result) * multiplier + result
    return round(result, 4)


def wilder_atr(candles: Iterable[Mapping[str, Any]], period: int = 14) -> float | None:
    """Latest Wilder ATR; at least period + 1 daily bars are required."""
    rows = _ordered(candles)
    if period <= 0 or len(rows) < period + 1:
        return None
    ranges = []
    for previous, current in zip(rows, rows[1:]):
        ranges.append(max(
            float(current["high"]) - float(current["low"]),
            abs(float(current["high"]) - float(previous["close"])),
            abs(float(current["low"]) - float(previous["close"])),
        ))
    result = sum(ranges[:period]) / period
    for true_range in ranges[period:]:
        result = ((result * (period - 1)) + true_range) / period
    return round(result, 4)


def efficiency_ratio(closes: Iterable[float], period: int = 10) -> float | None:
    """Kaufman efficiency ratio: 1 trends directly, 0 is directionless."""
    values = [float(value) for value in closes]
    if period <= 0 or len(values) < period + 1:
        return None
    window = values[-(period + 1):]
    path = sum(abs(current - previous) for previous, current in zip(window, window[1:]))
    return round(abs(window[-1] - window[0]) / path, 4) if path else 0.0


def daily_vwap_proxy(candles: Iterable[Mapping[str, Any]], lookback: int = 20) -> float | None:
    """Volume-weighted daily typical-price proxy, never an intraday VWAP."""
    rows = _ordered(candles)[-lookback:]
    numerator = sum(
        ((float(row["high"]) + float(row["low"]) + float(row["close"])) / 3)
        * float(row["volume"])
        for row in rows
        if float(row["volume"]) > 0
    )
    denominator = sum(float(row["volume"]) for row in rows if float(row["volume"]) > 0)
    return round(numerator / denominator, 4) if denominator else None


def relative_volume(candles: Iterable[Mapping[str, Any]], period: int = 20) -> float | None:
    """Latest daily volume relative to the preceding average daily volume."""
    rows = _ordered(candles)
    if period <= 0 or len(rows) < period + 1 or float(rows[-1]["volume"]) <= 0:
        return None
    baseline = sum(float(row["volume"]) for row in rows[-(period + 1):-1]) / period
    return round(float(rows[-1]["volume"]) / baseline, 4) if baseline else None


def classify_regime(
    *, close: float, ema20: float | None, ema50: float | None,
    atr: float | None, efficiency: float | None,
) -> str | None:
    """Classify only the daily-price regime supported by the available bars."""
    if not close or ema20 is None or ema50 is None or atr is None or efficiency is None:
        return None
    if (atr / close) >= 0.04:
        return "HIGH_VOLATILITY"
    if efficiency >= 0.30 and close > ema20 > ema50:
        return "TREND_UP"
    if efficiency >= 0.30 and close < ema20 < ema50:
        return "TREND_DOWN"
    return "RANGE_BOUND"


def _after_corporate_action(rows: list, limit: float = 0.25) -> list:
    """Drop bars before a >25% one-day close jump (split/bonus/demerger in raw bhavcopy).

    Unadjusted history would otherwise poison ATR/EMA. Fewer bars simply makes the
    confirmation gate fail closed ("atr_unavailable") until enough clean bars exist.
    """
    for index in range(len(rows) - 1, 0, -1):
        previous, current = float(rows[index - 1]["close"]), float(rows[index]["close"])
        if previous > 0 and abs(current / previous - 1) > limit:
            return rows[index:]
    return rows


def technical_context(candles: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Return explainable daily validation context and missing-data diagnostics."""
    rows = _after_corporate_action(_ordered(candles))
    closes = [float(row["close"]) for row in rows]
    latest_close = closes[-1] if closes else None
    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)
    atr14 = wilder_atr(rows, 14)
    efficiency10 = efficiency_ratio(closes, 10)
    daily_vwap20 = daily_vwap_proxy(rows, 20)
    rel_volume20 = relative_volume(rows, 20)
    prior_window = rows[-21:-1]
    daily_resistance20 = max((float(row["high"]) for row in prior_window), default=None)
    daily_support20 = min((float(row["low"]) for row in prior_window), default=None)
    missing: list[str] = []
    if len(rows) < 51:
        missing.append("at least 51 daily NSE bhavcopy bars")
    if daily_vwap20 is None:
        missing.append("20 daily bars with volume for daily VWAP proxy")
    if rel_volume20 is None:
        missing.append("21 daily bars with volume for relative volume")
    return {
        "bar_count": len(rows),
        "latest_close": round(latest_close, 4) if latest_close is not None else None,
        "ema20": ema20,
        "ema50": ema50,
        "atr14": atr14,
        "atr14_pct": round((atr14 / latest_close) * 100, 4) if atr14 and latest_close else None,
        "daily_vwap_proxy20": daily_vwap20,
        "relative_volume20": rel_volume20,
        "daily_resistance20": round(daily_resistance20, 4) if daily_resistance20 is not None else None,
        "daily_support20": round(daily_support20, 4) if daily_support20 is not None else None,
        "efficiency_ratio10": efficiency10,
        "regime": classify_regime(
            close=latest_close or 0,
            ema20=ema20,
            ema50=ema50,
            atr=atr14,
            efficiency=efficiency10,
        ),
        "validation_ready": not missing,
        "missing_data": missing,
        "data_frequency": "daily",
        "vwap_note": "daily_vwap_proxy20 is not an intraday VWAP.",
    }
