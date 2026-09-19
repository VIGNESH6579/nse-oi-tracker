"""Pure candle-based grading for paper setups."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True, slots=True)
class Grade:
    exit_reason: str
    exit_price: float
    exit_ts: str | None
    r_multiple: float
    mae: float
    mfe: float


def _r(direction: str, entry: float, price: float, risk: float) -> float:
    if risk <= 0:
        return 0.0
    signed = price - entry if direction.upper() == "BUY" else entry - price
    return signed / risk


def grade_candles(setup: Mapping, candles: Iterable[Mapping], *, time_exit: str = "15:15", cost_pct: float = 0.05) -> Grade:
    direction = str(setup.get("direction", "BUY")).upper()
    entry = float(setup["entry"])
    stop = float(setup["stop_loss"] if "stop_loss" in setup else setup["sl"])
    target1 = float(setup["target_1"] if "target_1" in setup else setup["tg1"])
    target2 = float(setup["target_2"] if "target_2" in setup else setup["tg2"])
    risk = abs(entry - stop)
    mae = 0.0
    mfe = 0.0
    for candle in candles:
        high, low = float(candle["high"]), float(candle["low"])
        mae = min(mae, _r(direction, entry, low if direction == "BUY" else high, risk))
        mfe = max(mfe, _r(direction, entry, high if direction == "BUY" else low, risk))
        stop_hit = low <= stop if direction == "BUY" else high >= stop
        target2_hit = high >= target2 if direction == "BUY" else low <= target2
        target1_hit = high >= target1 if direction == "BUY" else low <= target1
        # Conservative rule: when both sides are inside one candle, stop wins.
        if stop_hit:
            price = stop
            return Grade("SL_HIT", price, str(candle.get("time") or candle.get("ts") or ""), _r(direction, entry, price, risk) - cost_pct / 100, mae, mfe)
        if target2_hit:
            return Grade("TG2_HIT", target2, str(candle.get("time") or candle.get("ts") or ""), _r(direction, entry, target2, risk) - cost_pct / 100, mae, mfe)
        if target1_hit:
            return Grade("TG1_HIT", target1, str(candle.get("time") or candle.get("ts") or ""), _r(direction, entry, target1, risk) - cost_pct / 100, mae, mfe)
        if str(candle.get("time") or candle.get("ts") or "")[11:16] >= time_exit:
            close = float(candle["close"])
            return Grade("TIME_EXIT", close, str(candle.get("time") or candle.get("ts") or ""), _r(direction, entry, close, risk) - cost_pct / 100, mae, mfe)
    return Grade("OPEN", entry, None, -cost_pct / 100, mae, mfe)


def summarize_grades(grades: Iterable[Grade]) -> dict[str, float | int]:
    rows = list(grades)
    if not rows:
        return {"trades": 0, "hit_rate": 0.0, "avg_r": 0.0, "expectancy": 0.0, "profit_factor": 0.0, "max_drawdown_r": 0.0}
    rs = [row.r_multiple for row in rows]
    wins = [value for value in rs if value > 0]
    losses = [value for value in rs if value < 0]
    equity = peak = drawdown = 0.0
    for value in rs:
        equity += value
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return {"trades": len(rows), "hit_rate": round(sum(row.exit_reason.startswith("TG") for row in rows) / len(rows), 4),
            "avg_r": round(sum(rs) / len(rs), 4), "expectancy": round(sum(rs) / len(rs), 4),
            "profit_factor": round(sum(wins) / abs(sum(losses)), 4) if losses else 0.0,
            "max_drawdown_r": round(abs(drawdown), 4)}
