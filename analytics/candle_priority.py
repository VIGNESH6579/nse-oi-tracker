"""Deterministic priority ordering for scarce historical 5-minute candle requests."""
from __future__ import annotations
from typing import Any

def _num(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0

def _norm(value: float, values: list[float]) -> float:
    if not values:
        return 0.0
    lo, hi = min(values), max(values)
    if hi == lo:
        return 1.0 if value > 0 else 0.0
    return (value - lo) / (hi - lo)

def _h15(signal: dict[str, Any]) -> tuple[float, float]:
    h15 = signal.get("oi_window", {}).get("h15") or {}
    return abs(_num(h15.get("price_pct"))), abs(_num(h15.get("oi_pct")))

def priority_score(signal: dict[str, Any], batch: list[dict[str, Any]]) -> float:
    price, oi = _h15(signal)
    prices = [_h15(row)[0] for row in batch]
    ois = [_h15(row)[1] for row in batch]
    volumes = [_num((row.get("angel_quote") or {}).get("volume") or row.get("volume")) for row in batch]
    liquidity = [_num(row.get("oi")) for row in batch]
    consistency = 1.0 if (signal.get("oi_window") or {}).get("window_signal") == signal.get("signal") else 0.0
    new_candidate = 1.0 if signal.get("candidate_origin") == "window_15m" else 0.0
    volume = _num((signal.get("angel_quote") or {}).get("volume") or signal.get("volume"))
    liq = _num(signal.get("oi"))
    return round(35 * _norm(price, prices) + 30 * _norm(oi, ois) + 15 * _norm(volume, volumes)
                 + 10 * new_candidate + 5 * consistency + 5 * _norm(liq, liquidity), 4)

def prioritize_candidates(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(signals) <= 1:
        return list(signals)
    scored = [(priority_score(signal, signals), str(signal.get("symbol") or "")) for signal in signals]
    ordered = sorted(scored, key=lambda item: (-item[0], item[1]))
    rank = {symbol: index + 1 for index, (_score, symbol) in enumerate(ordered)}
    score_map = {symbol: score for score, symbol in scored}
    return [{**signal, "candle_priority_score": score_map[str(signal.get("symbol") or "")],
             "candle_priority_rank": rank[str(signal.get("symbol") or "")]} for signal in sorted(signals, key=lambda s: rank[str(s.get("symbol") or "")])]

__all__ = ["priority_score", "prioritize_candidates"]
