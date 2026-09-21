"""Transparent confirmation gate: decides whether a candidate is a paper-trade entry.

Fails closed: any missing critical input is reported in ``missing_confirmations``
instead of being silently ignored, so quality never degrades unnoticed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from analytics.intraday_confirm import relative_volume, SESSION_START_MIN

ENTRY_SIGNALS = {"LONG_BUILDUP": "BUY", "SHORT_BUILDUP": "SELL"}
INDEX_SYMBOLS = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYFPI"})


def _f(name: str, default: float) -> float:
    return float(os.getenv(name, default))


@dataclass(frozen=True)
class GateConfig:
    persistence_scans: int = 4
    min_history_minutes: float = 10.0
    max_extension_atr: float = 1.0
    max_vwap_dist_atr: float = 1.5
    min_rel_volume: float = 1.2
    gap_atr: float = 1.0
    gap_min_rel_volume: float = 1.8

    @classmethod
    def from_env(cls) -> "GateConfig":
        return cls(
            persistence_scans=int(_f("PERSISTENCE_SCANS", 4)),
            min_history_minutes=_f("GATE_MIN_HISTORY_MIN", 10),
            max_extension_atr=_f("EXTENSION_ATR_MULT", 1.0),
            max_vwap_dist_atr=_f("VWAP_DIST_ATR_MULT", 1.5),
            min_rel_volume=_f("REL_VOLUME_MIN", 1.2),
            gap_atr=_f("GAP_ATR_MULT", 1.0),
            gap_min_rel_volume=_f("GAP_REL_VOLUME_MIN", 1.8),
        )


def evaluate_gate(signal: dict[str, Any], *, oi_ctx: dict[str, Any], intraday: dict[str, Any] | None,
                  atr14: float | None, avg_volume: float | None, prev_close: float | None,
                  ema20: float | None, ema50: float | None, banned: bool, market_bias: str,
                  now: datetime, cfg: GateConfig | None = None, has_bars: bool = True,
                  is_index: bool = False) -> dict[str, Any]:
    cfg = cfg or GateConfig.from_env()
    name = str(signal.get("signal") or "NEUTRAL")
    direction = ENTRY_SIGNALS.get(name)
    ltp = float(signal.get("ltp") or 0)
    missing: list[str] = []
    comp: dict[str, float] = {}

    def need(ok: bool, reason: str) -> bool:
        if not ok:
            missing.append(reason)
        return ok

    need(direction is not None, "not_a_buildup_entry")
    need(has_bars, "no_daily_bars")
    need(not banned, "fo_ban_period")
    need(signal.get("stale_price") is not True, "stale_price")
    history = float(oi_ctx.get("history_minutes") or 0)
    if need(history >= cfg.min_history_minutes, "oi_history_warmup"):
        need(oi_ctx.get("window_signal") == name, "oi_window_disagrees")
        need(int(oi_ctx.get("streak") or 0) >= cfg.persistence_scans, "persistence_short")
    intraday = intraday or {}
    vwap = intraday.get("vwap")
    have_intraday = bool(intraday.get("available")) and vwap and ltp > 0
    rel = None
    if need(have_intraday, "no_intraday_data") and direction:
        need((ltp > vwap) if direction == "BUY" else (ltp < vwap), "wrong_side_of_vwap")
        if need(bool(intraday.get("or_complete")), "opening_range_incomplete"):
            if direction == "BUY":
                need(intraday.get("or_high") is not None and ltp > float(intraday["or_high"]), "inside_or_or_below")
            else:
                need(intraday.get("or_low") is not None and ltp < float(intraday["or_low"]), "inside_or_or_above")
        minutes_open = float(intraday.get("last_minute") or 0) - SESSION_START_MIN + 5
        rel = relative_volume(float(intraday.get("session_volume") or 0), avg_volume, minutes_open)
        if is_index:
            pass        # index candles carry no volume: volume checks do not apply
        elif has_bars and need(rel is not None, "rel_volume_unavailable"):
            need(rel >= cfg.min_rel_volume, "volume_too_low")
        if has_bars and need(bool(atr14) and atr14 > 0, "atr_unavailable"):
            day_open = float(intraday.get("day_open") or 0)
            need(abs(ltp - day_open) <= cfg.max_extension_atr * atr14, "extended_from_open")
            need(abs(ltp - float(vwap)) <= cfg.max_vwap_dist_atr * atr14, "extended_from_vwap")
            if prev_close and day_open and abs(day_open - prev_close) > cfg.gap_atr * atr14:
                # Gap day (results/news reaction) without a news feed: demand stronger volume.
                if not is_index:
                    need(rel is not None and rel >= cfg.gap_min_rel_volume, "gap_day_needs_volume")
                comp["gap_day"] = 1
    # --- transparent quality score (0-100), used for ranking and later calibration
    h15 = oi_ctx.get("h15") or oi_ctx.get("h30") or {}
    comp["persistence"] = round(min(int(oi_ctx.get("streak") or 0), 8) / 8 * 20, 1)
    comp["oi_strength"] = round(min(abs(float(h15.get("oi_pct") or 0)) / 1.5, 1) * 15, 1)
    comp["rel_volume"] = round(min((rel or 0) / 2, 1) * 20, 1)
    comp["vwap_or_alignment"] = 30.0 if not any(m in missing for m in ("wrong_side_of_vwap", "inside_or_or_below", "inside_or_or_above", "no_intraday_data", "opening_range_incomplete")) else 0.0
    trend_ok = bool(ema20 and ema50 and ((ema20 > ema50) == (direction == "BUY")))
    comp["daily_trend"] = 10.0 if trend_ok else 0.0
    bias_against = (direction == "BUY" and market_bias == "BEAR") or (direction == "SELL" and market_bias == "BULL")
    comp["counter_trend_penalty"] = -10.0 if bias_against else 0.0
    score = int(max(0, min(100, sum(comp.values()) + (5 if not missing else 0))))
    passed = not missing
    return {
        "confirmation_gate": "PASSED" if passed else "FAILED",
        "actionable": passed,
        "trade_recommendation": ("PAPER_" + direction) if (passed and direction) else "NO_TRADE",
        "missing_confirmations": missing,
        "quality_score": score,
        "quality_components": comp,
        "rel_volume": rel,
        "market_bias": market_bias,
    }
