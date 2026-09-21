"""Transparent summaries of stored candidate-event outcomes."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any


def _event_points(event: Mapping[str, Any]) -> float | None:
    entry = event.get("entry")
    exit_price = event.get("exit_price")
    if entry is None or exit_price is None:
        return None
    delta = float(exit_price) - float(entry)
    return delta if event.get("direction") == "BUY" else -delta


def _accuracy(events: list[Mapping[str, Any]]) -> float:
    completed = [event for event in events if event.get("status") not in {"OPEN", "TG1_HIT"}]
    wins = sum(1 for event in completed if int(event.get("max_target_hit") or 0) >= 1)
    return round((wins / len(completed)) * 100, 2) if completed else 0.0


def summarize_candidate_backtest(events: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize recorded events without implying executable strategy results."""
    rows = sorted(list(events), key=lambda event: str(event.get("captured_at_ist") or ""))
    counts: dict[str, int] = defaultdict(int)
    by_signal: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_hour: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_sector: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_symbol: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    pnl_points = 0.0
    equity = peak = 0.0
    max_drawdown = 0.0

    for event in rows:
        status = str(event.get("status") or "UNKNOWN")
        counts[status] += 1
        by_signal[str(event.get("signal") or "UNKNOWN")].append(event)
        by_sector[str(event.get("sector") or "Unclassified")].append(event)
        by_symbol[str(event.get("symbol") or "UNKNOWN")].append(event)
        captured = str(event.get("captured_at_ist") or "")
        try:
            hour = f"{datetime.fromisoformat(captured).hour:02d}:00"
        except ValueError:
            hour = "unknown"
        by_hour[hour].append(event)
        points = _event_points(event)
        if points is not None:
            pnl_points += points
            equity += points
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, peak - equity)

    per_signal = {
        signal: {
            "events": len(group),
            "accuracy": _accuracy(group),
            "pnl_points": round(sum(_event_points(event) or 0 for event in group), 2),
        }
        for signal, group in sorted(by_signal.items())
    }
    hourly = {
        hour: {"events": len(group), "accuracy": _accuracy(group)}
        for hour, group in sorted(by_hour.items())
    }
    def breakdown(groups: Mapping[str, list[Mapping[str, Any]]]) -> dict[str, dict[str, Any]]:
        return {
            name: {
                "events": len(group),
                "closed_events": len([event for event in group if event.get("status") not in {"OPEN", "TG1_HIT"}]),
                "accuracy": _accuracy(group),
                "pnl_points": round(sum(_event_points(event) or 0 for event in group), 2),
            }
            for name, group in sorted(groups.items())
        }
    closed = len(rows) - counts["OPEN"] - counts["TG1_HIT"]
    return {
        "events": len(rows),
        "closed_events": closed,
        "wins": sum(1 for event in rows if int(event.get("max_target_hit") or 0) >= 1),
        "losses": counts["SL_HIT"],
        "be_exits": counts["BE_EXIT"],
        "total_r": round(sum(float(event.get("result_r") or 0) for event in rows), 2),
        "accuracy": _accuracy(rows),
        "pnl_points": round(pnl_points, 2),
        "max_drawdown_points": round(max_drawdown, 2),
        "per_signal": per_signal,
        "hourly_accuracy": hourly,
        "sector_accuracy": breakdown(by_sector),
        "stock_accuracy": breakdown(by_symbol),
        "methodology_caveat": (
            "This summarizes simulated outcomes of stored price/OI candidates using "
            "the app's recorded fallback levels. It is not a validated, executable "
            "options strategy or investment-performance claim."
        ),
    }
