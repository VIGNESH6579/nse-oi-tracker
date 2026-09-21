"""Rolling per-symbol OI/price window kept in RAM (about 90 minutes).

The day-change price/OI from the feed cannot tell whether a build-up is
happening NOW (SHREECEM stayed "long build-up" while falling all day). This
keeps a short time series so classification, persistence and strength are
measured over 15/30/60 minutes and since the first observation of the session.
"""

from __future__ import annotations

import os
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Any

LONG_BUILDUP, SHORT_BUILDUP = "LONG_BUILDUP", "SHORT_BUILDUP"
SHORT_COVERING, LONG_UNWINDING = "SHORT_COVERING", "LONG_UNWINDING"
HORIZONS = (15, 30, 60)


def classify(price_pct: float, oi_pct: float, *, price_min: float | None = None, oi_min: float | None = None) -> str:
    price_min = float(os.getenv("WINDOW_PRICE_PCT", "0.25")) if price_min is None else price_min
    oi_min = float(os.getenv("WINDOW_OI_PCT", "0.30")) if oi_min is None else oi_min
    if price_pct >= price_min and oi_pct >= oi_min:
        return LONG_BUILDUP
    if price_pct <= -price_min and oi_pct >= oi_min:
        return SHORT_BUILDUP
    if price_pct >= price_min and oi_pct <= -oi_min:
        return SHORT_COVERING
    if price_pct <= -price_min and oi_pct <= -oi_min:
        return LONG_UNWINDING
    return "NEUTRAL"


def _pct(new: float, old: float) -> float:
    return (new - old) / old * 100.0 if old else 0.0


class OIWindow:
    def __init__(self, max_minutes: int = 90) -> None:
        self._max = timedelta(minutes=max_minutes)
        self._points: dict[str, deque[tuple[datetime, float, float]]] = {}
        self._first: dict[str, tuple[datetime, float, float]] = {}
        self._streak: dict[str, tuple[int, datetime]] = {}
        self._source: dict[str, str] = {}
        self._day = None
        self._lock = threading.Lock()

    def reset(self) -> None:
        with self._lock:
            self._points.clear(); self._first.clear(); self._streak.clear(); self._source.clear(); self._day = None

    def update(self, symbol: str, ts: datetime, ltp: float, oi: float, source: str = "") -> None:
        if not symbol or ltp <= 0 or oi <= 0:
            return
        with self._lock:
            if self._day != ts.date():
                self._points.clear(); self._first.clear(); self._streak.clear(); self._source.clear()
                self._day = ts.date()
            if source and self._source.get(symbol, source) != source:
                # data source changed (NSE <-> Angel): OI definitions differ, so restart this symbol
                self._points.pop(symbol, None); self._first.pop(symbol, None); self._streak.pop(symbol, None)
            if source:
                self._source[symbol] = source
            points = self._points.setdefault(symbol, deque())
            if points and ts <= points[-1][0]:
                return                                   # duplicate / out-of-order scan
            points.append((ts, float(ltp), float(oi)))
            self._first.setdefault(symbol, (ts, float(ltp), float(oi)))
            cutoff = ts - self._max
            while points and points[0][0] < cutoff:
                points.popleft()

    def last_price(self, symbol: str, now: datetime | None = None, max_age_s: float = 300.0) -> float | None:
        """Latest observed price for a symbol if it is fresh (used to monitor open trades)."""
        with self._lock:
            points = self._points.get(symbol) or self._points.get(symbol.upper())
            if not points:
                return None
            ts, price, _oi = points[-1]
            if now is not None and (now - ts).total_seconds() > max_age_s:
                return None
            return price

    def depth(self) -> dict[str, float]:
        """Median history length (minutes) and symbol count, for /api/health."""
        with self._lock:
            spans = sorted((p[-1][0] - p[0][0]).total_seconds() / 60 for p in self._points.values() if len(p) > 1)
            return {"symbols": len(self._points), "median_minutes": round(spans[len(spans) // 2], 1) if spans else 0.0}

    def _horizon(self, points, minutes: int) -> dict[str, float] | None:
        last = points[-1]
        target = last[0] - timedelta(minutes=minutes)
        ref = None
        for point in points:
            if point[0] <= target:
                ref = point
            else:
                break
        if ref is None:
            oldest = points[0]
            if (last[0] - oldest[0]).total_seconds() < 0.6 * minutes * 60:
                return None                                # not enough history yet
            ref = oldest
        return {"price_pct": round(_pct(last[1], ref[1]), 3), "oi_pct": round(_pct(last[2], ref[2]), 3),
                "minutes": round((last[0] - ref[0]).total_seconds() / 60, 1)}

    def context(self, symbol: str) -> dict[str, Any]:
        with self._lock:
            points = self._points.get(symbol)
            if not points or len(points) < 2:
                return {"points": len(points or ()), "history_minutes": 0.0, "window_signal": None}
            out: dict[str, Any] = {"points": len(points),
                                   "history_minutes": round((points[-1][0] - points[0][0]).total_seconds() / 60, 1)}
            for minutes in HORIZONS:
                out[f"h{minutes}"] = self._horizon(points, minutes)
            first = self._first[symbol]
            out["since_open"] = {"price_pct": round(_pct(points[-1][1], first[1]), 3),
                                 "oi_pct": round(_pct(points[-1][2], first[2]), 3),
                                 "minutes": round((points[-1][0] - first[0]).total_seconds() / 60, 1)}
            basis = out["h15"] or out["h30"]
            out["window_signal"] = classify(basis["price_pct"], basis["oi_pct"]) if basis else None
            return out

    def note_agreement(self, symbol: str, agrees: bool, now: datetime, max_gap_minutes: float = 6.0) -> int:
        """Consecutive evaluations where the window agrees with the published signal."""
        with self._lock:
            count, last = self._streak.get(symbol, (0, now))
            if (now - last).total_seconds() > max_gap_minutes * 60:
                count = 0                                   # symbol dropped out; not consecutive
            count = count + 1 if agrees else 0
            self._streak[symbol] = (count, now)
            return count
