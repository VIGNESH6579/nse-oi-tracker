"""Normalize free public NSE index, breadth, VIX, and FII/DII data."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


WATCHED_INDICES = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
    "FINNIFTY": "NIFTY FINANCIAL SERVICES",
    "MIDCPNIFTY": "NIFTY MIDCAP SELECT",
    "INDIA_VIX": "INDIA VIX",
}


def _number(value: object) -> float | None:
    try:
        return round(float(str(value).replace(",", "")), 4)
    except (TypeError, ValueError):
        return None


def normalize_market_overview(
    indices_payload: Mapping[str, Any] | None,
    fii_dii_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Produce a compact stable index snapshot; FII/DII is compatibility-only."""
    payload = indices_payload or {}
    rows = payload.get("data", []) if isinstance(payload, Mapping) else []
    by_index = {
        str(row.get("index") or "").strip().upper(): row
        for row in rows if isinstance(row, Mapping)
    }
    indices: dict[str, dict[str, Any] | None] = {}
    for key, nse_name in WATCHED_INDICES.items():
        row = by_index.get(nse_name)
        indices[key] = None if row is None else {
            "name": nse_name,
            "last": _number(row.get("last")),
            "change": _number(row.get("variation")),
            "change_pct": _number(row.get("percentChange")),
            "open": _number(row.get("open")),
            "high": _number(row.get("high")),
            "low": _number(row.get("low")),
            "previous_close": _number(row.get("previousClose")),
        }

    activity: dict[str, dict[str, Any]] = {}
    for row in fii_dii_rows:
        category = str(row.get("category") or "").upper()
        if category == "FII/FPI":
            category = "FII_FPI"
        if category not in {"FII_FPI", "DII"}:
            continue
        activity[category] = {
            "date": row.get("date"),
            "buy_value_crore": _number(row.get("buyValue")),
            "sell_value_crore": _number(row.get("sellValue")),
            "net_value_crore": _number(row.get("netValue")),
        }

    advances = _number(payload.get("advances"))
    declines = _number(payload.get("declines"))
    return {
        "source": "NSE public market APIs",
        "nse_timestamp": payload.get("timestamp"),
        "indices": indices,
        "market_breadth": {
            "advances": int(advances) if advances is not None else None,
            "declines": int(declines) if declines is not None else None,
            "unchanged": int(_number(payload.get("unchanged")) or 0) if payload.get("unchanged") is not None else None,
            "advance_decline_ratio": round(advances / declines, 4) if advances is not None and declines else None,
        },
        "fii_dii_cash_activity": activity,
        "data_caveat": "FII/DII cash activity is intentionally omitted from signal logic; context uses indices, VIX, and breadth.",
    }
