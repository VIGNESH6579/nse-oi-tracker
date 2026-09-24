"""Public-source capability inventory for the dashboard and operators."""

from __future__ import annotations

from typing import Any


def public_source_inventory() -> list[dict[str, Any]]:
    """Return explicit operating coverage without claiming unavailable feeds."""
    return [
        {"source": "NSE OI spurts", "coverage": "all-F&O price/OI candidates", "cadence": "market-hours poll", "status": "ACTIVE"},
        {"source": "NSE option chain", "coverage": "PCR, max pain, strike OI/delta OI", "cadence": "on demand with cache", "status": "ACTIVE"},
        {"source": "NSE equity bhavcopy", "coverage": "daily EMA/ATR/volume/regime context", "cadence": "weekday 18:10 IST", "status": "ACTIVE"},
        {"source": "NSE all indices", "coverage": "indices, India VIX, market breadth", "cadence": "scanner cache refresh", "status": "ACTIVE"},
        {"source": "NSE FII/DII activity", "coverage": "cash-market activity excluded from signal logic", "cadence": "not collected", "status": "NOT_USED"},
        {"source": "NSE corporate announcements", "coverage": "event-volatility labels excluded from signal logic", "cadence": "not collected", "status": "NOT_USED"},
        {"source": "NSE participant-wise OI", "coverage": "FII/DII/Client/Pro end-of-day positions", "cadence": "weekday after report publication", "status": "ACTIVE_EOD"},
        {"source": "NSE holiday master", "coverage": "F&O trading-session calendar", "cadence": "weekly plus bundled fallback", "status": "ACTIVE"},
        {"source": "BSE/media/macro feeds", "coverage": "not collected by this build", "cadence": "not applicable", "status": "NOT_CONFIGURED"},
    ]
