"""Public-source capability inventory for the dashboard and operators."""

from __future__ import annotations

from typing import Any


def public_source_inventory(
    *,
    oi_source: str = "no_scan_yet",
    oi_data_status: str = "NOT_RUN",
    angel_configured: bool = False,
    stream_health: dict[str, Any] | None = None,
    history_latest_trade_date: str | None = None,
    expected_bhavcopy_date: str | None = None,
) -> list[dict[str, Any]]:
    """Report capabilities separately from last observed availability."""
    stream_health = stream_health or {}
    stream_state = str(stream_health.get("state") or "unknown").lower()
    stream_age = stream_health.get("last_tick_age_s")
    live_angel_oi = oi_source in {"angel_futures", "angel_one_full_quote_overlay"}
    oi_rows_available = oi_data_status == "RECEIVED"
    if not angel_configured:
        angel_status = "NOT_CONFIGURED"
    elif live_angel_oi and oi_rows_available:
        angel_status = "ACTIVE"
    elif oi_data_status == "UNAVAILABLE":
        angel_status = "CONFIGURED_NO_LIVE_OI_ROWS"
    else:
        angel_status = "CONFIGURED_AWAITING_VERIFIED_SCAN"

    if stream_state == "connected":
        stream_status = "CONNECTED"
    elif not angel_configured:
        stream_status = "NOT_CONFIGURED"
    else:
        stream_status = "DISCONNECTED_OR_DISABLED"

    if oi_source == "nse_oi_spurts" and oi_rows_available:
        nse_oi_status = "ACTIVE_FALLBACK"
    elif oi_data_status == "UNAVAILABLE":
        nse_oi_status = "UNAVAILABLE"
    else:
        nse_oi_status = "STANDBY_FALLBACK"

    history_is_current = bool(
        history_latest_trade_date and expected_bhavcopy_date
        and str(history_latest_trade_date) >= str(expected_bhavcopy_date)
    )
    history_status = (
        "CURRENT" if history_is_current
        else "STALE_OR_MISSING" if expected_bhavcopy_date
        else "NOT_VERIFIED"
    )
    return [
        {"source": "Angel One NFO futures quotes/OI", "coverage": "Primary live F&O OI and underlying price inputs", "cadence": "market-hours scan", "status": angel_status},
        {"source": "Angel One WebSocket ticks/5-minute bars", "coverage": "Fresh intraday context and scalp monitoring; read-only", "cadence": "live stream", "status": stream_status, "last_tick_age_s": stream_age},
        {"source": "Angel One stock option chain", "coverage": "Near-ATM stock option CE/PE premiums, OI and volume", "cadence": "bounded on-demand requests", "status": "READ_ONLY_CONFIGURED" if angel_configured else "NOT_CONFIGURED"},
        {"source": "NSE OI spurts", "coverage": "Optional fallback for all-F&O price/OI candidates; not a required live dependency", "cadence": "throttled market-hours fallback", "status": nse_oi_status},
        {"source": "NSE option chain", "coverage": "Optional context only; not required for Angel stock-option entries", "cadence": "best-effort on demand", "status": "BEST_EFFORT_FALLBACK"},
        {"source": "NSE equity bhavcopy/history", "coverage": "Daily EMA/ATR/volume/regime context", "cadence": "best-effort after publication plus stored snapshot", "status": history_status, "latest_trade_date": history_latest_trade_date, "expected_trade_date": expected_bhavcopy_date},
        {"source": "NSE all indices", "coverage": "Index/VIX context; Angel read-only quotes may fill index values", "cadence": "cached context refresh", "status": "BEST_EFFORT_WITH_ANGEL_FALLBACK"},
        {"source": "NSE FII/DII activity", "coverage": "Cash-market activity excluded from signal logic", "cadence": "not collected", "status": "NOT_USED"},
        {"source": "NSE corporate announcements", "coverage": "Event-volatility labels excluded from signal logic", "cadence": "not collected", "status": "NOT_USED"},
        {"source": "NSE participant-wise OI", "coverage": "FII/DII/Client/Pro end-of-day positions", "cadence": "only after report publication", "status": "OPTIONAL_EOD"},
        {"source": "NSE holiday master", "coverage": "F&O trading-session calendar with bundled fallback", "cadence": "weekly plus fallback", "status": "BEST_EFFORT_WITH_BUNDLED_FALLBACK"},
        {"source": "BSE/media/macro feeds", "coverage": "not collected by this build", "cadence": "not applicable", "status": "NOT_CONFIGURED"},
    ]
