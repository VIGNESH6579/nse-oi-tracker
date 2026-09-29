from datetime import datetime
from zoneinfo import ZoneInfo

from analytics.candle_priority import prioritize_candidates
from analytics.intraday_confirm import summarize_candles


def test_fast_15m_momentum_gets_candle_priority():
    rows = [
        {"symbol": "SLOW", "signal": "LONG_BUILDUP", "oi": 100000, "oi_window": {"h15": {"price_pct": 0.5, "oi_pct": 1.0}, "window_signal": "LONG_BUILDUP"}, "volume": 1000},
        {"symbol": "POLICYBZR", "signal": "LONG_BUILDUP", "oi": 200000, "oi_window": {"h15": {"price_pct": 4.0, "oi_pct": 8.0}, "window_signal": "LONG_BUILDUP"}, "volume": 9000},
    ]
    ranked = prioritize_candidates(rows)
    assert ranked[0]["symbol"] == "POLICYBZR"
    assert ranked[0]["candle_priority_rank"] == 1


def test_malformed_candle_is_not_fresh():
    candles = [{"time": "2026-09-29 10:00", "open": 100, "high": 99, "low": 98, "close": 99, "volume": 100}]
    result = summarize_candles(candles, now=datetime(2026, 9, 29, 10, 2, tzinfo=ZoneInfo("Asia/Kolkata")))
    assert result["available"] is False
