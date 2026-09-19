from datetime import datetime, timedelta

from database.repository import SignalRepository
from utils.time import IST


def signal(symbol="RULE", price=100.0, direction="BUY", **extra):
    return {
        "symbol": symbol,
        "signal": "LONG_BUILDUP" if direction == "BUY" else "SHORT_BUILDUP",
        "signal_direction": direction,
        "confidence": 82,
        "confidence_tier": "HIGH",
        "ltp": price,
        "price_change_pct": 1.0,
        "oi_change_pct": 5.0,
        **extra,
    }


def test_entry_window_edges_and_skip_reason(tmp_path):
    from config.settings import get_settings
    get_settings.cache_clear()
    repo = SignalRepository(tmp_path / "rules.sqlite3")
    repo.record_scan([signal("EARLY")], datetime(2026, 9, 10, 9, 29, 59, tzinfo=IST))
    repo.record_scan([signal("AT_END")], datetime(2026, 9, 10, 14, 30, 0, tzinfo=IST))
    assert repo.history_for_date("2026-09-10")[1] == 0
    reasons = repo.skip_reasons_for_date("2026-09-10")
    assert [row["skip_reason"] for row in reasons] == ["window", "window"]


def test_max_open_and_daily_cap_are_enforced(tmp_path, monkeypatch):
    monkeypatch.setenv("MAX_OPEN", "1")
    monkeypatch.setenv("MAX_SETUPS_DAY", "1")
    from config.settings import get_settings
    get_settings.cache_clear()
    repo = SignalRepository(tmp_path / "caps.sqlite3")
    at = datetime(2026, 9, 10, 10, 0, tzinfo=IST)
    repo.record_scan([signal("FIRST")], at)
    repo.record_scan([signal("SECOND")], at + timedelta(minutes=1))
    assert repo.history_for_date("2026-09-10")[1] == 1
    assert repo.skip_reasons_for_date("2026-09-10")[-1]["skip_reason"] == "cap"


def test_opposite_direction_requires_flip_gap(tmp_path):
    from config.settings import get_settings
    get_settings.cache_clear()
    repo = SignalRepository(tmp_path / "flip.sqlite3")
    at = datetime(2026, 9, 10, 10, 0, tzinfo=IST)
    repo.record_scan([signal("FLIP")], at)
    repo.update_open_events([signal("FLIP", price=99.0)], at + timedelta(minutes=1))
    repo.record_scan([signal("FLIP", price=98.0, direction="SELL")], at + timedelta(minutes=2))
    assert repo.history_for_date("2026-09-10")[1] == 1
    assert repo.skip_reasons_for_date("2026-09-10")[-1]["skip_reason"] == "flip_gap"
