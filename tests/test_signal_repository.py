from datetime import datetime, timedelta

from database.repository import SignalRepository
from utils.time import IST


def _signal(symbol: str = "RELIANCE", price: float = 100.0) -> dict:
    return {
        "symbol": symbol,
        "signal": "LONG_BUILDUP",
        "signal_direction": "BUY",
        "signal_label": "Long Buildup",
        "signal_emoji": "green",
        "confidence": 82,
        "confidence_tier": "HIGH",
        "ltp": price,
        "price_change_pct": 1.4,
        "oi_change_pct": 9.0,
    }


def test_snapshots_are_deduplicated_and_events_are_server_owned(tmp_path):
    repository = SignalRepository(tmp_path / "tracker.sqlite3")
    captured_at = datetime(2026, 9, 10, 10, 0, 15, tzinfo=IST)

    first = repository.record_scan([_signal()], captured_at)
    duplicate = repository.record_scan([_signal()], captured_at)

    assert first.created is True
    assert duplicate.created is False
    assert duplicate.snapshot_id == first.snapshot_id

    events, total = repository.history_for_date("2026-09-10")
    assert total == 1
    assert events[0]["symbol"] == "RELIANCE"
    assert events[0]["entry"] == 100.0
    assert events[0]["currentLTP"] == events[0]["entry"]
    assert events[0]["risk_source"] == "percentage_fallback_pending_atr"


def test_initial_ltp_is_normalized_to_rounded_entry(tmp_path):
    repository = SignalRepository(tmp_path / "prices.sqlite3")
    repository.record_scan([_signal("PRICE", 100.1234)], datetime(2026, 9, 10, 10, 0, tzinfo=IST))
    events, _ = repository.history_for_date("2026-09-10")
    assert events[0]["entry"] == 100.12
    assert events[0]["currentLTP"] == events[0]["entry"]


def test_repeated_open_setup_is_not_counted_as_a_new_trade(tmp_path):
    repository = SignalRepository(tmp_path / "tracker.sqlite3")
    first_at = datetime(2026, 9, 10, 10, 0, tzinfo=IST)

    repository.record_scan([_signal("REPEAT", 100.0)], first_at)
    repository.record_scan([_signal("REPEAT", 100.0)], first_at + timedelta(minutes=2))

    events, total = repository.history_for_date("2026-09-10")
    assert total == 1
    assert len(events) == 1
    assert events[0]["symbol"] == "REPEAT"


def test_stop_loss_blocks_same_direction_reentry_during_cooldown(tmp_path):
    repository = SignalRepository(tmp_path / "tracker.sqlite3")
    first_at = datetime(2026, 9, 10, 10, 0, tzinfo=IST)
    repository.record_scan([_signal("COOLDOWN", 100.0)], first_at)
    repository.update_open_events([_signal("COOLDOWN", 99.0)], first_at + timedelta(minutes=1))

    repository.record_scan([_signal("COOLDOWN", 98.0)], first_at + timedelta(minutes=30))
    events, total = repository.history_for_date("2026-09-10")
    assert total == 1
    assert events[0]["status"] == "SL_HIT"

    repository.record_scan([_signal("COOLDOWN", 98.0)], first_at + timedelta(minutes=62))
    events, total = repository.history_for_date("2026-09-10")
    assert total == 2


def test_target_progression_and_market_close_are_recorded(tmp_path):
    repository = SignalRepository(tmp_path / "tracker.sqlite3")
    captured_at = datetime(2026, 9, 10, 10, 0, tzinfo=IST)
    repository.record_scan([_signal("TARGET", 100.0)], captured_at)

    assert repository.update_open_events([_signal("TARGET", 100.6)], captured_at + timedelta(minutes=1)) == 0
    events, _ = repository.history_for_date("2026-09-10")
    assert events[0]["status"] == "TG1_HIT"
    assert events[0]["max_target_hit"] == 1

    assert repository.update_open_events([_signal("TARGET", 101.1)], captured_at + timedelta(minutes=2)) == 1
    events, _ = repository.history_for_date("2026-09-10")
    assert events[0]["status"] == "TG2_HIT"
    assert events[0]["max_target_hit"] == 2

    repository.record_scan([_signal("EXPIRE", 100.0)], captured_at + timedelta(minutes=3))
    assert repository.expire_open_events(captured_at + timedelta(hours=6)) == 1
    events, _ = repository.history_for_date("2026-09-10")
    expiry_event = next(event for event in events if event["symbol"] == "EXPIRE")
    assert expiry_event["status"] == "EXPIRED"
    assert expiry_event["exitPrice"] == 100.0


def test_previous_day_events_are_hidden_after_rollover(tmp_path):
    repository = SignalRepository(tmp_path / "tracker.sqlite3")
    yesterday = datetime(2026, 9, 10, 10, 0, tzinfo=IST)
    today = yesterday + timedelta(days=1)
    repository.record_scan([_signal()], yesterday)
    repository.record_scan([_signal("TODAY")], today)

    assert repository.archive_previous_history("2026-09-11", retention_days=30) == 1
    old_events, old_total = repository.history_for_date("2026-09-10")
    current_events, current_total = repository.history_for_date("2026-09-11")
    assert old_events == []
    assert old_total == 0
    assert current_total == 1
    assert current_events[0]["symbol"] == "TODAY"


def test_daily_equity_bars_are_upserted_and_returned_chronologically(tmp_path):
    repository = SignalRepository(tmp_path / "tracker.sqlite3")
    bars = [
        {"trade_date": "2026-09-10", "symbol": "RELIANCE", "open": 1400, "high": 1420, "low": 1390, "close": 1410, "volume": 100},
        {"trade_date": "2026-09-11", "symbol": "RELIANCE", "open": 1411, "high": 1430, "low": 1400, "close": 1425, "volume": 120},
    ]

    assert repository.upsert_daily_equity_bars(bars) == 2
    assert repository.upsert_daily_equity_bars([{**bars[0], "close": 1412}]) == 1
    rows = repository.daily_equity_bars_for_symbol("reliance")

    assert [row["trade_date"] for row in rows] == ["2026-09-10", "2026-09-11"]
    assert rows[0]["close"] == 1412.0
    multi = repository.daily_equity_bars_for_symbols(["RELIANCE", "MISSING"])
    assert [row["trade_date"] for row in multi["RELIANCE"]] == ["2026-09-10", "2026-09-11"]
    assert multi["MISSING"] == []
    assert repository.daily_equity_bar_summary()["bars"] == 2



