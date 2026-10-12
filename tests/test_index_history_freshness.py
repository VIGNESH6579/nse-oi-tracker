from datetime import date

import app.main as main


def test_index_history_refill_flags_nonempty_but_stale_history(monkeypatch):
    monkeypatch.setattr(main, "expected_latest_bhavcopy_date", lambda now=None: date(2026, 10, 12))
    assert main.index_history_backfill_required(
        {"bars": 172, "symbols": 4, "latest_trade_date": "2026-10-09"}
    ) is True


def test_index_history_refill_accepts_current_session_history(monkeypatch):
    monkeypatch.setattr(main, "expected_latest_bhavcopy_date", lambda now=None: date(2026, 10, 12))
    assert main.index_history_backfill_required(
        {"bars": 172, "symbols": 4, "latest_trade_date": "2026-10-12"}
    ) is False


def test_index_history_refill_fails_closed_for_empty_or_missing_summary(monkeypatch):
    monkeypatch.setattr(main, "expected_latest_bhavcopy_date", lambda now=None: date(2026, 10, 12))
    assert main.index_history_backfill_required({"bars": 0, "latest_trade_date": None}) is True
    assert main.index_history_backfill_required({"bars": 172, "latest_trade_date": None}) is True
