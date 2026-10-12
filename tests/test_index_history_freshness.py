import asyncio
from datetime import date, datetime

import app.main as main
from utils.time import IST


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


def test_preopen_index_repair_retries_stale_history_and_snapshots_only_after_recovery(monkeypatch):
    now = datetime(2026, 10, 13, 8, 55, tzinfo=IST)
    summaries = iter([
        {"bars": 172, "symbols": 4, "latest_trade_date": "2026-10-09"},
        {"bars": 176, "symbols": 4, "latest_trade_date": "2026-10-12"},
    ])
    called = {"ingest": 0, "snapshot": 0}
    monkeypatch.setattr(main, "now_ist", lambda: now)
    monkeypatch.setattr(main, "is_trading_holiday", lambda day: False)
    monkeypatch.setattr(main, "expected_latest_bhavcopy_date", lambda observed=None: date(2026, 10, 12))
    monkeypatch.setattr(main.repository, "daily_index_bar_summary", lambda: next(summaries))

    async def fake_ingest():
        called["ingest"] += 1
        return 4

    def fake_snapshot(path):
        called["snapshot"] += 1

    monkeypatch.setattr(main, "ingest_daily_index_bars", fake_ingest)
    monkeypatch.setattr(main, "upload_database_snapshot", fake_snapshot)

    asyncio.run(main.scheduled_index_history_repair())

    assert called == {"ingest": 1, "snapshot": 1}


def test_preopen_index_repair_skips_current_history(monkeypatch):
    now = datetime(2026, 10, 13, 8, 55, tzinfo=IST)
    called = {"ingest": 0}
    monkeypatch.setattr(main, "now_ist", lambda: now)
    monkeypatch.setattr(main, "is_trading_holiday", lambda day: False)
    monkeypatch.setattr(main, "expected_latest_bhavcopy_date", lambda observed=None: date(2026, 10, 12))
    monkeypatch.setattr(
        main.repository,
        "daily_index_bar_summary",
        lambda: {"bars": 176, "symbols": 4, "latest_trade_date": "2026-10-12"},
    )

    async def fake_ingest():
        called["ingest"] += 1
        return 0

    monkeypatch.setattr(main, "ingest_daily_index_bars", fake_ingest)
    asyncio.run(main.scheduled_index_history_repair())

    assert called["ingest"] == 0
