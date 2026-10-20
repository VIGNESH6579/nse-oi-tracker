import asyncio
from datetime import datetime
from types import SimpleNamespace

import app.main as main
from utils.time import IST


def test_daytime_backfill_is_idle_outside_market_hours(monkeypatch):
    calls = []
    async def fake_backfill(**kwargs):
        calls.append(kwargs)
        return {}

    monkeypatch.setattr(main, "settings", SimpleNamespace(startup_backfill=True, backfill_target_days=60))
    monkeypatch.setattr(main, "now_ist", lambda: datetime(2026, 10, 20, 22, 0, tzinfo=IST))
    monkeypatch.setattr(main, "is_market_open", lambda: False)
    monkeypatch.setattr(main, "run_backfill", fake_backfill)

    asyncio.run(main.scheduled_backfill_topup())

    assert calls == []


def test_post_close_backfill_runs_explicit_bounded_larger_pass(monkeypatch):
    calls = []
    async def fake_backfill(**kwargs):
        calls.append(kwargs)
        return {}

    target = 60
    monkeypatch.setattr(main, "settings", SimpleNamespace(startup_backfill=True, backfill_target_days=target))
    monkeypatch.setattr(main, "now_ist", lambda: datetime(2026, 10, 19, 16, 5, tzinfo=IST))
    monkeypatch.setattr(main, "run_backfill", fake_backfill)

    asyncio.run(main.scheduled_backfill_topup(post_close=True))

    assert calls == [{"required_days": target, "max_downloads": target}]


def test_post_close_backfill_rejects_wrong_time(monkeypatch):
    calls = []
    async def fake_backfill(**kwargs):
        calls.append(kwargs)
        return {}

    monkeypatch.setattr(main, "settings", SimpleNamespace(startup_backfill=True, backfill_target_days=60))
    monkeypatch.setattr(main, "now_ist", lambda: datetime(2026, 10, 19, 21, 0, tzinfo=IST))
    monkeypatch.setattr(main, "run_backfill", fake_backfill)

    asyncio.run(main.scheduled_backfill_topup(post_close=True))

    assert calls == []
