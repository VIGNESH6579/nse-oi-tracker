from datetime import datetime

import signal_engine.scalp_chain as scalp
from utils.time import IST


class FakeMarket:
    configured = True


class FakeStream:
    def ensure_symbols(self, symbols):
        return 0


class FakeRepo:
    def open_scalps(self, trade_date):
        return []


def test_ban_list_unavailable_does_not_block_scalp_refresh(monkeypatch):
    engine = scalp.ScalpChainEngine(FakeMarket(), FakeStream(), FakeRepo())
    monkeypatch.setattr(scalp, "now_ist", lambda: datetime(2026, 10, 7, 10, 0, tzinfo=IST))
    monkeypatch.setattr(scalp, "ban_info", lambda: {"ban_list_ok": False})
    monkeypatch.setattr(scalp, "banned_symbols", lambda: set())
    called = {"candidates": 0}
    monkeypatch.setattr(engine, "_candidate_contexts", lambda: called.__setitem__("candidates", called["candidates"] + 1) or [])
    engine.refresh()
    assert called["candidates"] == 1
    assert engine._last_error == "fo_ban_list_unavailable"
