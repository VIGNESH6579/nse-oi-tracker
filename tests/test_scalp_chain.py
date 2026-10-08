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

class RankingStream:
    def __init__(self):
        self.ticks = {"AAA": {"ltp": 110, "close": 100}, "BBB": {"ltp": 90, "close": 100}, "CCC": {"ltp": 105, "close": 100}, "DDD": {"ltp": 95, "close": 100}}
    def ensure_symbols(self, symbols):
        return len(symbols)
    def latest_tick_for_symbol(self, symbol, max_age_s=15.0):
        return self.ticks.get(symbol)


def test_dynamic_ranking_uses_top_gainers_and_losers(monkeypatch):
    engine = scalp.ScalpChainEngine(FakeMarket(), RankingStream(), FakeRepo())
    monkeypatch.setattr(scalp, "cached_universe", lambda: {"AAA", "BBB", "CCC", "DDD"})
    monkeypatch.setattr(scalp, "RANK_TOP_N", 1)
    ranked, stats = engine._ranked_universe()
    assert ranked == ["AAA", "BBB"]
    assert stats == {"ranked_universe": 4, "gainers": 1, "losers": 1}
