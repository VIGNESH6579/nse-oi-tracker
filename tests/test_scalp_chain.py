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



def test_underlying_context_accepts_timestamp_ms_normalized_to_ist(monkeypatch):
    from datetime import datetime
    candles = [
        {"timestamp_ms": 1791518100000, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 100},
        {"timestamp_ms": 1791518400000, "open": 100, "high": 102, "low": 100, "close": 101, "volume": 100},
        {"timestamp_ms": 1791520500000, "open": 102, "high": 103, "low": 102, "close": 103, "volume": 100},
        {"timestamp_ms": 1791520800000, "open": 103, "high": 105, "low": 103, "close": 104, "volume": 100},
    ]

    class CandleStream(RankingStream):
        def recent_candles_for_symbol(self, symbol, limit=80):
            return candles[-limit:]

    engine = scalp.ScalpChainEngine(FakeMarket(), CandleStream(), FakeRepo())
    monkeypatch.setattr(scalp, "now_ist", lambda: datetime.fromisoformat("2026-10-09T10:00:00+05:30"))
    ctx = engine._underlying_context("AAA")
    assert ctx is not None
    assert ctx["data_frequency"] == "FIVE_MINUTE"
    assert ctx["candle_source"] == "angel_one_websocket_v2"
    assert ctx["direction"] == "BUY"


def test_candidate_contexts_reports_early_rejections(monkeypatch):
    class EmptyCandleStream(RankingStream):
        def recent_candles_for_symbol(self, symbol, limit=80):
            return []

    engine = scalp.ScalpChainEngine(FakeMarket(), EmptyCandleStream(), FakeRepo())
    monkeypatch.setattr(scalp, "cached_universe", lambda: {"AAA", "BBB"})
    monkeypatch.setattr(engine, "_underlying_context", lambda symbol: setattr(engine, "_last_context_rejection", "insufficient_candle_count") or None)
    candidates, stats = engine._candidate_contexts()
    assert candidates == []
    assert stats["early_rejections"] == {"insufficient_candle_count": 2}
    assert stats["early_rejected_symbols"] == {"AAA": "insufficient_candle_count", "BBB": "insufficient_candle_count"}
