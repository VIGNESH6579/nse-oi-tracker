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
    monkeypatch.setattr(engine, "_candidate_contexts", lambda: called.__setitem__("candidates", called["candidates"] + 1) or ([], {}))
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
    ranked, gainers, losers, gainer_symbols, loser_symbols = engine._ranked_universe()
    assert ranked == ["AAA", "BBB"]
    assert (gainers, losers) == (1, 1)
    assert gainer_symbols == ["AAA"]
    assert loser_symbols == ["BBB"]



def test_underlying_context_accepts_timestamp_ms_normalized_to_ist(monkeypatch):
    from datetime import datetime
    candles = [
        {"timestamp_ms": 1791517500000, "data_frequency": "FIVE_MINUTE", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 100},
        {"timestamp_ms": 1791517800000, "data_frequency": "FIVE_MINUTE", "open": 100, "high": 102, "low": 100, "close": 101, "volume": 100},
        {"timestamp_ms": 1791519900000, "data_frequency": "FIVE_MINUTE", "open": 102, "high": 103, "low": 102, "close": 103, "volume": 100},
        {"timestamp_ms": 1791520200000, "data_frequency": "FIVE_MINUTE", "open": 103, "high": 105, "low": 103, "close": 104, "volume": 100},
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


def test_documented_scalp_env_names_take_precedence(monkeypatch):
    monkeypatch.setenv("SCALP_ENTRY_START", "09:35")
    monkeypatch.setenv("ENTRY_START", "09:30")
    monkeypatch.setenv("SCALP_ENTRY_END", "14:55")
    monkeypatch.setenv("ENTRY_END", "15:00")
    monkeypatch.setenv("SCALP_MAX_HOLD_MINUTES", "8")
    monkeypatch.setenv("MAX_HOLD_MINUTES", "10")
    monkeypatch.setenv("SCALP_MAX_OPEN", "2")
    monkeypatch.setenv("SCALP_MAX_CONCURRENT", "3")
    import importlib
    importlib.reload(scalp)
    assert scalp.ENTRY_START == "09:35"
    assert scalp.ENTRY_END == "14:55"
    assert scalp.MAX_HOLD_MINUTES == 8
    assert scalp.MAX_CONCURRENT == 2
    importlib.reload(scalp)
