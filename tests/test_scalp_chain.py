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
    monkeypatch.undo()
    importlib.reload(scalp)



class ExitRepo(FakeRepo):
    def __init__(self):
        self.closed = []

    def close_scalp(self, event_id, **kwargs):
        self.closed.append({"id": event_id, **kwargs})


class ExitStream(RankingStream):
    def latest_tick_for_symbol(self, symbol, max_age_s=15.0):
        return {"ltp": 100.0}


def _open_position(engine, entry_time="2026-10-07T10:00:00+05:30"):
    engine._open["AAA"] = {
        "id": 10,
        "symbol": "AAA",
        "direction": "BUY",
        "strike": 100.0,
        "option_type": "CE",
        "expiry": "27OCT2026",
        "entry_time_ist": entry_time,
        "underlying_entry": 100.0,
        "entry_option_ltp": 5.0,
        "chain_snapshot_id": "test",
    }


def test_max_hold_exit_without_option_quote_is_unpriced_not_breakeven(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T10:11:00+05:30")
    repo = ExitRepo()
    engine = scalp.ScalpChainEngine(FakeMarket(), ExitStream(), repo)
    _open_position(engine)
    monkeypatch.setattr(scalp, "now_ist", lambda: now)
    monkeypatch.setattr(scalp, "MAX_HOLD_MINUTES", 10)
    monkeypatch.setattr(engine, "_underlying_context", lambda symbol: None)
    monkeypatch.setattr(engine, "_chain", lambda symbol, *, spot, force_refresh=False: None)

    engine._manage_open()

    assert len(repo.closed) == 1
    event = repo.closed[0]
    assert event["exit_reason"] == "TIME"
    assert event["exit_option_ltp"] is None
    assert event["underlying_exit"] == 100.0
    assert event["payload"]["pnl_status"] == "UNPRICED"
    assert event["payload"]["exit_quote_unavailable"] is True
    assert "pnl_premium_pct" not in event["payload"]
    assert engine._open == {}


def test_hard_flat_closes_even_without_underlying_context_or_chain_quote(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T15:15:04+05:30")
    repo = ExitRepo()
    engine = scalp.ScalpChainEngine(FakeMarket(), ExitStream(), repo)
    _open_position(engine, entry_time="2026-10-07T15:10:00+05:30")
    monkeypatch.setattr(scalp, "now_ist", lambda: now)
    monkeypatch.setattr(scalp, "MAX_HOLD_MINUTES", 10)
    monkeypatch.setattr(engine, "_underlying_context", lambda symbol: None)
    monkeypatch.setattr(engine, "_chain", lambda symbol, *, spot, force_refresh=False: None)

    engine.refresh()

    assert len(repo.closed) == 1
    assert repo.closed[0]["exit_reason"] == "CLOCK"
    assert repo.closed[0]["exit_option_ltp"] is None
    assert repo.closed[0]["payload"]["pnl_status"] == "UNPRICED"
    assert engine._open == {}


def test_max_hold_uses_exact_contract_quote_when_available(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T10:11:00+05:30")
    repo = ExitRepo()
    engine = scalp.ScalpChainEngine(FakeMarket(), ExitStream(), repo)
    _open_position(engine)
    monkeypatch.setattr(scalp, "now_ist", lambda: now)
    monkeypatch.setattr(scalp, "MAX_HOLD_MINUTES", 10)
    monkeypatch.setattr(engine, "_underlying_context", lambda symbol: None)
    chain = {"expiry": "27OCT2026", "rows": [{"strike": 100.0, "option_type": "CE", "ltp": 6.25}]}
    seen = {}
    def fake_chain(symbol, *, spot, force_refresh=False):
        seen["force_refresh"] = force_refresh
        return chain
    monkeypatch.setattr(engine, "_chain", fake_chain)

    engine._manage_open()

    event = repo.closed[0]
    assert seen["force_refresh"] is True
    assert event["exit_option_ltp"] == 6.25
    assert event["payload"]["pnl_status"] == "QUOTED_PAPER_EXIT"
    assert event["payload"]["pnl_premium_pct"] == 25.0


def test_live_paper_position_has_nonzero_option_risk_levels(monkeypatch):
    engine = scalp.ScalpChainEngine(FakeMarket(), ExitStream(), FakeRepo())
    monkeypatch.setattr(scalp, "now_ist", lambda: datetime.fromisoformat("2026-10-07T10:01:00+05:30"))
    _open_position(engine, entry_time="2026-10-07T10:00:00+05:30")

    result = engine.live()
    pos = result["open"][0]
    assert pos["stop_loss"] == round(5.0 * (1 - scalp.OPTION_STOP_PCT / 100), 2)
    assert pos["target"] == round(5.0 * (1 + scalp.OPTION_TARGET_PCT / 100), 2)
    assert pos["pnl_status"] == "WAITING_FOR_QUOTE"
