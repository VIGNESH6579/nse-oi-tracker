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

    def scalp_events_for_date(self, trade_date):
        return []


def test_ban_list_unavailable_fails_closed_for_new_entries(monkeypatch):
    engine = scalp.ScalpChainEngine(FakeMarket(), FakeStream(), FakeRepo())
    monkeypatch.setattr(scalp, "now_ist", lambda: datetime(2026, 10, 7, 10, 0, tzinfo=IST))
    monkeypatch.setattr(scalp, "ban_info", lambda: {"ban_list_ok": False})
    called = {"candidates": 0}
    monkeypatch.setattr(
        engine,
        "_candidate_contexts",
        lambda: called.__setitem__("candidates", called["candidates"] + 1) or ([], {}),
    )

    engine.refresh()

    assert called["candidates"] == 0
    assert engine._last_error == "fo_ban_list_unavailable"
    assert engine._last_run_stats["ban_list_unavailable"] is True
    assert engine._last_run_stats["top_rejections"] == {"fo_ban_list_unavailable": 1}


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
        {"timestamp_ms": 1791517800000, "data_frequency": "FIVE_MINUTE", "open": 100, "high": 103, "low": 100, "close": 101, "volume": 100},
        {"timestamp_ms": 1791519900000, "data_frequency": "FIVE_MINUTE", "open": 102, "high": 103, "low": 102, "close": 103, "volume": 100},
        {"timestamp_ms": 1791520200000, "data_frequency": "FIVE_MINUTE", "open": 103.9, "high": 104.1, "low": 103.8, "close": 104.0, "volume": 150},
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


def test_candidate_rejection_details_include_volume_measurements(monkeypatch):
    engine = scalp.ScalpChainEngine(FakeMarket(), RankingStream(), FakeRepo())
    monkeypatch.setattr(engine, "_ranked_universe", lambda: (["AAA"], 1, 0, ["AAA"], []))
    monkeypatch.setattr(engine, "_underlying_context", lambda symbol: {
        "direction": None,
        "last_volume": 80,
        "median_volume": 100,
        "relative_volume": 0.8,
        "volume_ok": False,
        "volume_valid": True,
        "volume_source": "angel_one_historical_5m",
        "candle_source": "angel_one_historical_5m",
        "candle_age_s": 42.0,
        "momentum_pct": 0.15,
        "breakout_distance_pct": 0.7,
        "breakout_extension_ok": True,
    })
    monkeypatch.setattr(engine, "_last_context_rejection", "volume_not_confirmed", raising=False)
    candidates, stats = engine._candidate_contexts()
    assert candidates == []
    detail = stats["early_rejection_details"]["AAA"]
    assert detail["reason"] == "volume_not_confirmed"
    assert detail["relative_volume"] == 0.8
    assert detail["volume_valid"] is True
    assert detail["volume_source"] == "angel_one_historical_5m"


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


def test_missing_option_quote_does_not_force_elapsed_time_exit(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T10:11:00+05:30")
    repo = ExitRepo()
    engine = scalp.ScalpChainEngine(FakeMarket(), ExitStream(), repo)
    _open_position(engine)
    monkeypatch.setattr(scalp, "now_ist", lambda: now)
    monkeypatch.setattr(scalp, "MAX_HOLD_MINUTES", 10)
    monkeypatch.setattr(engine, "_underlying_context", lambda symbol: None)
    monkeypatch.setattr(engine, "_chain", lambda symbol, *, spot, force_refresh=False: None)

    engine._manage_open()

    assert repo.closed == []
    assert "AAA" in engine._open


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


def test_elapsed_time_and_target_do_not_close_while_momentum_persists(monkeypatch):
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

    assert seen["force_refresh"] is False
    assert repo.closed == []
    assert "AAA" in engine._open
    assert engine._open["AAA"]["_trailing_armed"] is True
    assert engine._open["AAA"]["_peak_option_ltp"] == 6.25


def test_live_paper_position_has_nonzero_option_risk_levels(monkeypatch):
    engine = scalp.ScalpChainEngine(FakeMarket(), ExitStream(), FakeRepo())
    monkeypatch.setattr(scalp, "now_ist", lambda: datetime.fromisoformat("2026-10-07T10:01:00+05:30"))
    _open_position(engine, entry_time="2026-10-07T10:00:00+05:30")

    result = engine.live()
    pos = result["open"][0]
    assert pos["stop_loss"] == round(5.0 * (1 - scalp.OPTION_STOP_PCT / 100), 2)
    assert pos["target"] == round(5.0 * (1 + scalp.OPTION_TARGET_PCT / 100), 2)
    assert pos["pnl_status"] == "WAITING_FOR_QUOTE"



def test_underlying_context_recovers_missing_opening_range_from_real_history(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T10:00:30+05:30")
    raw_candles = [
        {"time": "2026-10-07T09:15:00+05:30", "open": 102.0, "high": 102.5, "low": 101.5, "close": 102.0, "volume": 100},
        {"time": "2026-10-07T09:20:00+05:30", "open": 102.0, "high": 103.0, "low": 102.0, "close": 102.5, "volume": 100},
        {"time": "2026-10-07T09:25:00+05:30", "open": 102.5, "high": 103.5, "low": 102.4, "close": 103.2, "volume": 100},
        {"time": "2026-10-07T09:30:00+05:30", "open": 103.2, "high": 103.7, "low": 103.1, "close": 103.5, "volume": 100},
        {"time": "2026-10-07T09:35:00+05:30", "open": 103.5, "high": 103.6, "low": 103.4, "close": 103.5, "volume": 100},
        {"time": "2026-10-07T09:55:00+05:30", "open": 103.5, "high": 104.0, "low": 103.4, "close": 103.95, "volume": 120},
        {"time": "2026-10-07T10:00:00+05:30", "open": 103.5, "high": 105.0, "low": 103.4, "close": 104.5, "volume": 130},
    ]

    class HistoryMarket(FakeMarket):
        def __init__(self):
            self.calls = []

        def intraday_candles(self, symbol, *, interval="FIVE_MINUTE", exchange="NSE", days=1):
            self.calls.append((symbol, interval, exchange, days))
            return raw_candles

    class EmptyStream:
        def recent_candles_for_symbol(self, symbol, limit=80):
            return []

    market = HistoryMarket()
    engine = scalp.ScalpChainEngine(market, EmptyStream(), FakeRepo())
    monkeypatch.setattr(scalp, "now_ist", lambda: now)

    ctx = engine._underlying_context("AAA")

    assert ctx is not None
    assert ctx["direction"] == "BUY"
    assert ctx["candle_source"] == "angel_one_historical_5m"
    assert ctx["data_frequency"] == "FIVE_MINUTE"
    assert ctx["volume_ok"] is True
    assert ctx["relative_volume"] >= 1.2
    assert ctx["candle_close_location"] >= scalp.MIN_CANDLE_CLOSE_LOCATION
    assert len(market.calls) == 1
    assert engine._underlying_context("AAA")["direction"] == "BUY"
    assert len(market.calls) == 1  # cached; no repeated historical calls



def test_reload_open_positions_after_database_restore(monkeypatch):
    class MutableRepo(FakeRepo):
        def __init__(self):
            self.rows = []

        def open_scalps(self, trade_date):
            return list(self.rows)

    now = datetime.fromisoformat("2026-10-07T10:00:00+05:30")
    repo = MutableRepo()
    engine = scalp.ScalpChainEngine(FakeMarket(), RankingStream(), repo)
    assert engine._open == {}

    repo.rows = [{
        "id": 42, "symbol": "AAA", "direction": "BUY", "strike": 100.0,
        "option_type": "CE", "expiry": "27OCT2026",
        "entry_time_ist": now.isoformat(), "underlying_entry": 100.0,
        "entry_option_ltp": 5.0, "chain_snapshot_id": "restored-row",
    }]
    monkeypatch.setattr(scalp, "now_ist", lambda: now)
    engine.reload_open_positions()

    assert "AAA" in engine._open
    assert engine._open["AAA"]["id"] == 42


def test_dynamic_universe_unavailable_fails_closed_instead_of_using_fixed_list(monkeypatch):
    engine = scalp.ScalpChainEngine(FakeMarket(), RankingStream(), FakeRepo())
    monkeypatch.setattr(scalp, "cached_universe", lambda: set())

    ranked, gainers, losers, gainer_symbols, loser_symbols = engine._ranked_universe()

    assert ranked == []
    assert (gainers, losers, gainer_symbols, loser_symbols) == (0, 0, [], [])
    assert engine._last_error == "dynamic_fno_universe_unavailable"
    health = engine.health()
    assert health["universe_size"] == 0
    assert health["fixed_universe_size"] == 0


def test_expired_option_chain_cache_is_not_reported_as_healthy():
    engine = scalp.ScalpChainEngine(FakeMarket(), RankingStream(), FakeRepo())
    engine._chain_cache["AAA"] = (scalp.time.monotonic() - scalp.CHAIN_TTL_S - 1, {"ok": True})

    health = engine.health()

    assert health["chain_ok"] is False
    assert health["chain_cache_fresh_symbols"] == 0


def test_momentum_weakness_requires_two_consecutive_weak_observations(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T10:30:00+05:30")
    engine = scalp.ScalpChainEngine(FakeMarket(), ExitStream(), ExitRepo())
    _open_position(engine, entry_time="2026-10-07T10:00:00+05:30")
    monkeypatch.setattr(scalp, "now_ist", lambda: now)
    weak = {"ltp": 99.5, "vwap": 100.0, "or_high": 101.0, "or_low": 98.0, "momentum_pct": -0.25}

    assert engine._exit_reason(engine._open["AAA"], weak, 5.50, now) is None
    assert engine._open["AAA"]["_weak_signal_count"] == 1
    assert engine._exit_reason(engine._open["AAA"], weak, 5.45, now) == "MOMENTUM_WEAK"


def test_trailing_exit_requires_profit_retrace_and_weak_momentum(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T10:30:00+05:30")
    engine = scalp.ScalpChainEngine(FakeMarket(), ExitStream(), ExitRepo())
    _open_position(engine, entry_time="2026-10-07T10:00:00+05:30")
    position = engine._open["AAA"]
    position["_peak_option_ltp"] = 6.50
    position["_trailing_armed"] = True
    monkeypatch.setattr(scalp, "now_ist", lambda: now)
    weak = {"ltp": 99.5, "vwap": 100.0, "or_high": 101.0, "or_low": 98.0, "momentum_pct": -0.25}

    assert engine._exit_reason(position, weak, 5.65, now) == "TRAILING_MOMENTUM_WEAK"


def test_ranked_universe_interleaves_strong_gainers_and_losers(monkeypatch):
    stream = RankingStream()
    stream.ticks = {
        "GAIN_BIG": {"ltp": 120, "close": 100},
        "LOSS_BIG": {"ltp": 80, "close": 100},
        "GAIN_SMALL": {"ltp": 102, "close": 100},
        "LOSS_SMALL": {"ltp": 98, "close": 100},
    }
    engine = scalp.ScalpChainEngine(FakeMarket(), stream, FakeRepo())
    monkeypatch.setattr(scalp, "cached_universe", lambda: set(stream.ticks))
    monkeypatch.setattr(scalp, "RANK_TOP_N", 2)

    ranked, gainers, losers, _, _ = engine._ranked_universe()

    assert ranked == ["GAIN_BIG", "LOSS_BIG", "GAIN_SMALL", "LOSS_SMALL"]
    assert (gainers, losers) == (2, 2)


def test_opening_range_budget_deferral_is_not_misreported_as_missing_range(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T10:00:30+05:30")
    candles = [
        {"time": f"2026-10-07T09:{minute:02d}:00+05:30", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 100,
         "data_frequency": "FIVE_MINUTE", "candle_source": "angel_one_websocket_v2"}
        for minute in (35, 40, 45, 50, 55)
    ]

    class NoOpeningRangeStream(RankingStream):
        def recent_candles_for_symbol(self, symbol, limit=80):
            return candles[-limit:]

    engine = scalp.ScalpChainEngine(FakeMarket(), NoOpeningRangeStream(), FakeRepo())
    monkeypatch.setattr(scalp, "now_ist", lambda: now)
    monkeypatch.setattr(scalp, "HIST_CANDLE_MAX_CALLS_PER_REFRESH", 2)
    engine._historical_fetches_this_refresh = 2

    assert engine._underlying_context("AAA") is None
    assert engine._last_context_rejection == "historical_fetch_budget_deferred"



def test_chain_confirmation_rejects_falling_directional_oi_even_if_opposite_falls_faster():
    engine = scalp.ScalpChainEngine(FakeMarket(), RankingStream(), FakeRepo())
    engine._previous_chain["AAA"] = {
        "rows": [
            {"strike": 100.0, "option_type": "CE", "ltp": 5.0, "oi": 12000, "volume": 300},
            {"strike": 100.0, "option_type": "PE", "ltp": 5.2, "oi": 15000, "volume": 300},
        ]
    }
    current = {
        "atm_strike": 100.0,
        "rows": [
            {"strike": 100.0, "option_type": "CE", "ltp": 5.5, "oi": 11000, "volume": 400,
             "best_bid": 5.45, "best_ask": 5.50, "bid_qty": 500, "ask_qty": 500},
            {"strike": 100.0, "option_type": "PE", "ltp": 4.9, "oi": 13000, "volume": 400},
        ],
    }

    assert engine._chain_confirmation("AAA", "BUY", current) is None
    assert engine._last_chain_rejection == "option_oi_not_directional"


def test_chain_confirmation_requires_positive_directional_oi_and_premium_follow_through():
    engine = scalp.ScalpChainEngine(FakeMarket(), RankingStream(), FakeRepo())
    engine._previous_chain["AAA"] = {
        "rows": [
            {"strike": 100.0, "option_type": "CE", "ltp": 5.0, "oi": 12000, "volume": 300},
            {"strike": 100.0, "option_type": "PE", "ltp": 5.2, "oi": 15000, "volume": 300},
        ]
    }
    current = {
        "atm_strike": 100.0,
        "rows": [
            {"strike": 100.0, "option_type": "CE", "ltp": 5.5, "oi": 14000, "volume": 400,
             "best_bid": 5.45, "best_ask": 5.50, "bid_qty": 500, "ask_qty": 500},
            {"strike": 100.0, "option_type": "PE", "ltp": 4.9, "oi": 15500, "volume": 400},
        ],
    }

    result = engine._chain_confirmation("AAA", "BUY", current)

    assert result is not None
    assert result["oi_delta"] == 2000
    assert result["opposite_oi_delta"] == 500
    assert result["premium_change_pct"] >= scalp.MIN_PREMIUM_CHANGE_PCT


def test_historical_rate_limit_cools_down_all_symbols(monkeypatch):
    class RateLimitedMarket(FakeMarket):
        def __init__(self):
            self.calls = 0
        def intraday_candles(self, symbol, *, interval="FIVE_MINUTE", exchange="NSE", days=1):
            self.calls += 1
            raise RuntimeError("HTTP 429 rate limit")

    market = RateLimitedMarket()
    engine = scalp.ScalpChainEngine(market, RankingStream(), FakeRepo())
    monkeypatch.setattr(scalp, "HIST_CANDLE_TTL_S", 300.0)
    monkeypatch.setattr(scalp, "HIST_CANDLE_MAX_CALLS_PER_REFRESH", 2)

    assert engine._historical_context_candles("AAA") == []
    assert engine._historical_context_candles("BBB") == []
    assert market.calls == 1
    assert engine._historical_failure_reasons["AAA"][1] == "angel_historical_rate_limited"
    assert engine._historical_failure_reasons["BBB"][1] == "angel_historical_rate_limited"


def test_underlying_context_rejects_breakout_without_relative_volume(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T10:00:30+05:30")
    candles = [
        {"time": "2026-10-07T09:15:00+05:30", "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.0, "volume": 100},
        {"time": "2026-10-07T09:20:00+05:30", "open": 100.0, "high": 101.0, "low": 100.0, "close": 100.5, "volume": 100},
        {"time": "2026-10-07T09:25:00+05:30", "open": 100.5, "high": 101.0, "low": 100.4, "close": 100.8, "volume": 100},
        {"time": "2026-10-07T09:30:00+05:30", "open": 100.8, "high": 101.2, "low": 100.7, "close": 101.0, "volume": 100},
        {"time": "2026-10-07T09:35:00+05:30", "open": 101.0, "high": 101.1, "low": 100.9, "close": 101.0, "volume": 100},
        {"time": "2026-10-07T09:55:00+05:30", "open": 103.0, "high": 104.0, "low": 102.9, "close": 103.5, "volume": 120},
        {"time": "2026-10-07T10:00:00+05:30", "open": 103.5, "high": 105.0, "low": 103.4, "close": 104.5, "volume": 80},
    ]

    class HistoryMarket(FakeMarket):
        def intraday_candles(self, symbol, *, interval="FIVE_MINUTE", exchange="NSE", days=1):
            return candles

    class EmptyStream:
        def recent_candles_for_symbol(self, symbol, limit=80):
            return []

    engine = scalp.ScalpChainEngine(HistoryMarket(), EmptyStream(), FakeRepo())
    monkeypatch.setattr(scalp, "now_ist", lambda: now)

    context = engine._underlying_context("AAA")
    assert context is not None and context["direction"] is None
    assert context["volume_ok"] is False
    assert context["relative_volume"] < scalp.MIN_RELATIVE_VOLUME
    assert engine._last_context_rejection == "volume_not_confirmed"


def test_entry_persists_underlying_and_option_quality_evidence(monkeypatch):
    class EntryRepo(FakeRepo):
        def save_scalp_entry(self, event):
            self.event = event
            return 1

    repo = EntryRepo()
    engine = scalp.ScalpChainEngine(FakeMarket(), RankingStream(), repo)
    monkeypatch.setattr(scalp, "now_ist", lambda: datetime.fromisoformat("2026-10-07T10:00:00+05:30"))
    candidate = {
        "symbol": "AAA", "direction": "BUY", "ltp": 104.5, "day_change_pct": 2.1,
        "vwap": 102.0, "or_high": 101.0, "or_low": 99.5, "momentum_pct": 0.8,
        "last_volume": 150, "median_volume": 100, "relative_volume": 1.5,
        "volume_ok": True, "candle_close_location": 0.8, "candle_body_pct": 0.4,
        "breakout_distance_pct": 0.3, "candle_count": 10, "candle_age_s": 20,
        "candle_source": "angel_one_historical_5m", "data_frequency": "FIVE_MINUTE",
    }
    confirmation = {
        "option": {"strike": 100.0, "option_type": "CE", "ltp": 5.5},
        "premium_change_pct": 2.0, "previous_option_ltp": 5.4,
        "oi_delta": 2000, "opposite_oi_delta": 500, "option_oi": 14000,
        "option_volume": 400, "best_bid": 5.45, "best_ask": 5.50,
        "bid_qty": 500, "ask_qty": 500, "spread_pct": 0.9132,
        "rule": "CHAIN_PREMIUM_RISING_DIRECTIONAL_OI_AND_TIGHT_SPREAD",
    }
    chain = {"expiry": "27OCT2026", "atm_strike": 100.0, "rows": []}

    engine._enter(candidate, confirmation, chain)

    quality = repo.event["signal_quality"]
    assert quality["version"] == "scalp-quality-v2"
    assert quality["underlying"]["relative_volume"] == 1.5
    assert quality["option"]["directional_oi_delta"] == 2000
    assert quality["spread_check"] == "passed"
    assert quality["option"]["spread_pct"] == 0.9132



def test_chain_confirmation_rejects_wide_option_spread():
    engine = scalp.ScalpChainEngine(FakeMarket(), RankingStream(), FakeRepo())
    engine._previous_chain["AAA"] = {
        "rows": [
            {"strike": 100.0, "option_type": "CE", "ltp": 5.0, "oi": 12000, "volume": 300},
            {"strike": 100.0, "option_type": "PE", "ltp": 5.2, "oi": 15000, "volume": 300},
        ]
    }
    current = {
        "atm_strike": 100.0,
        "rows": [
            {"strike": 100.0, "option_type": "CE", "ltp": 5.5, "oi": 14000, "volume": 400,
             "best_bid": 5.30, "best_ask": 5.50, "bid_qty": 500, "ask_qty": 500},
            {"strike": 100.0, "option_type": "PE", "ltp": 4.9, "oi": 15500, "volume": 400},
        ],
    }

    assert engine._chain_confirmation("AAA", "BUY", current) is None
    assert engine._last_chain_rejection == "option_spread_too_wide"



def test_candidate_context_scan_order_rotates_to_share_historical_fetch_budget(monkeypatch):
    engine = scalp.ScalpChainEngine(FakeMarket(), RankingStream(), FakeRepo())
    monkeypatch.setattr(scalp, "HIST_CANDLE_MAX_CALLS_PER_REFRESH", 2)
    monkeypatch.setattr(
        engine,
        "_ranked_universe",
        lambda: (["AAA", "BBB", "CCC", "DDD"], 2, 2, ["AAA", "BBB"], ["CCC", "DDD"]),
    )
    seen = []
    def reject(symbol):
        seen.append(symbol)
        engine._last_context_rejection = "historical_fetch_budget_deferred"
        return None
    monkeypatch.setattr(engine, "_underlying_context", reject)

    engine._candidate_contexts()
    first = list(seen)
    seen.clear()
    engine._candidate_contexts()
    second = list(seen)

    assert first == ["AAA", "BBB", "CCC", "DDD"]
    assert second == ["CCC", "DDD", "AAA", "BBB"]


def test_invalid_websocket_volume_is_repaired_from_real_historical_candles(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T10:00:30+05:30")
    historical = [
        {"time": "2026-10-07T09:15:00+05:30", "open": 102.0, "high": 102.5, "low": 101.5, "close": 102.0, "volume": 100},
        {"time": "2026-10-07T09:20:00+05:30", "open": 102.0, "high": 103.0, "low": 102.0, "close": 102.5, "volume": 100},
        {"time": "2026-10-07T09:25:00+05:30", "open": 102.5, "high": 103.5, "low": 102.4, "close": 103.2, "volume": 100},
        {"time": "2026-10-07T09:30:00+05:30", "open": 103.2, "high": 103.7, "low": 103.1, "close": 103.5, "volume": 100},
        {"time": "2026-10-07T09:35:00+05:30", "open": 103.5, "high": 103.6, "low": 103.4, "close": 103.5, "volume": 100},
        {"time": "2026-10-07T09:55:00+05:30", "open": 103.5, "high": 104.0, "low": 103.4, "close": 103.95, "volume": 120},
        {"time": "2026-10-07T10:00:00+05:30", "open": 103.5, "high": 105.0, "low": 103.4, "close": 104.5, "volume": 130},
    ]
    stream_candles = [
        {**c, "volume": 0, "volume_valid": False, "data_frequency": "FIVE_MINUTE",
         "source": "angel_one_websocket_v2", "candle_source": "angel_one_websocket_v2",
         "observed_at": now.timestamp()}
        for c in historical
    ]

    class HistoryMarket(FakeMarket):
        def __init__(self):
            self.calls = 0
        def intraday_candles(self, symbol, *, interval="FIVE_MINUTE", exchange="NSE", days=1):
            self.calls += 1
            return historical

    class InvalidVolumeStream:
        def recent_candles_for_symbol(self, symbol, limit=80):
            return stream_candles[-limit:]

    market = HistoryMarket()
    engine = scalp.ScalpChainEngine(market, InvalidVolumeStream(), FakeRepo())
    monkeypatch.setattr(scalp, "now_ist", lambda: now)

    context = engine._underlying_context("AAA")

    assert market.calls == 1
    assert context is not None and context["direction"] == "BUY"
    assert context["volume_ok"] is True
    assert context["volume_source"] == "angel_one_historical_5m"
    assert context["relative_volume"] >= 1.2



def test_health_exposes_per_symbol_fresh_tick_coverage(monkeypatch):
    stream = RankingStream()
    engine = scalp.ScalpChainEngine(FakeMarket(), stream, FakeRepo())
    engine._last_run_stats = {
        "gainer_symbols": ["AAA", "CCC"],
        "loser_symbols": ["BBB", "DDD"],
        "at": "2026-10-07T10:00:00+05:30",
    }
    health = engine.health()
    assert health["ranked_symbols_last_scan"] == 4
    assert health["ranked_symbols_fresh_ticks_now"] == 4
    assert health["ranked_symbols_without_fresh_ticks_now"] == 0
    assert health["ranked_symbol_tick_freshness"]["AAA"]["status"] == "fresh"



def test_entry_window_uses_exact_second_boundary():
    start = datetime.fromisoformat("2026-10-07T09:30:00+05:30")
    end = datetime.fromisoformat("2026-10-07T15:00:00+05:30")
    assert scalp._in_window(start, "09:30", "15:00")
    assert scalp._in_window(end, "09:30", "15:00")
    assert not scalp._in_window(end.replace(second=1), "09:30", "15:00")
    assert not scalp._in_window(end.replace(microsecond=1), "09:30", "15:00")



def test_open_trade_monitor_persists_fresh_underlying_and_option_snapshot(monkeypatch):
    now = datetime.fromisoformat("2026-10-07T10:00:30+05:30")

    class MonitorRepo(ExitRepo):
        def __init__(self):
            super().__init__()
            self.snapshots = []

        def record_scalp_monitor_snapshot(self, event_id, **kwargs):
            self.snapshots.append({"event_id": event_id, **kwargs})

    repo = MonitorRepo()
    engine = scalp.ScalpChainEngine(FakeMarket(), ExitStream(), repo)
    _open_position(engine)
    monkeypatch.setattr(scalp, "now_ist", lambda: now)
    monkeypatch.setattr(engine, "_underlying_context", lambda symbol: {
        "ltp": 102.0, "vwap": 101.0, "momentum_pct": 0.4,
        "or_high": 101.0, "or_low": 98.0, "relative_volume": 1.8,
        "volume_ok": True, "last_volume": 180, "median_volume": 100,
        "candle_age_s": 1.0, "candle_source": "angel_one_websocket_v2",
        "volume_source": "angel_one_websocket_v2", "data_frequency": "FIVE_MINUTE",
    })
    monkeypatch.setattr(engine, "_chain", lambda symbol, *, spot, force_refresh=False: {
        "expiry": "27OCT2026",
        "rows": [{
            "strike": 100.0, "option_type": "CE", "ltp": 5.2, "oi": 12000,
            "volume": 500, "best_bid": 5.15, "best_ask": 5.2,
            "bid_qty": 300, "ask_qty": 350,
        }],
    })

    engine._manage_open()

    assert len(repo.snapshots) == 1
    saved = repo.snapshots[0]
    assert saved["event_id"] == 10
    assert saved["status"] == "MONITORING"
    assert saved["snapshot"]["underlying"]["momentum_pct"] == 0.4
    assert saved["snapshot"]["option"]["ltp"] == 5.2
    assert saved["snapshot"]["mark_pnl_pct"] == 4.0



def test_extended_breakout_requires_strong_volume_and_momentum():
    # JSWSTEEL's saved entry: 1.64% extended, only 1.38x volume and 0.37% momentum.
    assert scalp._breakout_extension_ok(1.6395, 1.382, 0.3705) is False
    # A modest extension can pass only when participation and momentum agree.
    assert scalp._breakout_extension_ok(3.3486, 2.432, 0.5513) is True
    assert scalp._breakout_extension_ok(2.6898, 3.067, -1.4118) is True


def test_extended_breakout_has_absolute_cap_even_with_extreme_participation():
    # Recent VMM/SRF entries were 4.35% and 5.11% beyond the opening range.
    # Extremely high volume must not override the hard anti-chase ceiling.
    assert scalp._breakout_extension_ok(4.3478, 37.118, -2.728) is False
    assert scalp._breakout_extension_ok(5.1118, 38.43, -2.0) is False


def test_breakout_extension_rejects_non_finite_or_invalid_inputs():
    assert scalp._breakout_extension_ok(float("nan"), 10.0, 1.0) is False
    assert scalp._breakout_extension_ok(2.0, float("inf"), 1.0) is False
    assert scalp._breakout_extension_ok(-1.0, 10.0, 1.0) is False
    # A modest breakout remains eligible under the existing confirmation gates.
    assert scalp._breakout_extension_ok(0.7257, 1.20, 0.30) is True
