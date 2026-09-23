import asyncio
from datetime import datetime, timedelta

import pytest

import app.main as main
import app.oi_analyzer as oi
import integrations.angel_one_market_data as angel
from database.repository import SignalRepository, TG1_BOOK_FRACTION, _closed_r
from utils.time import IST

AT = datetime(2026, 9, 21, 10, 0, tzinfo=IST)


def _open(repo, direction="BUY", symbol="AAA"):
    sig = {"symbol": symbol, "signal": "LONG_BUILDUP" if direction == "BUY" else "SHORT_BUILDUP",
           "signal_direction": direction, "ltp": 100.0, "confidence": 90, "price_change_pct": 1.0, "oi_change_pct": 5.0}
    repo.record_scan([sig], AT)
    with repo._connect() as c:
        row = c.execute("SELECT id, entry, stop_loss, target_1, target_2 FROM signal_events WHERE symbol = ?", (symbol,)).fetchone()
    return dict(row)


def _state(repo, event_id):
    with repo._connect() as c:
        return dict(c.execute("SELECT status, result, result_r, exit_price, max_target_hit, result_source FROM signal_events WHERE id = ?", (event_id,)).fetchone())


def _tick(repo, price, symbol="AAA", minutes=2):
    return repo.update_open_events([], AT + timedelta(minutes=minutes), extra_prices={symbol: price})


def test_tg1_then_reversal_is_breakeven_exit_not_a_loss(tmp_path):
    repo = SignalRepository(tmp_path / "a.sqlite3"); e = _open(repo)
    _tick(repo, e["target_1"] + 0.01)
    assert _state(repo, e["id"])["status"] == "TG1_HIT"
    assert _tick(repo, e["entry"] - 0.05, minutes=4) == 1
    s = _state(repo, e["id"])
    risk = e["entry"] - e["stop_loss"]
    r1 = (e["target_1"] - e["entry"]) / risk
    assert s["status"] == "BE_EXIT" and s["result"] == "BE_EXIT" and s["max_target_hit"] == 1
    assert 0 < s["result_r"] < r1 * TG1_BOOK_FRACTION + 1e-6 and s["exit_price"] <= e["entry"]
    assert s["result_source"] == "live_monitor"


def test_stop_before_tg1_is_sl_hit_minus_one_r_and_fills_at_worse_price(tmp_path):
    repo = SignalRepository(tmp_path / "b.sqlite3"); e = _open(repo)
    _tick(repo, e["stop_loss"] - 0.2)                              # gapped through the stop
    s = _state(repo, e["id"])
    assert s["status"] == "SL_HIT" and s["exit_price"] == pytest.approx(e["stop_loss"] - 0.2)
    assert s["result_r"] <= -1.0


def test_tg2_result_books_half_at_tg1_and_half_at_tg2(tmp_path):
    repo = SignalRepository(tmp_path / "c.sqlite3"); e = _open(repo)
    _tick(repo, e["target_2"] + 0.3)
    s = _state(repo, e["id"])
    risk = e["entry"] - e["stop_loss"]
    expected = TG1_BOOK_FRACTION * (e["target_1"] - e["entry"]) / risk + (1 - TG1_BOOK_FRACTION) * (e["target_2"] - e["entry"]) / risk
    assert s["status"] == "TG2_HIT" and s["exit_price"] == e["target_2"] and s["result_r"] == pytest.approx(expected, abs=0.002)


def test_short_trade_mirrors_breakeven_logic(tmp_path):
    repo = SignalRepository(tmp_path / "d.sqlite3"); e = _open(repo, "SELL")
    _tick(repo, e["target_1"] - 0.01)
    assert _state(repo, e["id"])["status"] == "TG1_HIT"
    _tick(repo, e["entry"] + 0.05, minutes=4)
    s = _state(repo, e["id"])
    assert s["status"] == "BE_EXIT" and s["result_r"] > 0 and s["exit_price"] >= e["entry"]


def test_open_trade_keeps_being_monitored_when_symbol_left_the_signal_list(tmp_path):
    repo = SignalRepository(tmp_path / "e.sqlite3"); e = _open(repo)
    assert repo.update_open_events([{"symbol": "OTHER", "ltp": 50.0}], AT + timedelta(minutes=2), extra_prices={"AAA": e["stop_loss"] - 0.1}) == 1
    assert _state(repo, e["id"])["status"] == "SL_HIT"


def test_time_exit_after_tg1_keeps_locked_profit_and_pre_tg1_exit_is_plain_r(tmp_path):
    repo = SignalRepository(tmp_path / "f.sqlite3")
    a, b = _open(repo, symbol="AAA"), _open(repo, symbol="BBB")
    _tick(repo, a["target_1"] + 0.01, symbol="AAA")                 # AAA armed at TG1
    _tick(repo, b["entry"] + 0.1, symbol="BBB")                     # BBB still below TG1
    assert repo.expire_open_events(AT + timedelta(hours=5), result_source="live_exit") == 2
    sa, sb = _state(repo, a["id"]), _state(repo, b["id"])
    assert sa["status"] == "EXPIRED" and sa["result_r"] > 0 and sa["result_source"] == "live_exit"
    assert sb["status"] == "EXPIRED" and sb["result_r"] == pytest.approx(0.1 / (b["entry"] - b["stop_loss"]), abs=0.01)
    assert repo.performance_for_date("2026-09-21")["total_r"] == pytest.approx(sa["result_r"] + sb["result_r"], abs=0.01)


def test_closed_r_formula():
    assert _closed_r("SL_HIT", "BUY", 100, 99, 101, 102, 99, False) == -1.0
    assert _closed_r("TG2_HIT", "BUY", 100, 99, 101, 102, 102, True) == pytest.approx(0.5 * 1 + 0.5 * 2)
    assert _closed_r("BE_EXIT", "SELL", 100, 101, 99, 98, 100, True) == pytest.approx(0.5)


def test_monitor_job_closes_trades_from_window_prices_without_any_dashboard_visit(tmp_path, monkeypatch):
    repo = SignalRepository(tmp_path / "g.sqlite3"); e = _open(repo)
    monkeypatch.setattr(main, "repository", repo)
    monkeypatch.setattr(main, "angel_market_data", None)
    monkeypatch.setattr(main, "now_ist", lambda: datetime(2026, 9, 21, 10, 5, tzinfo=IST))
    oi.oi_window.reset()
    oi.oi_window.update("AAA", datetime(2026, 9, 21, 10, 4, tzinfo=IST), e["stop_loss"] - 0.5, 1000)
    asyncio.run(main.scheduled_trade_monitor())
    assert _state(repo, e["id"])["status"] == "SL_HIT"
    monkeypatch.setattr(main, "now_ist", lambda: datetime(2026, 9, 21, 16, 0, tzinfo=IST))   # outside market hours: no-op
    e2 = _open(repo, symbol="CCC")
    oi.oi_window.update("CCC", datetime(2026, 9, 21, 15, 59, tzinfo=IST), e2["stop_loss"] - 1, 1000)
    asyncio.run(main.scheduled_trade_monitor())
    assert _state(repo, e2["id"])["status"] == "OPEN"


def _feed(symbol, ltp, oi_value):
    return {"symbol": symbol, "ltp": ltp, "latestOI": oi_value, "changeInOI": 0, "oiChangePct": 0.05, "pChange": 0.05, "volume": 10}


def test_window_candidates_find_fresh_buildups_that_day_change_filter_misses(monkeypatch):
    oi.oi_window.reset(); oi._feed_symbols.clear()
    t0 = datetime(2026, 9, 21, 11, 0, tzinfo=IST)
    clock = {"i": 0}
    monkeypatch.setattr(oi, "now_ist", lambda: t0 + timedelta(minutes=2 * clock["i"]))

    def rows():
        i = clock["i"]
        return [_feed("FRESH", 100.0 * (1 + 0.0009 * i), 9_000_000 * (1 + 0.0012 * i)),        # ~+0.5%/15m price, +0.9%/15m OI
                _feed("FLAT", 100.0, 9_000_000),
                _feed("WEAKOI", 100.0 * (1 + 0.0009 * i), 9_000_000 * (1 + 0.00002 * i)),
                _feed("NIFTYFPI", 100.0 * (1 + 0.0009 * i), 9_000_000 * (1 + 0.0012 * i))]
    monkeypatch.setattr(oi, "fetch_all_fno_oi_change", rows)
    results = []
    for i in range(0, 16):
        clock["i"] = i
        results = oi.scan_all_fno_realtime()
    names = {r["symbol"]: r for r in results}
    assert "FRESH" in names and names["FRESH"]["candidate_origin"] == "window_15m" and names["FRESH"]["signal"] == "LONG_BUILDUP"
    assert names["FRESH"]["oi_window"]["streak"] >= 4 and names["FRESH"]["signal_direction"] == "BUY"
    assert "FLAT" not in names and "WEAKOI" not in names and "NIFTYFPI" not in names


class _Resp:
    status_code = 403
    text = "Access denied because of exceeding access rate"
    def raise_for_status(self):
        raise RuntimeError("HTTP Error 403")


def test_angel_historical_api_cools_down_after_403_instead_of_hammering(monkeypatch):
    client = angel.AngelOneMarketData(api_key="k", client_code="c", password="p", totp_secret="JBSWY3DPEHPK3PXP")
    client._login = lambda: None
    client._headers = lambda: {}
    client.instrument = lambda symbol, exchange="NSE": angel.AngelInstrument("TCS", "11536", "NSE")
    calls = []
    monkeypatch.setattr(angel.requests, "post", lambda *a, **k: calls.append(1) or _Resp())
    monkeypatch.setenv("ANGEL_HIST_MIN_INTERVAL", "0")
    with pytest.raises(RuntimeError, match="exceeding access rate"):        # body snippet is surfaced for diagnosis
        client.intraday_candles("TCS")
    with pytest.raises(angel.AngelUnavailable):
        client.intraday_candles("TCS")
    assert len(calls) == 1                                          # second call never left the process


# ---------------- quote-based intraday context (no candle API) ----------------
from analytics.intraday_confirm import quote_context
from analytics.oi_window import OIWindow


def test_opening_range_is_tracked_from_scan_prices_and_needs_coverage():
    w = OIWindow()
    day = datetime(2026, 9, 21, tzinfo=IST)
    for minute, price in ((556, 100.0), (560, 101.5), (564, 99.5), (568, 100.8)):        # 09:16 .. 09:28
        w.update("TCS", day.replace(hour=minute // 60, minute=minute % 60), price, 1000)
    w.update("TCS", day.replace(hour=9, minute=40), 105.0, 1000)                         # after the range: ignored
    rng = w.opening_range("TCS")
    assert rng == {"high": 101.5, "low": 99.5, "span_min": 12, "samples": 4}
    assert w.opening_range("NOPE") is None
    w.update("LATE", day.replace(hour=9, minute=29), 50.0, 1000)
    assert w.opening_range("LATE")["span_min"] == 0                                       # restarted at 09:29: not covered


def test_quote_context_uses_exchange_vwap_and_flags_incomplete_opening_range():
    quote = {"avg_price": 102.0, "open": 100.0, "high": 106.0, "low": 99.0, "ltp": 105.0, "volume": 30000}
    full = quote_context(quote, {"high": 104.0, "low": 99.0, "span_min": 12, "samples": 6}, 10 * 60 + 5)
    assert full["vwap"] == 102.0 and full["or_complete"] is True and full["last_minute"] == 10 * 60 and full["source"] == "angel_quote"
    assert quote_context(quote, None, 10 * 60)["or_complete"] is False                  # e.g. right after a mid-day restart
    assert quote_context(quote, {"high": 1, "low": 1, "span_min": 3, "samples": 2}, 10 * 60)["or_complete"] is False
    assert quote_context({"avg_price": 0, "open": 100}, None, 600) is None             # index / missing avgPrice


def test_gate_passes_a_setup_using_only_quote_context(monkeypatch):
    monkeypatch.setattr(main, "banned_symbols", lambda: frozenset())
    monkeypatch.setattr(main, "_market_bias_cached", lambda: "BULL")
    monkeypatch.setattr(main, "now_ist", lambda: datetime(2026, 9, 21, 10, 0, tzinfo=IST))
    ctx = quote_context({"avg_price": 102.0, "open": 100.0, "high": 106.0, "low": 99.0, "ltp": 104.5, "volume": 45000},
                        {"high": 104.0, "low": 99.0, "span_min": 12, "samples": 6}, 10 * 60)
    sig = {"symbol": "OKAY", "signal": "LONG_BUILDUP", "ltp": 104.5,
           "oi_window": {"history_minutes": 30, "window_signal": "LONG_BUILDUP", "streak": 5, "h15": {"oi_pct": 1.0}},
           "intraday_context": ctx, "technical_context": {"atr14": 5.0, "ema20": 101, "ema50": 99}}
    bars = {"OKAY": [{"trade_date": f"2026-08-{d:02d}", "close": 99.5, "volume": 50000} for d in range(1, 21)]}
    out = main._apply_confirmation_gate([sig], bars)[0]
    assert out["actionable"], out["missing_confirmations"]


# ---------------- tiers: every signal is tracked automatically ----------------
def _failed(symbol, direction="BUY", price=100.0):
    return {"symbol": symbol, "signal": "LONG_BUILDUP" if direction == "BUY" else "SHORT_BUILDUP", "signal_direction": direction,
            "ltp": price, "confidence": 80, "price_change_pct": 1.0, "oi_change_pct": 5.0,
            "confirmation_gate": "FAILED", "actionable": False, "missing_confirmations": ["oi_window_disagrees"], "quality_score": 20}


def _passed(symbol, direction="BUY", price=100.0):
    return {**_failed(symbol, direction, price), "confirmation_gate": "PASSED", "actionable": True, "missing_confirmations": [], "quality_score": 80}


def _rows(repo, sql="SELECT symbol, tier, status FROM signal_events ORDER BY id"):
    with repo._connect() as c:
        return [tuple(r) for r in c.execute(sql).fetchall()]


def test_gate_failed_signal_is_not_tracked_or_graded(tmp_path):
    repo = SignalRepository(tmp_path / "w.sqlite3")
    for minute in (0, 2, 4, 6):                                       # same candidate seen on 4 scans
        repo.record_scan([_failed("WATCHME", price=100.0 + minute * 0.01)], AT + timedelta(minutes=minute))
    assert _rows(repo) == []


def test_rejected_candidates_do_not_consume_trade_caps(tmp_path):
    repo = SignalRepository(tmp_path / "t.sqlite3")
    repo.record_scan([_failed("W1"), _failed("W2"), _failed("W3")], AT)
    repo.record_scan([_passed("TRADE1")], AT + timedelta(minutes=2))
    assert [r[1] for r in _rows(repo)] == ["TRADE"]
    repo.record_scan([_passed("W1")], AT + timedelta(minutes=4))
    assert [(r[0], r[1]) for r in _rows(repo)] == [("TRADE1", "TRADE"), ("W1", "TRADE")]
    trade = repo.performance_for_date("2026-09-21", "TRADE")
    assert trade["signals_generated"] == 2


def test_rejected_rows_are_not_created_late_or_early(tmp_path):
    repo = SignalRepository(tmp_path / "l.sqlite3")
    repo.record_scan([_failed("LATE")], AT.replace(hour=15, minute=5))
    assert _rows(repo) == []
    repo.record_scan([_failed("EARLY")], AT)
    events, total = repo.history_for_date("2026-09-21")
    assert total == 0 and events == []
    assert repo.backtest_events("2026-09-21", "2026-09-21") == []
    assert repo.backtest_events("2026-09-21", "2026-09-21", tier=None) == []


def test_daily_ingestion_stores_only_the_fno_universe_and_coverage_uses_it(tmp_path, monkeypatch):
    repo = SignalRepository(tmp_path / "i.sqlite3")
    monkeypatch.setattr(main, "repository", repo)
    monkeypatch.setattr(main, "bundled_fno_symbols", lambda: {"AAA", "BBB"})
    monkeypatch.setattr(main, "collect_equity_bhavcopy", lambda d: [
        {"trade_date": d.isoformat(), "symbol": s, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10} for s in ("AAA", "BBB", "JUNK1", "JUNK2")])
    assert asyncio.run(main.ingest_daily_bhavcopy("2026-09-21")) == 2
    assert repo.daily_equity_bar_summary()["symbols"] == 2
    monkeypatch.setattr(main, "bundled_fno_symbols", lambda: set())          # universe unavailable: fail closed
    assert asyncio.run(main.ingest_daily_bhavcopy("2026-09-22")) == 0
    # coverage: 2 of the 4 universe names have >=1 bar; measured against the universe, not the whole table
    cov = repo.daily_history_coverage(atr_bars=1, ready_bars=2, symbols={"AAA", "BBB", "CCC", "DDD"})
    assert cov["symbols"] == 4 and cov["atr_coverage_pct"] == 50.0 and cov["history_ready_pct"] == 0.0


def test_nifty_regime_from_a_single_quote():
    from analytics.intraday_confirm import bias_from_quote
    assert bias_from_quote({"ltp": 23450, "open": 23330, "close": 23346}) == "BULL"
    assert bias_from_quote({"ltp": 23200, "open": 23330, "close": 23346}) == "BEAR"
    assert bias_from_quote({"ltp": 23335, "open": 23330, "close": 23346}) == "NEUTRAL"
    assert bias_from_quote({"ltp": 0, "open": 1, "close": 1}) == "UNKNOWN"
