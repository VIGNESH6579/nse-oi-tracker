import time
from datetime import date, datetime, timedelta

import app.main as main
import app.oi_analyzer as oi
from analytics.intraday_confirm import summarize_candles
from analytics.oi_window import OIWindow
from collector import backfill, universe
from database.repository import SignalRepository
from signal_engine.confirmation import GateConfig, evaluate_gate
from utils.time import IST

T0 = datetime(2026, 9, 21, 9, 20, tzinfo=IST)


def _c(hhmm, o, h, l, c, v):
    return {"time": f"2026-09-21T{hhmm}:00+0530", "open": o, "high": h, "low": l, "close": c, "volume": v}


def _base(**over):
    args = dict(
        signal={"signal": "LONG_BUILDUP", "ltp": 104.5},
        oi_ctx={"history_minutes": 30, "window_signal": "LONG_BUILDUP", "streak": 5, "h15": {"oi_pct": 1.0}},
        intraday={"available": True, "vwap": 102.0, "or_high": 104.0, "or_low": 99.0, "or_complete": True,
                  "day_open": 100.0, "last_minute": 9 * 60 + 55, "session_volume": 30000},
        atr14=5.0, avg_volume=50000, prev_close=99.5, ema20=101, ema50=99, banned=False, market_bias="BULL",
        now=T0, cfg=GateConfig())
    args.update(over)
    return args


def test_missing_bars_reports_one_clear_reason_not_atr_and_volume_noise():
    r = evaluate_gate(**_base(atr14=None, avg_volume=None, has_bars=False))
    assert "no_daily_bars" in r["missing_confirmations"]
    assert "atr_unavailable" not in r["missing_confirmations"] and "rel_volume_unavailable" not in r["missing_confirmations"]


def test_index_symbols_pass_without_volume_using_twap_and_index_atr():
    candles = [_c("09:15", 100, 102, 99, 101, 0), _c("09:20", 101, 103, 100, 102, 0), _c("09:25", 102, 104, 101, 103, 0),
               _c("09:30", 103, 105, 102, 104, 0), _c("09:35", 104, 106, 103, 105, 0)]
    s = summarize_candles(candles)
    assert s["vwap"] is None and 100 < s["twap"] < 105 and s["session_volume"] == 0
    intraday = {**s, "vwap": s["twap"], "available": True}
    good = _base(signal={"signal": "LONG_BUILDUP", "ltp": 105.0}, intraday=intraday, atr14=5.0, is_index=True)
    r = evaluate_gate(**good)
    assert r["actionable"], r["missing_confirmations"]
    stock = evaluate_gate(**{**good, "is_index": False})
    assert "volume_too_low" in stock["missing_confirmations"] or "rel_volume_unavailable" in stock["missing_confirmations"]


def test_oi_window_restarts_symbol_when_data_source_changes():
    w = OIWindow()
    for i in range(0, 20, 2):
        w.update("TCS", T0 + timedelta(minutes=i), 100, 1000 + i, source="nse")
    assert w.context("TCS")["points"] == 10
    w.update("TCS", T0 + timedelta(minutes=22), 100, 400, source="angel")     # different OI definition
    assert w.context("TCS")["points"] == 1                                    # no fake OI drop across the switch
    w.update("TCS", T0 + timedelta(minutes=24), 100, 401, source="angel")
    assert w.context("TCS")["points"] == 2


def test_universe_extra_symbols_exclude_indices_and_tests():
    universe._extra.clear()
    universe._cache = None
    try:
        assert universe.add_extra_symbols(["TMPV", "tmcv", "NIFTY", "011NSETEST", ""]) == 2
        assert {"TMPV", "TMCV"} <= universe.cached_universe() and universe.add_extra_symbols(["TMPV"]) == 0
        assert universe.cached_universe_size() == 2
    finally:
        universe._extra.clear()


def test_backfill_symbol_gaps_keeps_only_requested_symbols(tmp_path, monkeypatch):
    repo = SignalRepository(tmp_path / "g.sqlite3")
    monkeypatch.setattr(backfill, "collect_equity_bhavcopy", lambda d: [
        {"trade_date": d.isoformat(), "symbol": s, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10}
        for s in ("TMPV", "TCS", "JUNK")])
    result = backfill.backfill_symbol_gaps(repo, {"TMPV"}, end_date=date(2026, 9, 18), dates=3, delay_seconds=0)
    assert result["downloaded"] == 3 and result["stored"] == 3 and result["failed"] == 0
    assert repo.symbols_missing_bars(["TMPV"], 3) == [] and repo.symbols_missing_bars(["TCS"], 1) == ["TCS"]
    assert backfill.backfill_symbol_gaps(repo, set(), end_date=date(2026, 9, 18))["requested"] == 0


def test_maybe_fill_feed_gaps_finds_feed_names_missing_from_universe(tmp_path, monkeypatch):
    repo = SignalRepository(tmp_path / "f.sqlite3")
    repo.upsert_daily_equity_bars([{"trade_date": f"2026-08-{d:02d}", "symbol": "TCS", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10}
                                   for d in range(1, 20)])
    seen = {}
    monkeypatch.setattr(main, "repository", repo)
    monkeypatch.setattr(main, "render_startup_backfill_enabled", lambda: True)
    monkeypatch.setattr(main, "cached_universe", lambda: {"TCS"})
    monkeypatch.setattr(oi, "_feed_symbols", {"TCS", "TMPV"})
    monkeypatch.setattr(main, "backfill_symbol_gaps", lambda repo_, syms, **kw: seen.update(symbols=set(syms)) or {"stored": 5})
    monkeypatch.setattr(main, "add_extra_symbols", lambda syms: seen.update(extras=set(syms)))
    main._gap_fill.update(running=False, last_start=None, symbols=[], result=None, gave_up=set())
    assert main.maybe_fill_feed_gaps() is True
    for _ in range(50):
        if not main._gap_fill["running"]:
            break
        time.sleep(0.05)
    assert seen == {"extras": {"TMPV"}, "symbols": {"TMPV"}} and main._gap_fill["result"] == {"stored": 5}
    assert main.maybe_fill_feed_gaps() is False          # rate limited: not again within 20 minutes


def test_gate_stats_record_failing_symbols(monkeypatch):
    monkeypatch.setattr(main, "banned_symbols", lambda: frozenset())
    monkeypatch.setattr(main, "_market_bias_cached", lambda: "BULL")
    monkeypatch.setattr(main, "now_ist", lambda: datetime(2026, 9, 21, 10, 0, tzinfo=IST))
    sig = {"symbol": "TMPV", "signal": "LONG_BUILDUP", "ltp": 104.5, "oi_window": {}, "intraday_context": None, "technical_context": {}}
    main._apply_confirmation_gate([sig], {})
    assert "no_daily_bars" in main._gate_stats["failed_symbols"]["TMPV"]


def test_merge_bars_does_not_erase_stock_history_regression(tmp_path):
    """Regression: dict.update() with the index result (empty lists) blanked every stock's bars."""
    repo = SignalRepository(tmp_path / "m.sqlite3")
    repo.upsert_daily_equity_bars([{"trade_date": f"2026-08-{d:02d}", "symbol": "PATANJALI", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10}
                                   for d in range(1, 21)])
    with repo._connect() as c:
        c.executemany("INSERT INTO daily_index_bars(trade_date, symbol, open, high, low, close) VALUES (?,?,?,?,?,?)",
                      [(f"2026-08-{d:02d}", "NIFTY", 1, 2, 1, 2) for d in range(1, 21)])
    syms = ["PATANJALI", "NIFTY", "NIFTYFPI"]
    equity, index = repo.daily_equity_bars_for_symbols(syms), repo.daily_index_bars_for_symbols(syms)
    naive = dict(equity); naive.update(index)
    assert len(naive["PATANJALI"]) == 0                       # the old behaviour (bug)
    merged = main._merge_bars(equity, index)
    assert len(merged["PATANJALI"]) == 20 and len(merged["NIFTY"]) == 20 and merged["NIFTYFPI"] == []


def test_gap_fill_gives_up_on_symbols_with_no_bhavcopy_rows(tmp_path, monkeypatch):
    repo = SignalRepository(tmp_path / "gu.sqlite3")
    calls = []
    monkeypatch.setattr(main, "repository", repo)
    monkeypatch.setattr(main, "render_startup_backfill_enabled", lambda: True)
    monkeypatch.setattr(main, "cached_universe", lambda: {"AAA"})
    monkeypatch.setattr(oi, "_feed_symbols", set())
    monkeypatch.setattr(main, "backfill_symbol_gaps", lambda repo_, syms, **kw: calls.append(set(syms)) or {"stored": 0})
    monkeypatch.setattr(main, "add_extra_symbols", lambda syms: 0)
    main._gap_fill.update(running=False, last_start=None, symbols=[], result=None, gave_up=set())
    assert main.maybe_fill_feed_gaps() is True
    for _ in range(60):
        if not main._gap_fill["running"]:
            break
        time.sleep(0.05)
    assert calls == [{"AAA"}] and main._gap_fill["gave_up"] == {"AAA"}
    main._gap_fill["last_start"] = None
    assert main.maybe_fill_feed_gaps() is False                # nothing left to try
