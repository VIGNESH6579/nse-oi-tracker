from datetime import datetime, timedelta

from analytics.intraday_confirm import (average_daily_volume, bias_from_candles, expected_volume_fraction,
                                         relative_volume, summarize_candles)
from analytics.oi_window import OIWindow, classify
from collector.fno_ban import parse_ban_csv, refresh_ban_list, banned_symbols, ban_info
from signal_engine.confirmation import GateConfig, evaluate_gate
from utils.time import IST

T0 = datetime(2026, 9, 21, 9, 20, tzinfo=IST)


def _candle(hhmm, o, h, l, c, v):
    return {"time": f"2026-09-21T{hhmm}:00+0530", "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_classify_windows():
    assert classify(0.5, 1.0) == "LONG_BUILDUP" and classify(-0.5, 1.0) == "SHORT_BUILDUP"
    assert classify(0.5, -1.0) == "SHORT_COVERING" and classify(-0.5, -1.0) == "LONG_UNWINDING"
    assert classify(0.1, 5.0) == "NEUTRAL"


def test_window_measures_recent_move_not_day_change():
    """SHREECEM pattern: day-change says up, but the last 15 minutes are falling with OI rising."""
    w = OIWindow()
    for i in range(0, 31, 2):                           # 30 minutes of scans, price falling, OI rising
        w.update("SHREECEM", T0 + timedelta(minutes=i), 22650 - i * 8, 100000 + i * 300)
    ctx = w.context("SHREECEM")
    assert ctx["h15"]["price_pct"] < -0.25 and ctx["h15"]["oi_pct"] > 0.3
    assert ctx["window_signal"] == "SHORT_BUILDUP"      # a day-signal of LONG_BUILDUP would NOT agree
    assert ctx["history_minutes"] == 30.0


def test_window_needs_history_and_ignores_duplicates_and_resets_daily():
    w = OIWindow()
    w.update("A", T0, 100, 1000); w.update("A", T0, 100, 1000)
    assert w.context("A")["window_signal"] is None      # <2 points
    w.update("A", T0 + timedelta(minutes=2), 101, 1010)
    assert w.context("A")["h15"] is None                # only 2 minutes of history
    w.update("A", T0 + timedelta(days=1), 100, 1000)
    assert w.context("A")["points"] == 1                # new session cleared old data
    assert w.depth()["symbols"] == 1


def test_persistence_streak_and_gap_reset():
    w = OIWindow()
    assert [w.note_agreement("A", True, T0 + timedelta(minutes=2 * i)) for i in range(4)] == [1, 2, 3, 4]
    assert w.note_agreement("A", False, T0 + timedelta(minutes=9)) == 0
    assert w.note_agreement("A", True, T0 + timedelta(minutes=11)) == 1
    assert w.note_agreement("A", True, T0 + timedelta(minutes=30)) == 1      # gap > 6 min: not consecutive


def test_candle_summary_opening_range_vwap_and_volume_pace():
    candles = [_candle("09:15", 100, 102, 99, 101, 1000), _candle("09:20", 101, 103, 100, 102, 1000),
               _candle("09:25", 102, 104, 101, 103, 1000), _candle("09:30", 103, 105, 102, 104, 1000)]
    s = summarize_candles(candles)
    assert s["or_high"] == 104 and s["or_low"] == 99 and s["or_complete"] is True and s["day_open"] == 100
    assert s["session_volume"] == 4000 and s["vwap"] and 100 < s["vwap"] < 105
    assert summarize_candles([_candle("09:15", 1, 2, 1, 2, 10)])["or_complete"] is False
    assert summarize_candles([]) == {"available": False}
    assert expected_volume_fraction(0) == 0 and expected_volume_fraction(375) == 1 and 0.19 < expected_volume_fraction(30) < 0.21
    assert relative_volume(1000, 10000, 5) is None      # too early to judge
    assert relative_volume(2400, 10000, 30) == 1.2
    assert average_daily_volume([{"volume": 100}] * 3) is None and average_daily_volume([{"volume": 100}] * 10) == 100


def test_bias_from_candles():
    up = [_candle(f"09:{15 + 5 * i:02d}", 100 + i, 101 + i, 99 + i, 100.5 + i, 1) for i in range(8)]
    down = [_candle(f"09:{15 + 5 * i:02d}", 100 - i, 101 - i, 99 - i, 99.5 - i, 1) for i in range(8)]
    assert bias_from_candles(up) == "BULL" and bias_from_candles(down) == "BEAR" and bias_from_candles(up[:2]) == "UNKNOWN"


def _good():
    return dict(
        signal={"signal": "LONG_BUILDUP", "ltp": 104.5},
        oi_ctx={"history_minutes": 30, "window_signal": "LONG_BUILDUP", "streak": 5, "h15": {"oi_pct": 1.0}},
        intraday={"available": True, "vwap": 102.0, "or_high": 104.0, "or_low": 99.0, "or_complete": True,
                  "day_open": 100.0, "last_minute": 9 * 60 + 55, "session_volume": 30000},
        atr14=5.0, avg_volume=50000, prev_close=99.5, ema20=101, ema50=99, banned=False, market_bias="BULL",
        now=T0, cfg=GateConfig())


def test_gate_passes_clean_setup_and_scores():
    r = evaluate_gate(**_good())
    assert r["actionable"] and r["confirmation_gate"] == "PASSED" and r["trade_recommendation"] == "PAPER_BUY"
    assert r["missing_confirmations"] == [] and 60 <= r["quality_score"] <= 100


def test_gate_blocks_shreecem_style_late_long():
    g = _good()
    g["oi_ctx"] = {"history_minutes": 30, "window_signal": "SHORT_BUILDUP", "streak": 0}   # falling now
    g["signal"] = {"signal": "LONG_BUILDUP", "ltp": 99.0}
    g["intraday"] = {**g["intraday"], "vwap": 102.0}
    r = evaluate_gate(**g)
    assert not r["actionable"]
    for reason in ("oi_window_disagrees", "persistence_short", "wrong_side_of_vwap", "inside_or_or_below"):
        assert reason in r["missing_confirmations"]


def test_gate_fails_closed_on_missing_inputs():
    g = _good(); g["intraday"] = None; g["oi_ctx"] = {"history_minutes": 2}
    r = evaluate_gate(**g)
    assert not r["actionable"] and "no_intraday_data" in r["missing_confirmations"] and "oi_history_warmup" in r["missing_confirmations"]
    g = _good(); g["atr14"] = None
    assert "atr_unavailable" in evaluate_gate(**g)["missing_confirmations"]


def test_gate_extension_ban_gap_and_counter_trend():
    g = _good(); g["signal"] = {"signal": "LONG_BUILDUP", "ltp": 106.0}
    assert "extended_from_open" in evaluate_gate(**g)["missing_confirmations"]
    g = _good(); g["banned"] = True
    assert "fo_ban_period" in evaluate_gate(**g)["missing_confirmations"]
    g = _good(); g["prev_close"] = 90.0                       # 10 pt gap > 1 ATR: needs 1.8x volume
    g["intraday"] = {**g["intraday"], "session_volume": 19500}   # rel volume 1.5: enough normally, not on a gap day
    res = evaluate_gate(**g)
    assert "gap_day_needs_volume" in res["missing_confirmations"] and "volume_too_low" not in res["missing_confirmations"]
    g = _good(); g["market_bias"] = "BEAR"
    assert evaluate_gate(**g)["quality_components"]["counter_trend_penalty"] == -10
    g = _good(); g["signal"] = {"signal": "SHORT_COVERING", "ltp": 104.5}
    assert "not_a_buildup_entry" in evaluate_gate(**g)["missing_confirmations"]


def test_ban_parser_and_refresh_keeps_last_good_list():
    text = "Securities in Ban For Trade Date 19-SEP-2026:\n1,ABFRL\n2,M&M\n3,BAJAJ-AUTO\nTotal,\n"
    assert parse_ban_csv(text) == frozenset({"ABFRL", "M&M", "BAJAJ-AUTO"})
    assert refresh_ban_list(fetch=lambda url: text) and "ABFRL" in banned_symbols()
    def boom(url):
        raise RuntimeError("blocked")
    assert refresh_ban_list(fetch=boom) is False
    assert "ABFRL" in banned_symbols() and ban_info()["ban_list_ok"] is False


# ---------------- wiring tests (scan -> window -> gate -> admission) ----------------
import app.oi_analyzer as oi
from database.repository import SignalRepository


def _feed_rows(price, oi_value):
    strong = {"symbol": "STRONG", "ltp": price, "latestOI": oi_value, "changeInOI": oi_value * 0.08,
              "oiChangePct": 8.0, "pChange": 3.0, "volume": 1000}
    quiet = {"symbol": "QUIET", "ltp": 100.0, "latestOI": 9_000_000, "changeInOI": 0, "oiChangePct": 0.0, "pChange": 0.1, "volume": 10}
    return [strong, quiet]


def test_scan_feeds_window_for_every_symbol_and_attaches_context(monkeypatch):
    oi.oi_window.reset()
    scans = iter(range(10))
    monkeypatch.setattr(oi, "fetch_all_fno_oi_change", lambda: _feed_rows(500.0, 9_000_000))
    times = [T0 + timedelta(minutes=2 * i) for i in range(6)]
    it = iter(times)
    monkeypatch.setattr(oi, "now_ist", lambda: next(it))
    results = []
    for _ in range(6):
        results = oi.scan_all_fno_realtime()
    assert oi.oi_window.depth()["symbols"] == 2                      # QUIET is tracked even though never published
    assert oi.oi_window.context("QUIET")["points"] == 6
    strong = [r for r in results if r["symbol"] == "STRONG"]
    if strong:                                                        # published after the 2-scan gate
        ctx = strong[0]["oi_window"]
        assert ctx["points"] >= 2 and "streak" in ctx
    assert oi._last_scan_stats["rows"] == 2 and oi._last_scan_stats["parsed"] >= 1


def test_apply_gate_in_main_and_admission(monkeypatch, tmp_path):
    import app.main as main
    monkeypatch.setattr(main, "banned_symbols", lambda: frozenset({"BAN"}))
    monkeypatch.setattr(main, "_market_bias_cached", lambda: "BULL")
    monkeypatch.setattr(main, "now_ist", lambda: datetime(2026, 9, 21, 10, 0, tzinfo=IST))
    good = {"symbol": "OKAY", "signal": "LONG_BUILDUP", "signal_direction": "BUY", "ltp": 104.5, "confidence": 90,
            "oi_window": {"history_minutes": 30, "window_signal": "LONG_BUILDUP", "streak": 5, "h15": {"oi_pct": 1.0}},
            "intraday_context": {"available": True, "vwap": 102.0, "or_high": 104.0, "or_low": 99.0, "or_complete": True,
                                 "day_open": 100.0, "last_minute": 9 * 60 + 55, "session_volume": 30000},
            "technical_context": {"atr14": 5.0, "ema20": 101, "ema50": 99}}
    bad = {**good, "symbol": "BAN"}
    bars = {s: [{"trade_date": "2026-09-18", "close": 99.5, "volume": 50000}] * 12 for s in ("OKAY", "BAN")}
    out = {r["symbol"]: r for r in main._apply_confirmation_gate([good, bad], bars)}
    assert out["OKAY"]["actionable"] is True and out["BAN"]["actionable"] is False
    assert "fo_ban_period" in out["BAN"]["missing_confirmations"]
    assert main._gate_stats["passed"] == 1 and main._gate_stats["top_missing"]["fo_ban_period"] == 1

    repo = SignalRepository(tmp_path / "adm.sqlite3")
    at = datetime(2026, 9, 21, 10, 0, tzinfo=IST)
    repo.record_scan([out["OKAY"], out["BAN"]], at)
    with repo._connect() as c:
        events = [r[0] for r in c.execute("SELECT symbol FROM signal_events ORDER BY id")]
        skips = [(r[0], r[1]) for r in c.execute("SELECT symbol, skip_reason FROM setup_skips")]
    assert events == ["OKAY", "BAN"] and skips == []           # gate-failed candidates are tracked as WATCH, not dropped
    with repo._connect() as c:
        tiers = dict(c.execute("SELECT symbol, tier FROM signal_events").fetchall())
    assert tiers == {"OKAY": "TRADE", "BAN": "WATCH"}
