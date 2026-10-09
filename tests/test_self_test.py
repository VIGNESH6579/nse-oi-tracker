from datetime import datetime

import app.self_test as st
from database.repository import SignalRepository
from utils.time import IST

NOW = datetime(2026, 9, 21, 9, 5, tzinfo=IST)


class FakeAngel:
    def __init__(self, **over):
        self.over = over

    def health(self):
        return {"state": self.over.get("state", "authenticated")}

    def full_quotes(self, symbols):
        if self.over.get("quotes_fail"):
            raise RuntimeError("HTTP 403")
        return {s: {"ltp": 100.0} for s in symbols}

    def fno_quotes(self, symbols):
        return {s: {"ltp": 100.0, "oi": 5000 if not self.over.get("no_oi") else 0, "oi_includes_next_month": False} for s in symbols}

    def daily_candles(self, symbol, days=5):
        return [{"time": f"2026-09-1{i}T00:00:00+0530"} for i in range(5)]

    def intraday_candles(self, symbol):
        first = self.over.get("first_minute", "09:15")
        return [{"time": f"2026-09-21T{first}:00+0530"}, {"time": "2026-09-21T09:20:00+0530"}, {"time": "2026-09-21T09:25:00+0530"}]


def _repo(tmp_path, universe, with_bars=15):
    repo = SignalRepository(tmp_path / "s.sqlite3")
    bars = [{"trade_date": f"2026-08-{d:02d}", "symbol": sym, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10}
            for sym in universe[:-2] for d in range(1, with_bars + 1)]
    repo.upsert_daily_equity_bars(bars)
    with repo._connect() as c:
        c.executemany("INSERT INTO daily_index_bars(trade_date, symbol, open, high, low, close) VALUES (?,?,?,?,?,?)",
                      [(f"2026-08-{d:02d}", "NIFTY", 1, 2, 1, 2) for d in range(1, 26)])
    return repo


def _probes(stage, angel, repo, universe, scan=None, depth=None, ban_ok=True):
    return st.build_probes(stage, angel=angel, repository=repo, universe=lambda: set(universe),
                           ban_info=lambda: {"ban_list_ok": ban_ok, "ban_list_size": 4, "ban_list_age_s": 10},
                           scan_stats=lambda: scan or {"rows": 200, "oi_source": "nse_oi_spurts"},
                           window_depth=lambda: depth or {"symbols": 200, "median_minutes": 20})


def test_repository_gap_helpers(tmp_path):
    universe = [f"S{i}" for i in range(20)]
    repo = _repo(tmp_path, universe)
    assert repo.symbols_missing_bars(universe, 15) == ["S18", "S19"]
    assert repo.symbols_missing_bars(universe, 16) == sorted(universe)
    assert repo.index_bar_count("nifty") == 25


def test_pre_open_ready(tmp_path):
    universe = [f"S{i}" for i in range(200)]
    repo = _repo(tmp_path, universe)
    result = st.run_stage("pre_open", _probes("pre_open", FakeAngel(), repo, universe), NOW)
    assert result["verdict"] == "READY" and result["failed"] == [] and st.readiness() == "READY"


def test_pre_open_does_not_check_live_quotes_and_shows_missing_symbols(tmp_path):
    """Before 09:15 the day's open/high/low do not exist yet, so a live quote/futures-OI
    check would (correctly) fail every single morning and report a false BLOCKED for
    hours. Those checks now only run in post_open, once trading has actually started."""
    universe = [f"S{i}" for i in range(200)]
    repo = _repo(tmp_path, universe)
    result = st.run_stage("pre_open", _probes("pre_open", FakeAngel(quotes_fail=True), repo, universe), NOW)
    assert "angel_equity_quotes" not in result["checks"] and "angel_futures_oi" not in result["checks"]
    assert result["verdict"] == "READY"
    assert result["checks"]["bars_coverage"]["ok"] is True            # 2 missing of 200 = 99% >= 90%
    assert "S198" in result["checks"]["bars_coverage"]["detail"]


def test_post_open_blocked_when_angel_quotes_fail(tmp_path):
    repo = _repo(tmp_path, ["A", "B", "C"])
    result = st.run_stage("post_open", _probes("post_open", FakeAngel(quotes_fail=True, state="breaker_open"), repo, ["A"]), NOW)
    assert result["verdict"] == "BLOCKED"
    assert "angel_equity_quotes" in result["failed"] and "HTTP 403" in result["checks"]["angel_equity_quotes"]["detail"]


def test_pre_open_degraded_when_only_noncritical_fails(tmp_path):
    universe = [f"S{i}" for i in range(200)]
    repo = _repo(tmp_path, universe)
    result = st.run_stage("pre_open", _probes("pre_open", FakeAngel(), repo, universe, ban_ok=False), NOW)
    assert result["verdict"] == "DEGRADED" and result["failed"] == ["ban_list"]


def test_bars_coverage_fails_below_90_percent(tmp_path):
    universe = [f"S{i}" for i in range(20)]
    repo = _repo(tmp_path, universe, with_bars=5)                     # only 5 bars each: all missing
    result = st.run_stage("pre_open", _probes("pre_open", FakeAngel(), repo, universe), NOW)
    assert result["verdict"] == "BLOCKED" and "bars_coverage" in result["failed"]


def test_post_open_checks_rows_window_and_ist_candles(tmp_path):
    repo = _repo(tmp_path, ["A", "B", "C"])
    ok = st.run_stage("post_open", _probes("post_open", FakeAngel(), repo, ["A"]), NOW)
    assert ok["verdict"] == "READY"
    few = st.run_stage("post_open", _probes("post_open", FakeAngel(), repo, ["A"], scan={"rows": 20, "oi_source": "angel_futures"}), NOW)
    assert few["verdict"] == "BLOCKED" and "oi_rows" in few["failed"]
    # candles that start at 04:30 (UTC-window bug) instead of 09:15 must be caught
    wrong = st.run_stage("post_open", _probes("post_open", FakeAngel(first_minute="04:30"), repo, ["A"]), NOW)
    assert wrong["verdict"] == "DEGRADED" and "angel_intraday_candles" in wrong["failed"]
    warm = st.run_stage("post_open", _probes("post_open", FakeAngel(), repo, ["A"], depth={"symbols": 5, "median_minutes": 2}), NOW)
    assert warm["verdict"] == "DEGRADED" and warm["failed"] == ["oi_window_depth"]


def test_probe_exception_never_crashes_and_unknown_stage_rejected(tmp_path):
    import pytest
    result = st.run_stage("pre_open", {"boom": lambda: 1 / 0}, NOW)
    assert result["failed"] == ["boom"] and "ZeroDivisionError" in result["checks"]["boom"]["detail"]
    with pytest.raises(ValueError):
        st.build_probes("nope", angel=None, repository=None, universe=set, ban_info=dict, scan_stats=dict, window_depth=dict)


def test_holiday_health_is_service_healthy_even_without_trading_self_test():
    from fastapi.testclient import TestClient
    import app.main as main
    from unittest import mock
    with TestClient(main.app) as client:
        with mock.patch.object(main, "get_market_status", return_value=main.MARKET_STATUS_CLOSED_HOLIDAY), \
             mock.patch.object(main.repository, "daily_equity_bar_summary", return_value={"bars": 60, "symbols": 213, "latest_trade_date": "2026-10-01", "min_bars": 60, "max_bars": 60}), \
             mock.patch.object(main.self_test, "readiness", return_value="UNTESTED"):
            body = client.get("/api/health").json()
    assert body["status"] == "ok"


def test_health_exposes_readiness_and_gap_fields():
    from fastapi.testclient import TestClient
    import app.main as main
    with TestClient(main.app) as client:
        body = client.get("/api/health").json()
    assert "readiness" in body and "self_test" in body["data_quality"] and "universe_missing_bars" in body["data_quality"]
    assert body["startup_state"] in {"READY", "DEGRADED", "BLOCKED"}
    # In the dependency-free test environment the self-test must not falsely claim READY.
    if body["data_quality"]["self_test"].get("pre_open", {}).get("verdict") == "BLOCKED":
        assert body["startup_state"] == "BLOCKED"
    assert body["scheduler_started_at_ist"] and body["scheduler_heartbeat_at_ist"]
    if body["startup_state"] == "READY":
        assert body["startup_ready_at_ist"]


def test_readiness_is_not_ready_when_no_scan_has_ever_completed():
    """A cold start outside the self-test window (e.g. a free-tier spin-down
    wake-up mid-day, or any boot with the market closed) must not report
    "READY" on the strength of the process having booted alone.
    scheduled_refresh() is a no-op when the market is closed, so
    _startup_state flipping to "READY" is not evidence a scan ever ran.
    """
    import app.main as main
    from fastapi.testclient import TestClient

    with TestClient(main.app) as client:
        # Simulate: process finished booting (this really did happen), but
        # nothing has ever produced a scan, and self_test has not run yet.
        # main.repository is a process-wide singleton shared across the test
        # session, so an earlier test's persisted snapshot must be excluded
        # explicitly rather than relying on it being absent by chance.
        main._startup_state = "READY"
        main._last_refresh_at_ist = None
        main._last_snapshot_id = None
        main.self_test._latest.clear()
        import unittest.mock as mock
        with mock.patch.object(main.repository, "latest_snapshot_metadata", return_value=None):
            body = client.get("/api/health").json()

    assert body["startup_state"] == "READY"          # process is genuinely up
    assert body["last_scan_at"] is None               # but nothing has scanned
    assert body["readiness"] == "UNTESTED"             # so readiness must not lie


def test_readiness_reports_ready_once_a_scan_has_actually_completed():
    import app.main as main
    from fastapi.testclient import TestClient

    with TestClient(main.app) as client:
        main._startup_state = "READY"
        main._last_refresh_at_ist = "2026-09-24T09:20:00+05:30"
        main.self_test._latest.clear()
        body = client.get("/api/health").json()

    assert body["last_scan_at"] == "2026-09-24T09:20:00+05:30"
    assert body["readiness"] == "READY"


def test_scalp_failure_is_not_core_blocker(tmp_path):
    repo = _repo(tmp_path, ["A", "B", "C"])
    probes = _probes("post_open", FakeAngel(), repo, ["A"])
    probes["scalp_chain"] = lambda: (False, "latest_tick unavailable")
    result = st.run_stage("post_open", probes, NOW)
    assert result["verdict"] == "DEGRADED"
    assert "scalp_chain" in result["failed"]


def test_readiness_recheck_can_recover_after_hours_without_restart(monkeypatch):
    import asyncio
    import app.main as main

    called = []

    async def fake_self_test(stage):
        called.append(stage)

    monkeypatch.setattr(main, "is_market_open", lambda: False)
    monkeypatch.setattr(main, "is_trading_holiday", lambda day: False)
    monkeypatch.setattr(main, "now_ist", lambda: NOW.replace(hour=21, minute=35))
    monkeypatch.setattr(main, "scheduled_self_test", fake_self_test)
    monkeypatch.setattr(main.self_test, "readiness", lambda: "READY")
    monkeypatch.setattr(main, "_startup_state", "BLOCKED")
    monkeypatch.setattr(main, "_startup_error", "startup self-test verdict=BLOCKED")
    monkeypatch.setattr(main, "_startup_ready_at_ist", None)

    asyncio.run(main.scheduled_readiness_recheck())

    assert called == ["pre_open"]
    assert main._startup_state == "READY"
    assert main._startup_error is None
    assert main._startup_ready_at_ist is not None
