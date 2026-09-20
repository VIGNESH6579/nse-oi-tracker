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


def test_pre_open_blocked_when_angel_fails_and_shows_missing_symbols(tmp_path):
    universe = [f"S{i}" for i in range(200)]
    repo = _repo(tmp_path, universe)
    result = st.run_stage("pre_open", _probes("pre_open", FakeAngel(quotes_fail=True, state="breaker_open"), repo, universe), NOW)
    assert result["verdict"] == "BLOCKED"
    assert "angel_equity_quotes" in result["failed"] and "HTTP 403" in result["checks"]["angel_equity_quotes"]["detail"]
    assert result["checks"]["bars_coverage"]["ok"] is True            # 2 missing of 200 = 99% >= 90%
    assert "S198" in result["checks"]["bars_coverage"]["detail"]


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
    assert wrong["verdict"] == "BLOCKED" and "angel_intraday_candles" in wrong["failed"]
    warm = st.run_stage("post_open", _probes("post_open", FakeAngel(), repo, ["A"], depth={"symbols": 5, "median_minutes": 2}), NOW)
    assert warm["verdict"] == "DEGRADED" and warm["failed"] == ["oi_window_depth"]


def test_probe_exception_never_crashes_and_unknown_stage_rejected(tmp_path):
    import pytest
    result = st.run_stage("pre_open", {"boom": lambda: 1 / 0}, NOW)
    assert result["failed"] == ["boom"] and "ZeroDivisionError" in result["checks"]["boom"]["detail"]
    with pytest.raises(ValueError):
        st.build_probes("nope", angel=None, repository=None, universe=set, ban_info=dict, scan_stats=dict, window_depth=dict)


def test_health_exposes_readiness_and_gap_fields():
    from fastapi.testclient import TestClient
    import app.main as main
    with TestClient(main.app) as client:
        body = client.get("/api/health").json()
    assert "readiness" in body and "self_test" in body["data_quality"] and "universe_missing_bars" in body["data_quality"]
