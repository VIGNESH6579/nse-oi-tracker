"""Regression tests for the day-relative price-source chain.

No live network: ``_nse.get`` is stubbed and bhavcopy history lives in a
temporary SQLite database pointed at via ``NSE_OI_DATA_DIR`` /
``NSE_OI_DATABASE`` exactly the way production resolves them.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest
from utils.time import now_ist

import app.nse_fetcher as nse_fetcher
from utils.time import IST

SPURTS_URL_SUFFIX = "live-analysis-oi-spurts-underlyings"
ALL_INDICES_URL_SUFFIX = "api/allIndices"


def _seed_bars(db_path, rows: list[tuple[str, str, float]]) -> None:
    """Create a minimal daily_equity_bars table and insert (date, symbol, close)."""
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_equity_bars (
                trade_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL
            )
            """
        )
        connection.executemany(
            "INSERT INTO daily_equity_bars VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(trade_date, symbol, close, close, close, close, 1000.0) for trade_date, symbol, close in rows],
        )
        connection.commit()
    finally:
        connection.close()


@pytest.fixture()
def price_env(tmp_path, monkeypatch):
    """Isolated fetcher state: temp DB path, empty snapshots, fresh index cache."""
    database = tmp_path / "nse_oi_tracker.sqlite3"
    monkeypatch.setenv("NSE_OI_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("NSE_OI_DATABASE", database.name)
    monkeypatch.setattr(nse_fetcher, "_price_snapshots", nse_fetcher.OrderedDict())
    monkeypatch.setattr(nse_fetcher, "_INDEX_PREVIOUS_CLOSES", {})
    monkeypatch.setattr(nse_fetcher, "_INDEX_PREVIOUS_CLOSES_AT", 0.0)
    return database


def _stub_get(monkeypatch, spurts_payload: dict, all_indices_payload: dict | None = None):
    calls: list[str] = []

    def fake_get(url: str, referer: str | None = None) -> dict:
        calls.append(url)
        if ALL_INDICES_URL_SUFFIX in url:
            return all_indices_payload or {}
        return spurts_payload

    monkeypatch.setattr(nse_fetcher._nse, "get", fake_get)
    return calls


# ---------------------------------------------------------------------------
# _stored_previous_close
# ---------------------------------------------------------------------------


def test_stored_previous_close_returns_recent_bar(price_env):
    _seed_bars(price_env, [(now_ist().date().isoformat(), "RELIANCE", 2400.5)])
    assert nse_fetcher._stored_previous_close("reliance") == 2400.5


def test_stored_previous_close_uses_ist_date_before_utc_rollover(price_env, monkeypatch):
    _seed_bars(price_env, [("2026-09-17", "RELIANCE", 2400.5)])
    monkeypatch.setattr(
        nse_fetcher,
        "now_ist",
        lambda: nse_fetcher.datetime(2026, 9, 17, 0, 15, tzinfo=IST),
    )
    assert nse_fetcher._stored_previous_close("RELIANCE") == 2400.5


def test_stored_previous_close_accepts_a_few_days_old(price_env):
    _seed_bars(price_env, [((now_ist().date() - timedelta(days=3)).isoformat(), "TCS", 3800.0)])
    assert nse_fetcher._stored_previous_close("TCS") == 3800.0


def test_stored_previous_close_rejects_stale_bar(price_env):
    _seed_bars(price_env, [((now_ist().date() - timedelta(days=11)).isoformat(), "TCS", 3800.0)])
    assert nse_fetcher._stored_previous_close("TCS") is None


def test_stored_previous_close_rejects_future_bar(price_env):
    _seed_bars(price_env, [((now_ist().date() + timedelta(days=1)).isoformat(), "TCS", 3800.0)])
    assert nse_fetcher._stored_previous_close("TCS") is None


def test_stored_previous_close_rejects_non_positive_close(price_env):
    _seed_bars(price_env, [(now_ist().date().isoformat(), "TCS", 0.0)])
    assert nse_fetcher._stored_previous_close("TCS") is None


def test_stored_previous_close_missing_symbol(price_env):
    _seed_bars(price_env, [(now_ist().date().isoformat(), "INFY", 1500.0)])
    assert nse_fetcher._stored_previous_close("MISSINGSYM") is None


def test_stored_previous_close_missing_database(price_env):
    assert nse_fetcher._stored_previous_close("TCS") is None


def test_stored_previous_close_corrupt_database(price_env):
    price_env.write_bytes(b"this is not a sqlite database")
    assert nse_fetcher._stored_previous_close("TCS") is None


def test_previous_close_database_resolves_from_project_root(tmp_path, monkeypatch):
    """Relative NSE_OI_DATA_DIR resolves against the project root, not cwd."""
    project_root = Path(nse_fetcher.__file__).resolve().parents[1]
    data_dir = project_root / "data"
    data_dir.mkdir(exist_ok=True)
    database = data_dir / "pc_resolution_test.sqlite3"
    try:
        _seed_bars(database, [(now_ist().date().isoformat(), "WIPRO", 450.0)])
        monkeypatch.setenv("NSE_OI_DATA_DIR", "data")
        monkeypatch.setenv("NSE_OI_DATABASE", "pc_resolution_test.sqlite3")
        elsewhere = tmp_path / "unrelated-cwd"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)
        assert nse_fetcher._stored_previous_close("WIPRO") == 450.0
    finally:
        database.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# fetch_all_fno_oi_change priority chain
# ---------------------------------------------------------------------------


def test_native_price_change_is_never_overwritten(price_env, monkeypatch):
    _stub_get(monkeypatch, {"data": [{"symbol": "RELIANCE", "underlyingValue": 101.0, "pChange": 1.5}]})
    rows = nse_fetcher.fetch_all_fno_oi_change()
    assert rows[0]["price_source"] == "nse_native"
    assert rows[0]["pChange"] == 1.5


def test_stock_row_uses_day_relative_previous_close(price_env, monkeypatch):
    _seed_bars(price_env, [(now_ist().date().isoformat(), "RELIANCE", 100.0)])
    _stub_get(monkeypatch, {"data": [{"symbol": "RELIANCE", "underlyingValue": 101.0}]})
    rows = nse_fetcher.fetch_all_fno_oi_change()
    assert rows[0]["price_source"] == "previous_close_day_relative"
    assert rows[0]["pChange"] == pytest.approx(1.0)
    assert rows[0]["change"] == pytest.approx(1.0)


def test_index_row_uses_all_indices_previous_close(price_env, monkeypatch):
    calls = _stub_get(
        monkeypatch,
        {"data": [{"symbol": "NIFTY", "underlyingValue": 20200.0}]},
        {"data": [{"index": "NIFTY 50", "previousClose": 20000.0}]},
    )
    rows = nse_fetcher.fetch_all_fno_oi_change()
    assert rows[0]["price_source"] == "all_indices_previous_close"
    assert rows[0]["pChange"] == pytest.approx(1.0)
    assert any(ALL_INDICES_URL_SUFFIX in url for url in calls)


def test_second_poll_without_basis_falls_back_to_rolling_delta(price_env, monkeypatch):
    _stub_get(monkeypatch, {"data": [{"symbol": "SOLARINDS", "underlyingValue": 100.0}]})

    first = nse_fetcher.fetch_all_fno_oi_change()
    assert "price_source" not in first[0]  # no basis exists on the first poll

    _stub_get(monkeypatch, {"data": [{"symbol": "SOLARINDS", "underlyingValue": 101.0}]})
    second = nse_fetcher.fetch_all_fno_oi_change()
    assert second[0]["price_source"] == "rolling_minute_fallback"
    assert second[0]["pChange"] == pytest.approx(1.0)


def test_stale_stored_close_degrades_to_rolling_fallback(price_env, monkeypatch):
    _seed_bars(price_env, [((now_ist().date() - timedelta(days=11)).isoformat(), "SOLARINDS", 100.0)])
    _stub_get(monkeypatch, {"data": [{"symbol": "SOLARINDS", "underlyingValue": 100.0}]})
    nse_fetcher.fetch_all_fno_oi_change()

    _stub_get(monkeypatch, {"data": [{"symbol": "SOLARINDS", "underlyingValue": 102.0}]})
    second = nse_fetcher.fetch_all_fno_oi_change()
    assert second[0]["price_source"] == "rolling_minute_fallback"
    assert second[0]["pChange"] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# index previous-close cache
# ---------------------------------------------------------------------------


def test_index_previous_closes_cached_within_ttl(price_env, monkeypatch):
    calls = _stub_get(
        monkeypatch,
        {},
        {"data": [{"index": "NIFTY BANK", "previousClose": 51000.0}]},
    )

    first = nse_fetcher._index_previous_closes()
    second = nse_fetcher._index_previous_closes()

    assert first == {"BANKNIFTY": 51000.0}
    assert second == first
    all_indices_calls = [url for url in calls if ALL_INDICES_URL_SUFFIX in url]
    assert len(all_indices_calls) == 1

    # Simulate TTL expiry: the next call must hit the API again.
    monkeypatch.setattr(nse_fetcher, "_INDEX_PREVIOUS_CLOSES_AT", 0.0)
    nse_fetcher._index_previous_closes()
    assert len([url for url in calls if ALL_INDICES_URL_SUFFIX in url]) == 2
