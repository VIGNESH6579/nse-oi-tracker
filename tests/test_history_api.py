from datetime import datetime

from fastapi.testclient import TestClient

import app.main as main
from database.repository import SignalRepository
from utils.time import IST


def test_bhavcopy_backfill_required_tracks_expected_trading_date():
    before_publish = datetime(2026, 9, 17, 17, 0, tzinfo=IST)
    after_publish = datetime(2026, 9, 17, 18, 30, tzinfo=IST)
    stale = {"bars": 100, "latest_trade_date": "2026-09-15"}
    previous = {"bars": 100, "latest_trade_date": "2026-09-16"}
    current = {"bars": 100, "latest_trade_date": "2026-09-17"}

    assert main.bhavcopy_backfill_required(stale, before_publish) is True
    assert main.bhavcopy_backfill_required(previous, before_publish) is False
    assert main.bhavcopy_backfill_required(previous, after_publish) is True
    assert main.bhavcopy_backfill_required(current, after_publish) is False


def test_history_and_analytics_are_served_from_sqlite(monkeypatch, tmp_path):
    repository = SignalRepository(tmp_path / "tracker.sqlite3")
    captured_at = datetime.now(IST).replace(hour=10, minute=0, second=0, microsecond=0)
    repository.record_scan(
        [{
            "symbol": "API_TEST",
            "signal": "LONG_BUILDUP",
            "signal_direction": "BUY",
            "confidence": 80,
            "confidence_tier": "HIGH",
            "ltp": 100.0,
        }],
        captured_at,
    )
    monkeypatch.setattr(main, "repository", repository)

    with TestClient(main.app) as client:
        history = client.get("/api/history/today")
        analytics = client.get("/api/analytics/today")
        debug = client.get("/api/debug")

    assert history.status_code == 200
    assert history.json()["total_events"] == 1
    assert history.json()["events"][0]["symbol"] == "API_TEST"
    assert analytics.status_code == 200
    assert analytics.json()["metrics"]["signals_generated"] == 1
    assert analytics.json()["breakdowns"]["stock_accuracy"]["API_TEST"]["events"] == 1
    assert debug.status_code == 404




def test_market_overview_endpoint_returns_indices_only(monkeypatch):
    main.cache.delete("market-overview")
    monkeypatch.setattr(main, "fetch_market_indices", lambda: {
        "timestamp": "test", "advances": 2, "declines": 1, "unchanged": 0,
        "data": [{"index": "NIFTY 50", "last": 100, "variation": 1, "percentChange": 1}],
    })

    with TestClient(main.app) as client:
        response = client.get("/api/market-overview?refresh=true")

    assert response.status_code == 200
    assert response.json()["cached"] is False
    assert response.json()["indices"]["NIFTY"]["last"] == 100.0


def test_signal_api_exposes_sector_metadata_and_filters_cached_rows(monkeypatch):
    main.cache.set("all_signals", [{
        "symbol": "RELIANCE", "signal": "LONG_BUILDUP", "confidence_tier": "HIGH",
        "strength": 7, "sector": "Energy & Utilities",
    }], ttl=60)
    monkeypatch.setattr(main, "is_market_open", lambda: False)

    with TestClient(main.app) as client:
        response = client.get("/api/oi-signals?sector=energy%20%26%20utilities")

    assert response.status_code == 200
    assert response.json()["filtered_count"] == 1
    assert "Unclassified" in response.json()["available_sectors"]








def test_sources_endpoint_does_not_hide_unconfigured_feeds():
    with TestClient(main.app) as client:
        response = client.get("/api/sources")
    assert response.status_code == 200
    assert any(row["status"] == "NOT_CONFIGURED" for row in response.json()["sources"])




def test_backtest_endpoint_and_csv_export_are_auditable(monkeypatch, tmp_path):
    repository = SignalRepository(tmp_path / "tracker.sqlite3")
    captured_at = datetime.now(IST).replace(hour=10, minute=0, second=0, microsecond=0)
    repository.record_scan([{
        "symbol": "BACKTEST", "signal": "LONG_BUILDUP", "signal_direction": "BUY",
        "confidence": 80, "ltp": 100.0,
    }], captured_at)
    repository.update_open_events([{"symbol": "BACKTEST", "ltp": 101.0}], captured_at)
    monkeypatch.setattr(main, "repository", repository)
    trade_date = captured_at.date().isoformat()

    with TestClient(main.app) as client:
        response = client.get(f"/api/backtest?from_date={trade_date}&to_date={trade_date}")
        export = client.get(f"/api/backtest/export.csv?from_date={trade_date}&to_date={trade_date}")

    assert response.status_code == 200
    assert response.json()["metrics"]["events"] == 1
    assert "not a validated" in response.json()["metrics"]["methodology_caveat"]
    assert export.status_code == 200
    assert "BACKTEST" in export.text


def test_health_exposes_bhavcopy_backfill_status(monkeypatch, tmp_path):
    repository = SignalRepository(tmp_path / "health.sqlite3")
    monkeypatch.setattr(main, "repository", repository)
    with TestClient(main.app) as client:
        response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["daily_equity_data"]["bars"] == 0
    assert response.json()["bhavcopy_backfill_required"] is True
