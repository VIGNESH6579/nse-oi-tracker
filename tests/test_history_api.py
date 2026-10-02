    assert response.json()["bhavcopy_backfill_required"] is True


def test_health_status_reflects_blocked_readiness(monkeypatch, tmp_path):
    repository = SignalRepository(tmp_path / "health-status.sqlite3")
    monkeypatch.setattr(main, "repository", repository)
    monkeypatch.setattr(main.self_test, "readiness", lambda: "BLOCKED")
    monkeypatch.setattr(main, "get_market_status", lambda _now=None: main.MARKET_STATUS_CLOSED_BEFORE_OPEN)
    with TestClient(main.app) as client:
        response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["status"] == "blocked"
    assert response.json()["readiness"] == "BLOCKED"