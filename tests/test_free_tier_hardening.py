import base64
import gzip
import io
import sqlite3

from app import database_backup
from database.repository import SignalRepository


def test_daily_history_coverage_is_based_on_bar_depth(tmp_path):
    repository = SignalRepository(tmp_path / "coverage.sqlite3")
    rows = []
    for symbol, count in (("READY", 60), ("ATR_ONLY", 15), ("SHORT", 4)):
        rows.extend({"trade_date": f"2026-01-{day:02d}", "symbol": symbol, "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10} for day in range(1, count + 1))
    repository.upsert_daily_equity_bars(rows)
    coverage = repository.daily_history_coverage()
    assert coverage["symbols"] == 3
    assert coverage["atr_ready_symbols"] == 2
    assert coverage["history_ready_symbols"] == 1
    assert coverage["atr_coverage_pct"] == 66.67
    assert coverage["history_ready_pct"] == 33.33


def test_github_snapshot_upload_never_logs_token(monkeypatch, tmp_path, caplog):
    db = tmp_path / "db.sqlite3"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE sample (value TEXT)")
        connection.execute("INSERT INTO sample VALUES ('ok')")
    secret = "canary-github-token"
    monkeypatch.setenv("NSE_OI_BACKUP_GITHUB_REPO", "owner/private")
    monkeypatch.setenv("NSE_OI_BACKUP_GITHUB_TOKEN", secret)
    calls = []

    def request(url, **kwargs):
        calls.append((url, kwargs))
        if kwargs.get("method", "GET") == "GET":
            raise __import__("urllib.error", fromlist=["HTTPError"]).HTTPError(url, 404, "missing", {}, None)
        return b"{}"

    monkeypatch.setattr(database_backup, "_request", request)
    assert database_backup.upload_github_snapshot(db) is True
    assert secret not in caplog.text
    assert any(call[1].get("token") == secret for call in calls)


def test_github_snapshot_conflict_retries(monkeypatch, tmp_path):
    db = tmp_path / "db.sqlite3"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE sample (value TEXT)")
    monkeypatch.setenv("NSE_OI_BACKUP_GITHUB_REPO", "owner/private")
    monkeypatch.setenv("NSE_OI_BACKUP_GITHUB_TOKEN", "token")
    state = {"puts": 0}

    def request(url, **kwargs):
        if kwargs.get("method", "GET") == "GET":
            return b'{"sha":"old-sha"}'
        state["puts"] += 1
        if state["puts"] == 1:
            raise __import__("urllib.error", fromlist=["HTTPError"]).HTTPError(url, 409, "conflict", {}, None)
        return b"{}"

    monkeypatch.setattr(database_backup, "_request", request)
    assert database_backup.upload_github_snapshot(db) is True
    assert state["puts"] == 2
