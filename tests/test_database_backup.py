import gzip
import io
import sqlite3

from app import database_backup

def _db(path, bars=0, events=0):
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE daily_equity_bars (trade_date TEXT, symbol TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)")
        c.execute("CREATE TABLE signal_events (id INTEGER, symbol TEXT)")
        if bars: c.execute("INSERT INTO daily_equity_bars VALUES ('2026-09-15','NIFTY',1,2,0.5,1.5,0)")
        if events: c.execute("INSERT INTO signal_events VALUES (1,'NIFTY')")

def test_restore_skips_without_backup_config(monkeypatch, tmp_path):
    monkeypatch.delenv("NSE_OI_BACKUP_URL", raising=False)
    assert database_backup.restore_latest_backup(tmp_path / "db.sqlite3") is False

def test_restore_populates_bars_and_signal_events(monkeypatch, tmp_path):
    source=tmp_path / "source.sqlite3"; target=tmp_path / "target.sqlite3"
    _db(source, bars=1, events=1); raw=source.read_bytes(); payload=io.BytesIO()
    with gzip.GzipFile(fileobj=payload, mode="wb") as out: out.write(raw)
    monkeypatch.setattr(database_backup, "_config", lambda: ("https://bucket", "token"))
    monkeypatch.setattr(database_backup, "_request", lambda url, **kwargs: b'["snapshot.sqlite3.gz"]' if url.endswith("manifest.json") else payload.getvalue())
    assert database_backup.restore_latest_backup(target) is True
    with sqlite3.connect(target) as c:
        assert c.execute("SELECT COUNT(*) FROM daily_equity_bars").fetchone()[0] == 1
        assert c.execute("SELECT COUNT(*) FROM signal_events").fetchone()[0] == 1

def test_snapshot_upload_failure_is_non_blocking(monkeypatch, tmp_path):
    db=tmp_path / "db.sqlite3"; _db(db, bars=1)
    monkeypatch.setattr(database_backup, "_config", lambda: ("https://bucket", "token"))
    def fail(*args, **kwargs): raise OSError("bucket down")
    monkeypatch.setattr(database_backup, "_request", fail)
    assert database_backup.upload_database_snapshot(db) is False


def test_restore_temp_file_is_created_on_target_filesystem(monkeypatch, tmp_path):
    source = tmp_path / "source.sqlite3"
    target_dir = tmp_path / "target-dir"
    target = target_dir / "restored.sqlite3"
    _db(source, bars=1, events=1)
    payload = io.BytesIO()
    with gzip.GzipFile(fileobj=payload, mode="wb") as out:
        out.write(source.read_bytes())

    original = database_backup.tempfile.NamedTemporaryFile
    seen = {}

    def tracked_tempfile(*args, **kwargs):
        seen["dir"] = kwargs.get("dir")
        return original(*args, **kwargs)

    monkeypatch.setattr(database_backup.tempfile, "NamedTemporaryFile", tracked_tempfile)
    assert database_backup._restore_payload(target, payload.getvalue()) is True
    assert seen["dir"] == target_dir
    with sqlite3.connect(target) as connection:
        assert connection.execute("SELECT COUNT(*) FROM daily_equity_bars").fetchone()[0] == 1
