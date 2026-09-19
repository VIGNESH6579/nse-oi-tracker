import gzip
import json
import sqlite3
from datetime import datetime

import pytest

from collector import universe
from database.repository import SignalRepository
from utils.time import IST


def _master(n=150):
    rows = [{"exch_seg": "NFO", "instrumenttype": "FUTSTK", "name": f"STK{i}"} for i in range(n)]
    rows += [{"exch_seg": "NFO", "instrumenttype": "FUTIDX", "name": "NIFTY"},
             {"exch_seg": "NFO", "instrumenttype": "OPTSTK", "name": "OPTONLY"},
             {"exch_seg": "NSE", "instrumenttype": "", "name": "EQONLY"}]
    return rows


def test_parse_angel_master_only_stock_futures():
    got = universe.parse_angel_master(_master())
    assert len(got) == 150 and "NIFTY" not in got and "OPTONLY" not in got and "EQONLY" not in got


def test_parse_angel_master_rejects_implausible_size():
    assert universe.parse_angel_master(_master(10)) == frozenset()          # too small
    assert universe.parse_angel_master(_master(900)) == frozenset()         # too large


def test_parse_fo_mktlots():
    text = "UNDERLYING ,SYMBOL ,Oct-26\n" + "\n".join(f"Stock {i},SYM{i} ,100" for i in range(150))
    text += "\nNifty 50,NIFTY,75"
    got = universe.parse_fo_mktlots(text)
    assert len(got) == 150 and "SYM3" in got and "NIFTY" not in got


def test_load_universe_fallback_order_and_fail_closed():
    calls = []

    def bad():
        calls.append("bad")
        raise RuntimeError("blocked")

    good = frozenset(f"S{i}" for i in range(150))
    got = universe.load_fno_universe([("a", bad), ("b", lambda: good)], force=True)
    assert got == good and calls == ["bad"]
    assert universe.load_fno_universe([("a", bad), ("b", lambda: frozenset())], force=True) == frozenset()


def _bars(symbols, date="2026-09-10"):
    return [{"trade_date": date, "symbol": s, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10} for s in symbols]


def test_purge_non_fno_bars_and_fail_closed(tmp_path):
    repo = SignalRepository(tmp_path / "p.sqlite3")
    repo.upsert_daily_equity_bars(_bars(["AAA", "BBB", "JUNK1", "JUNK2", "JUNK3"]))
    assert repo.purge_non_fno_bars([])["deleted"] == 0            # empty universe never wipes
    result = repo.purge_non_fno_bars(["AAA", "BBB"])
    assert result["deleted"] == 3 and result["after"] == 2 and result["vacuumed"] == 1
    assert repo.purge_non_fno_bars(["AAA", "BBB"])["deleted"] == 0  # idempotent


def test_setup_skips_one_compact_row_per_day_symbol_reason(tmp_path):
    repo = SignalRepository(tmp_path / "s.sqlite3")
    sig = {"symbol": "LATE", "signal": "LONG_BUILDUP", "signal_direction": "BUY", "confidence": 82,
           "ltp": 100.0, "price_change_pct": 1.0, "oi_change_pct": 5.0, "big_blob": "x" * 5000}
    for minute in range(0, 40, 2):  # 20 scans after the 14:30 cutoff
        repo.record_scan([{**sig, "ltp": 100.0 + minute}], datetime(2026, 9, 10, 14, 30 + minute // 60, minute % 60, tzinfo=IST))
    rows = repo.skip_reasons_for_date("2026-09-10")
    assert len(rows) == 1 and rows[0]["skip_reason"] == "window"
    assert "big_blob" not in rows[0]["payload"]


def test_snapshot_prune_and_retry(tmp_path, monkeypatch):
    import app.database_backup as backup
    db = tmp_path / "snap.sqlite3"
    repo = SignalRepository(db)
    repo.upsert_daily_equity_bars(_bars([f"S{i}" for i in range(50)]))
    with sqlite3.connect(db) as c:
        c.executemany(
            "INSERT INTO scan_snapshots(trade_date,captured_at_ist,captured_minute_ist,source,is_stale,fingerprint,signal_count,archived,created_at_utc) VALUES(?,?,?,?,?,?,?,?,?)",
            [("2026-09-01", "2026-09-01T10:00:00", "2026-09-01T10:00", "t", 0, f"old-{i}-" + "x" * 200, 1, 0, "x") for i in range(600)]
            + [("2026-09-10", "2026-09-10T10:00:00", "2026-09-10T10:00", "t", 0, f"new-{i}", 1, 0, "x") for i in range(5)])
    full = len(backup._compress_database(db))                     # cap is large: no pruning
    pruned_copy = backup._prune_copy(db)
    pruned = len(backup._gzip_file(pruned_copy))
    pruned_copy.unlink()
    assert pruned < full
    monkeypatch.setattr(backup, "_MAX_PAYLOAD", (full + pruned) // 2)   # force the prune path
    payload = backup._compress_database(db)
    assert len(payload) <= backup._MAX_PAYLOAD
    restored = tmp_path / "r.sqlite3"
    restored.write_bytes(gzip.decompress(payload))
    with sqlite3.connect(restored) as c:
        assert {r[0] for r in c.execute("SELECT DISTINCT trade_date FROM scan_snapshots")} == {"2026-09-10"}
    with sqlite3.connect(db) as c:                                 # live DB untouched
        assert c.execute("SELECT COUNT(DISTINCT trade_date) FROM scan_snapshots").fetchone()[0] == 2
    assert backup.last_snapshot_info()["snapshot_bytes"] == len(payload)


def test_snapshot_hard_cap_raises(tmp_path, monkeypatch):
    import app.database_backup as backup
    SignalRepository(tmp_path / "cap.sqlite3")
    monkeypatch.setattr(backup, "_MAX_PAYLOAD", 10)
    with pytest.raises(ValueError):
        backup._compress_database(tmp_path / "cap.sqlite3")


def test_memory_watchdog_and_health_filter():
    import logging
    from app.main import memory_watchdog, _HealthAccessFilter
    assert memory_watchdog() is None or memory_watchdog() > 0
    f = _HealthAccessFilter()
    rec = logging.LogRecord("uvicorn.access", 20, "", 0, '%s - "GET /api/health HTTP/1.1" 200', ("1.2.3.4",), None)
    assert f.filter(rec) is False
    rec2 = logging.LogRecord("uvicorn.access", 20, "", 0, '%s - "GET /api/signals HTTP/1.1" 200', ("1.2.3.4",), None)
    assert f.filter(rec2) is True
