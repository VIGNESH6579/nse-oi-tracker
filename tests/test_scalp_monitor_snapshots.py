from database.repository import SignalRepository


def test_monitor_snapshots_are_timestamped_and_returned_chronologically(tmp_path):
    repository = SignalRepository(tmp_path / "monitor.sqlite3")
    repository.record_scalp_monitor_snapshot(
        42,
        trade_date="2026-10-19",
        symbol="JSWSTEEL",
        observed_at_ist="2026-10-19T12:29:00+05:30",
        status="MONITORING",
        snapshot={"underlying": {"ltp": 1245.8, "momentum_pct": 0.12}, "option": {"ltp": 17.1}},
    )
    repository.record_scalp_monitor_snapshot(
        42,
        trade_date="2026-10-19",
        symbol="JSWSTEEL",
        observed_at_ist="2026-10-19T12:29:30+05:30",
        status="OPTION_QUOTE_UNAVAILABLE",
        snapshot={"underlying": {"ltp": 1244.9}, "option_quote_available": False},
    )
    repository.record_scalp_monitor_snapshot(
        43,
        trade_date="2026-10-19",
        symbol="ABB",
        observed_at_ist="2026-10-19T13:15:00+05:30",
        status="MONITORING",
        snapshot={"option": {"ltp": 216.0}},
    )

    rows = repository.scalp_monitor_snapshots_for_event(42, limit=10)

    assert len(rows) == 2
    assert [row["observed_at_ist"] for row in rows] == [
        "2026-10-19T12:29:00+05:30",
        "2026-10-19T12:29:30+05:30",
    ]
    assert rows[0]["snapshot"]["underlying"]["momentum_pct"] == 0.12
    assert rows[1]["status"] == "OPTION_QUOTE_UNAVAILABLE"
    assert repository.scalp_monitor_snapshots_for_event(999, limit=10) == []
