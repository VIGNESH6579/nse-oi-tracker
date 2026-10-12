from analytics.sources import public_source_inventory


def test_source_inventory_separates_active_and_unconfigured_coverage():
    rows = public_source_inventory()
    assert any(row["source"] == "NSE option chain" and row["status"] == "BEST_EFFORT_FALLBACK" for row in rows)
    assert any(row["source"] == "BSE/media/macro feeds" and row["status"] == "NOT_CONFIGURED" for row in rows)



def test_sources_inventory_does_not_claim_nse_sources_are_always_active():
    from analytics.sources import public_source_inventory

    sources = public_source_inventory(
        oi_source="angel_futures",
        oi_data_status="RECEIVED",
        angel_configured=True,
        stream_health={"state": "connected", "last_tick_age_s": 0.4},
        market_open=True,
        history_latest_trade_date="2026-10-12",
        expected_bhavcopy_date="2026-10-12",
    )
    by_name = {row["source"]: row for row in sources}
    assert by_name["Angel One NFO futures quotes/OI"]["status"] == "ACTIVE"
    assert by_name["Angel One WebSocket ticks/5-minute bars"]["status"] == "LIVE"
    assert by_name["NSE OI spurts"]["status"] == "STANDBY_FALLBACK"
    assert by_name["NSE equity bhavcopy/history"]["status"] == "CURRENT"


def test_sources_inventory_reports_stale_history_and_unavailable_live_rows():
    from analytics.sources import public_source_inventory

    sources = public_source_inventory(
        oi_source="unavailable",
        oi_data_status="UNAVAILABLE",
        angel_configured=True,
        stream_health={"state": "disconnected"},
        history_latest_trade_date="2026-10-09",
        expected_bhavcopy_date="2026-10-12",
    )
    by_name = {row["source"]: row for row in sources}
    assert by_name["Angel One NFO futures quotes/OI"]["status"] == "CONFIGURED_NO_LIVE_OI_ROWS"
    assert by_name["NSE OI spurts"]["status"] == "UNAVAILABLE"
    assert by_name["NSE equity bhavcopy/history"]["status"] == "STALE_OR_MISSING"



def test_stream_inventory_does_not_call_stale_connected_ticks_live():
    from analytics.sources import public_source_inventory

    rows = public_source_inventory(
        angel_configured=True,
        stream_health={"state": "connected", "last_tick_age_s": 240},
        market_open=True,
    )
    stream = next(row for row in rows if row["source"] == "Angel One WebSocket ticks/5-minute bars")
    assert stream["status"] == "STALE_TICKS"


def test_stream_inventory_marks_after_hours_connection_without_claiming_live_data():
    from analytics.sources import public_source_inventory

    rows = public_source_inventory(
        angel_configured=True,
        stream_health={"state": "connected", "last_tick_age_s": 240},
        market_open=False,
    )
    stream = next(row for row in rows if row["source"] == "Angel One WebSocket ticks/5-minute bars")
    assert stream["status"] == "CONNECTED_MARKET_CLOSED"


def test_stream_inventory_handles_missing_tick_timestamp_fail_closed():
    from analytics.sources import public_source_inventory

    rows = public_source_inventory(
        angel_configured=True,
        stream_health={"state": "connected"},
        market_open=True,
    )
    stream = next(row for row in rows if row["source"] == "Angel One WebSocket ticks/5-minute bars")
    assert stream["status"] == "CONNECTED_NO_TICK_TIMESTAMP"
