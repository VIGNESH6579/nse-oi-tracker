from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
MAIN = (ROOT / "app" / "main.py").read_text(encoding="utf-8")


def test_manual_refresh_bypasses_signal_and_market_caches_and_news_feed_is_gone():
    assert "this.fetchSignals(true)" in INDEX
    assert "this.fetchMarketOverview(true)" in INDEX
    # NSE disclosure/news feed was removed: no endpoint call, tab, panel or polling timer.
    assert "/api/news" not in INDEX
    assert "activeTab === 'news'" not in INDEX
    assert "newsRefreshTimer = setInterval" not in INDEX
    assert '@app.get("/api/news")' not in MAIN
    assert "fetch_corporate_announcements(" not in MAIN.replace("    fetch_corporate_announcements,", "")


def test_empty_successful_signal_scan_does_not_force_another_upstream_fetch():
    assert "if cached is None and is_market_open():" in MAIN
    assert "cached is None or len(cached) == 0" not in MAIN




def test_live_signal_list_preserves_first_detected_time():
    assert "_signal_first_seen: dict[str, str]" in MAIN
    assert '"detected_at_ist": _signal_first_seen.get' in MAIN
    assert 'x-text="hm(row.detected_at_ist)"' in INDEX
    assert '<th class="px-3 py-2.5 text-right font-medium">Added</th>' in INDEX

def test_signal_api_exposes_actual_data_source_status():
    assert '"primary_market_data_source": active_source' in MAIN
    assert '"angel_one_configured": angel_market_data is not None' in MAIN
    assert '"refresh_supported": True' in MAIN
    assert 'realtime_source' in MAIN


def test_angel_candles_are_preferred_over_quote_opening_range_proxy():
    assert 'intraday_candles(symbol, interval="FIVE_MINUTE"' in MAIN
    assert 'if qctx and qctx["or_complete"]' not in MAIN


def test_empty_candles_fall_back_to_exchange_vwap_same_as_the_exception_path():
    # A 200 response with an unusable/empty candle list must not be treated
    # differently from a failed call: both should fall back to the exchange
    # VWAP quote (qctx) rather than silently keeping the weaker observation
    # VWAP. Previously only the except-block did this.
    assert "elif qctx:" in MAIN
    empty_branch = MAIN.split("if summary.get(\"available\"):", 1)[1].split("except Exception as exc:", 1)[0]
    assert "elif qctx:" in empty_branch
    assert "context = qctx" in empty_branch
    # The fallback must stay labelled as quote-based, never as real candles,
    # so it can never satisfy the FIVE_MINUTE gate on its own (fail-closed).
    assert '"data_frequency": "scan"' in (ROOT / "analytics" / "intraday_confirm.py").read_text(encoding="utf-8")


def test_gate_only_accepts_genuine_five_minute_candles_as_actionable():
    confirmation = (ROOT / "signal_engine" / "confirmation.py").read_text(encoding="utf-8")
    assert 'intraday.get("data_frequency") == "FIVE_MINUTE"' in confirmation
    assert 'int(intraday.get("candle_count") or 0) >= 3' in confirmation


def test_fii_dii_cash_activity_is_not_advertised_as_signal_input():
    sources = (ROOT / "analytics" / "sources.py").read_text(encoding="utf-8")
    overview = (ROOT / "analytics" / "market_overview.py").read_text(encoding="utf-8")
    assert '"status": "NOT_USED"' in sources
    assert "intentionally omitted from signal logic" in overview


def test_refreshes_have_a_minimum_interval_guard():
    assert "MIN_REFRESH_INTERVAL_SECONDS = 45.0" in MAIN
    assert "Skipping duplicate signal refresh inside" in MAIN


def test_render_auto_deploys_on_main_and_stays_free():
    render = (ROOT / "render.yaml").read_text(encoding="utf-8")
    assert "autoDeployTrigger: commit" in render
    assert "plan: free" in render
    assert 'value: "60"' in render
    for key in ("ANGEL_ONE_API_KEY", "ANGEL_ONE_CLIENT_CODE", "ANGEL_ONE_PASSWORD", "ANGEL_ONE_TOTP_SECRET"):
        assert f"key: {key}" in render




def test_startup_backfill_is_only_started_when_history_is_incomplete():
    assert "startup_backfill_needed = bhavcopy_backfill_required(repository.daily_equity_bar_summary())" in MAIN
    assert "startup_backfill_needed = True if" not in MAIN
    assert "timeout=450" in MAIN

def test_startup_blocks_until_bounded_bhavcopy_backfill_and_self_test():
    assert "Startup readiness BLOCKED until background Bhavcopy backfill/self-test completes" in MAIN
    assert "bounded backfill" in MAIN
    assert "backfill_recent_bhavcopies" in MAIN


def test_dashboard_and_api_only_keep_required_sections():
    """Lean app: only focus / signals / signal-history tabs and the endpoints they use."""
    for tab in ("chain", "heatmap", "intel", "guide"):
        assert f"activeTab === '{tab}'" not in INDEX
    for route in ("/api/market-intelligence", "/api/participant-oi", "/api/technical",
                  "/api/category", "/api/cas/", "/api/signal/{symbol}", "/api/market-regime", "/api/intraday"):
        assert route not in MAIN
    assert '@app.get("/api/option-chain/{symbol}")' in MAIN
    for kept in ("/api/oi-signals", "/api/history/today", "/api/analytics/today", "/api/market-overview", "/api/health"):
        assert kept in MAIN
    assert "['signals','trades']" in INDEX
    assert "x-for=\"tab in ['signals','trades']\"" in INDEX
    assert "activeTab === 'analytics'" not in INDEX
    assert "/api/analytics/today" not in INDEX


def test_data_source_labels_are_truthful():
    assert 'oi_engine._last_scan_stats.get("oi_source")' in MAIN
    assert "NSE public feed" not in INDEX and "Public NSE Data" not in INDEX
    assert "OI: " in INDEX and "Price: " in INDEX


def test_stream_context_preserves_observed_opening_range():
    assert '"or_high": qctx.get("or_high")' in MAIN
    assert '"or_low": qctx.get("or_low")' in MAIN
    assert '"or_complete": True' in MAIN


def test_end_of_day_confirmed_signal_cleanup_is_scheduled():
    assert "scheduled_confirmed_signal_cleanup" in MAIN
    assert 'CronTrigger(hour=23, minute=59, timezone=IST)' in MAIN
    assert 'id="daily-confirmed-signal-cleanup"' in MAIN


def test_startup_catches_up_previous_day_confirmed_signal_cleanup():
    assert "Free Render instances may sleep through 23:59 IST" in MAIN
    assert "previous_trade_date = (now_ist().date() - timedelta(days=1)).isoformat()" in MAIN
    assert "repository.delete_confirmed_signals_for_date" in MAIN


def test_option_chain_route_never_raises_bare_500():
    assert '"ok": False' in MAIN
    assert '"source": "angel_one_on_demand_opt"' in MAIN
