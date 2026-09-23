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



def test_signal_api_exposes_actual_data_source_status():
    assert '"primary_market_data_source": active_source' in MAIN
    assert '"angel_one_configured": angel_market_data is not None' in MAIN
    assert '"refresh_supported": True' in MAIN
    assert 'realtime_source' in MAIN


def test_refreshes_have_a_minimum_interval_guard():
    assert "MIN_REFRESH_INTERVAL_SECONDS = 45.0" in MAIN
    assert "Skipping duplicate signal refresh inside" in MAIN


def test_render_does_not_auto_deploy_during_market_hours():
    render = (ROOT / "render.yaml").read_text(encoding="utf-8")
    assert "autoDeployTrigger: off" in render
    for key in ("ANGEL_ONE_API_KEY", "ANGEL_ONE_CLIENT_CODE", "ANGEL_ONE_PASSWORD", "ANGEL_ONE_TOTP_SECRET"):
        assert f"key: {key}" in render


def test_startup_warns_when_bhavcopy_history_is_empty():
    assert "Daily bhavcopy history is empty" in MAIN
    assert "automatic bounded backfill" in MAIN
    assert "backfill_recent_bhavcopies" in MAIN


def test_dashboard_and_api_only_keep_required_sections():
    """Lean app: only focus / signals / signal-history tabs and the endpoints they use."""
    for tab in ("chain", "heatmap", "intel", "guide"):
        assert f"activeTab === '{tab}'" not in INDEX
    for route in ("/api/option-chain", "/api/market-intelligence", "/api/participant-oi", "/api/technical",
                  "/api/category", "/api/cas/", "/api/signal/{symbol}", "/api/market-regime", "/api/intraday"):
        assert route not in MAIN
    for kept in ("/api/oi-signals", "/api/history/today", "/api/analytics/today", "/api/market-overview", "/api/health"):
        assert kept in MAIN
    assert "['trades']" in INDEX
    assert "activeTab === 'signals'" not in INDEX
    assert "activeTab === 'analytics'" not in INDEX
    assert "/api/analytics/today" not in INDEX


def test_data_source_labels_are_truthful():
    assert 'oi_engine._last_scan_stats.get("oi_source")' in MAIN
    assert "NSE public feed" not in INDEX and "Public NSE Data" not in INDEX
    assert "OI: " in INDEX and "Price: " in INDEX
