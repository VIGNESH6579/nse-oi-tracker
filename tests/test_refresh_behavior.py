from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
MAIN = (ROOT / "app" / "main.py").read_text(encoding="utf-8")


def test_manual_refresh_bypasses_signal_market_and_news_caches():
    assert "this.fetchSignals(true)" in INDEX
    assert "this.fetchMarketOverview(true)" in INDEX
    assert "this.fetchNews(true)" in INDEX
    assert "newsRefreshTimer = setInterval(() => this.fetchNews(true), 180000)" in INDEX


def test_empty_successful_signal_scan_does_not_force_another_upstream_fetch():
    assert "if cached is None and is_market_open():" in MAIN
    assert "cached is None or len(cached) == 0" not in MAIN


def test_news_endpoint_supports_explicit_upstream_refresh():
    assert "async def corporate_news" in MAIN
    assert "if refresh:" in MAIN
    assert "fetch_corporate_announcements" in MAIN
    assert "upsert_corporate_announcements" in MAIN


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
