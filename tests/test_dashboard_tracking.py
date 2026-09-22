"""The dashboard must show server-tracked results only: same on every device, no manual marking."""
import re
from pathlib import Path

INDEX = (Path(__file__).parents[1] / "static" / "index.html").read_text(encoding="utf-8")


def test_history_is_server_only_no_browser_log_and_no_manual_marking():
    for banned in ("localStorage", "indexedDB", "IndexedDB", "markTrade", "addTrade", "autoSaveSignals", "removeTrade",
                   "clearTrades", "STORAGE_KEY", "Mark:", "Clear All", "Paper track"):
        assert banned not in INDEX, banned
    assert "await fetch('/api/history/today')" in INDEX or "fetch('/api/history/today')" in INDEX
    assert "this.trades = data.events || []" in INDEX


def test_signal_history_shows_confirmed_trades_only_no_rejected_watchlist():
    # Signal History must show only setups that passed the confirmation
    # gate. A separate "Watchlist" listing of rejected candidates belongs
    # nowhere in that tab; the user explicitly does not want it there.
    assert "get tradeEvents()" in INDEX
    assert "passed every check" in INDEX
    assert "👁 Watchlist" not in INDEX
    assert "signals that did not pass the checks" not in INDEX
    for label in ("TG2 hit", "SL hit", "TG1 hit, stopped at entry", "Time exit"):
        assert label in INDEX
    assert "no manual marking" in INDEX


def test_focus_tab_explains_what_each_candidate_is_waiting_for():
    assert "['focus','signals','analytics','trades']" in INDEX and "activeTab: 'focus'" in INDEX
    assert "get focusReady()" in INDEX and "get focusWatch()" in INDEX and "missingLabel(code)" in INDEX
    for code in ("oi_window_disagrees", "persistence_short", "extended_from_open", "wrong_side_of_vwap", "no_daily_bars"):
        assert code in INDEX


def test_signal_table_uses_the_server_plan_so_levels_match_the_tracked_trade():
    assert "row.plan && row.plan.entry" in INDEX


def test_no_mangled_characters_in_visible_ui_strings():
    """A tool once replaced every emoji/rupee sign with '?'. Guard the visible ones."""
    assert "'?' + fmt2(" not in INDEX
    assert not re.search(r"emoji:'\?\?'", INDEX)
    assert "\u20b9" in INDEX and "\U0001F3AF Focus" in INDEX
    assert "<title>NSE F&O OI Scanner ??</title>" not in INDEX
    assert "FII/DII values are public" not in INDEX


def test_market_overview_reads_are_null_safe_before_data_arrives():
    unguarded = [m.group(0) for m in re.finditer(r"marketOverview\.(?!\?)[A-Za-z_]+", INDEX)
                 if "marketOverview?.nse_timestamp ?" not in INDEX[max(0, m.start() - 60):m.start()]]
    assert unguarded == []


def test_market_context_refreshes_on_the_same_60s_cycle_as_signals():
    """Previously fetchMarketOverview() ran only at page load and on manual click, so the four
    index CMPs (and VIX) went stale immediately while everything else kept refreshing."""
    assert "Promise.all([this.fetchSignals(), this.fetchMarketOverview()]).then(() => this.startTimer())" in INDEX
    assert "marketOverviewFetchedAt" in INDEX and "(stale)" in INDEX
