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


def test_signal_history_excludes_candidate_rows():
    assert "get tradeEvents() { return this.trades.filter(t => (t.tier || 'TRADE') === 'TRADE'" in INDEX
    assert "['TRADE', 'CANDIDATE']" not in INDEX


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
    assert "entry, exit, target and stop tracked automatically" in INDEX


def test_single_confirmed_history_tab_replaces_focus_and_raw_signals():
    assert "['signals','trades']" in INDEX and "activeTab: 'signals'" in INDEX
    assert "Confirmed Signal History" in INDEX
    assert "tradeEvents" in INDEX and "No signal has passed every check yet today." in INDEX
    assert "x-for=\"tab in ['signals','trades']\"" in INDEX


def test_signal_table_uses_the_server_plan_so_levels_match_the_tracked_trade():
    assert "row.plan && row.plan.entry" in INDEX


def test_confirmed_history_keeps_only_trade_levels_and_times():
    assert "Entry time" in INDEX and "Exit" in INDEX
    assert "auto-tracked · checked every 30s" not in INDEX


def test_no_mangled_characters_in_visible_ui_strings():
    """A tool once replaced every emoji/rupee sign with '?'. Guard the visible ones."""
    assert "'?' + fmt2(" not in INDEX
    assert not re.search(r"emoji:'\?\?'", INDEX)
    assert "\u20b9" in INDEX and "Confirmed Signal History" in INDEX
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


def test_frontend_removes_unreachable_focus_panel_and_marks_paper_signals():
    assert "activeTab === 'focus'" not in INDEX
    assert "PAPER BUY SIGNAL" in INDEX


def test_signal_table_keeps_detected_patterns_when_confirmation_gate_blocks_trade():
    """Live OI patterns must remain visible as OBSERVE · NO TRADE when the safety gate fails."""
    assert "let list = this.signals.slice();" in INDEX
    assert "signalCount(key)" in INDEX
    assert "get actionableTotal()" in INDEX
    assert "OBSERVE · NO TRADE" in INDEX


def test_history_shows_exact_entry_seconds_and_refreshes_with_live_monitor():
    assert "second:'2-digit'" in INDEX
    assert " + ' IST'" in INDEX
    assert "setInterval(() => this.loadServerHistory(), 30000)" in INDEX


def test_history_shows_server_tracked_current_ltp_separately_from_entry():
    assert "Current LTP" in INDEX
    assert "t.currentLTP" in INDEX
