from pathlib import Path


INDEX = Path(__file__).parents[1] / "static" / "index.html"


def test_trade_history_is_scoped_to_the_current_day():
    source = INDEX.read_text(encoding="utf-8")
    assert "one independent trade list per IST day" in source
    assert "Daily history — resets at midnight IST" in source
    assert "localStorage.getItem(`${STORAGE_KEY}:${today}`)" in source
    assert "Clear all saved trades? This cannot be undone." in source


def test_trade_history_has_all_history_fallback():
    source = INDEX.read_text(encoding="utf-8")
    assert "Array.isArray(parsed) ? parsed : []" in source
    assert "localStorage.setItem(`${STORAGE_KEY}:${today}`, JSON.stringify(this.trades));" in source




def test_trade_save_records_date_and_migrates_only_same_day_legacy_data():
    source = INDEX.read_text(encoding="utf-8")
    assert "tradeDate:        this.todayIST()" in source
    assert "localStorage.getItem(STORAGE_DATE) === today" in source
    assert "Never resurrect an older day's history" in source
    assert "Trade history could not be saved in browser storage" in source


def test_trade_history_has_indexeddb_redundant_backup():
    source = INDEX.read_text(encoding="utf-8")
    assert "indexedDB.open('nse_oi_tracker_history', 1)" in source
    assert "saveTradesToIndexedDB(today, this.trades)" in source
    assert "loadTradesFromIndexedDB()" in source
    assert "!this._dailyStoragePresent" in source


def test_received_signals_are_auto_saved_once_per_day():
    source = INDEX.read_text(encoding="utf-8")
    assert "this.autoSaveSignals(this.signals);" in source
    assert "if (!this.alreadyAdded(row.symbol)) this.addTrade(row, true);" in source
    assert "addTrade(row, silent=false)" in source
    assert "if (!silent) this.activeTab = 'trades';" in source
