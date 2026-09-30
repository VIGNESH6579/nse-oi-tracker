from datetime import date, datetime

from collector import index_backfill
from utils.time import IST


class FakeRepository:
    def __init__(self):
        self.saved = []

    def upsert_daily_index_bars(self, bars):
        rows = list(bars)
        self.saved.extend(rows)
        return len(rows)


def test_index_backfill_uses_ist_previous_day(monkeypatch):
    captured = []
    monkeypatch.setattr(
        index_backfill,
        "now_ist",
        lambda: datetime(2026, 9, 17, 0, 15, tzinfo=IST),
    )

    def fetch(index_type, start, end):
        captured.append((index_type, start, end))
        return []

    monkeypatch.setattr(index_backfill, "fetch_index_history", fetch)
    index_backfill.backfill_index_bars(FakeRepository(), days=2)

    assert captured[0][1:] == (date(2026, 9, 12), date(2026, 9, 16))


def test_index_backfill_continues_after_one_index_fetch_fails(monkeypatch):
    repository = FakeRepository()
    requested = []

    def fetch(index_type, start, end):
        requested.append(index_type)
        if index_type == "NIFTY 50":
            raise RuntimeError("NSE historical endpoint blocked")
        return [{
            "trade_date": "2026-09-16",
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100.5,
        }]

    monkeypatch.setattr(index_backfill, "fetch_index_history", fetch)
    result = index_backfill.backfill_index_bars(repository, days=2)

    assert requested == [
        "NIFTY 50",
        "NIFTY BANK",
        "NIFTY FINANCIAL SERVICES",
        "NIFTY MIDCAP SELECT",
    ]
    assert result["stored"] == 3
    assert {row["symbol"] for row in repository.saved} == {
        "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY",
    }



def test_index_backfill_falls_back_to_nse_when_angel_history_is_unavailable(monkeypatch):
    repository = FakeRepository()
    calls = []

    class Angel:
        def daily_candles(self, symbol, days, exchange):
            calls.append(("angel", symbol))
            raise RuntimeError("HTTP 403 rate limit")

    def fetch(index_type, start, end):
        calls.append(("nse", index_type))
        return [{
            "trade_date": "2026-09-16",
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100.5,
        }]

    monkeypatch.setattr(index_backfill, "fetch_index_history", fetch)
    result = index_backfill.backfill_index_bars(
        repository,
        days=2,
        angel_client=Angel(),
    )

    assert len([c for c in calls if c[0] == "angel"]) == 4
    assert len([c for c in calls if c[0] == "nse"]) == 4
    assert result["stored"] == 4
    assert all(row["source"] == "nse_index_history" for row in repository.saved)
