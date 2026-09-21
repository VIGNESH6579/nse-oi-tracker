from datetime import date

from collector.index_backfill import backfill_index_bars
from integrations.angel_one_market_data import AngelOneMarketData


class FakeAngel:
    def daily_candles(self, symbol, *, days, exchange):
        return [
            {"time": "2026-09-18T00:00:00+05:30", "open": 100, "high": 110, "low": 95, "close": 105},
        ]


class FakeRepository:
    def __init__(self):
        self.rows = []

    def upsert_daily_index_bars(self, rows):
        self.rows.extend(rows)
        return len(rows)


def test_daily_candles_delegates_to_one_day_endpoint(monkeypatch):
    client = AngelOneMarketData.__new__(AngelOneMarketData)
    monkeypatch.setattr(client, "intraday_candles", lambda symbol, *, interval, exchange, days: [{"interval": interval, "days": days}])
    assert client.daily_candles("NIFTY", days=80) == [{"interval": "ONE_DAY", "days": 80}]


def test_index_backfill_uses_angel_daily_candles(monkeypatch):
    from datetime import datetime
    from utils.time import IST
    monkeypatch.setattr("collector.index_backfill.now_ist", lambda: datetime(2026, 9, 19, 12, 0, tzinfo=IST))   # date-independent
    repository = FakeRepository()
    result = backfill_index_bars(repository, days=1, angel_client=FakeAngel())
    assert result["source"] == "angel_one_daily_index"
    assert result["stored"] == 4
    assert {row["symbol"] for row in repository.rows} == {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}
    assert all(row["trade_date"] == "2026-09-18" for row in repository.rows)


def test_vwap_context_source_is_broker_grade_in_main_source():
    source = open("app/main.py", encoding="utf-8").read()
    assert '"source": "angel_one_5m_ohlcv"' in source
    assert 'interval="FIVE_MINUTE"' in source


def test_index_aliases_match_angel_instrument_master():
    source = open("integrations/angel_one_market_data.py", encoding="utf-8").read()
    assert '"NIFTY": "NIFTY 50"' in source
    assert '"FINNIFTY": "NIFTY FIN SERVICE"' in source
    assert '"MIDCPNIFTY": "NIFTY MID SELECT"' in source
