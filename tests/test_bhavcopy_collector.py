from datetime import date

from collector.bhavcopy import collect_equity_bhavcopy, parse_equity_bhavcopy


CSV = """SYMBOL, SERIES, DATE1, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, TTL_TRD_QNTY
RELIANCE, EQ, 10-Sep-2026, 1400, 1420, 1390, 1410, 123456
RELIANCE, BE, 10-Sep-2026, 1400, 1420, 1390, 1410, 5
"""


def test_bhavcopy_parser_keeps_only_normalized_eq_bars():
    assert parse_equity_bhavcopy(CSV) == [{
        "symbol": "RELIANCE", "trade_date": "2026-09-10",
        "open": 1400.0, "high": 1420.0, "low": 1390.0,
        "close": 1410.0, "volume": 123456.0,
    }]


def test_collector_returns_empty_when_nse_archive_is_unavailable(monkeypatch):
    monkeypatch.setattr("collector.bhavcopy.fetch_equity_bhavcopy", lambda _: None)
    assert collect_equity_bhavcopy(date(2026, 9, 10)) == []


def test_bhavcopy_parser_accepts_udiff_columns():
    udiff = """TradDt,TckrSymb,SctySrs,OpnPric,HghPric,LwPric,ClsPric,TtlTradgVol
2026-09-10,RELIANCE,EQ,1400,1420,1390,1410,123456
2026-09-10,RELIANCE,BE,1400,1420,1390,1410,5
"""
    assert parse_equity_bhavcopy(udiff) == [{
        "symbol": "RELIANCE", "trade_date": "2026-09-10",
        "open": 1400.0, "high": 1420.0, "low": 1390.0,
        "close": 1410.0, "volume": 123456.0,
    }]
