import json
from datetime import date

import pytest

import integrations.angel_one_market_data as angel
from integrations.angel_one_market_data import AngelInstrument, AngelOneMarketData
from utils.jsonstream import iter_json_array


def _client():
    c = AngelOneMarketData(api_key="k", client_code="c", password="p", totp_secret="JBSWY3DPEHPK3PXP")
    c._login = lambda: None
    return c


def test_iter_json_array_streams_rows():
    text = json.dumps([{"a": 1}, {"b": [1, 2, {"c": "x,]"}]}, 3]) + "  "
    assert list(iter_json_array(text)) == [{"a": 1}, {"b": [1, 2, {"c": "x,]"}]}, 3]
    assert list(iter_json_array("[]")) == [] and list(iter_json_array("nothing")) == []


class _Resp:
    def __init__(self, rows):
        self.text = json.dumps(rows)
    def raise_for_status(self):
        pass


def test_master_is_filtered_to_needed_rows(monkeypatch):
    rows = [{"exch_seg": "NSE", "symbol": "TCS-EQ", "token": "1", "instrumenttype": ""},
            {"exch_seg": "NSE", "symbol": "TCS-BE", "token": "2", "instrumenttype": ""},
            {"exch_seg": "NSE", "symbol": "Nifty 50", "token": "99926000", "instrumenttype": "AMXIDX"},
            {"exch_seg": "NFO", "symbol": "TCS30SEP26FUT", "token": "3", "instrumenttype": "FUTSTK", "expiry": "30SEP2026"},
            {"exch_seg": "NFO", "symbol": "TCS30SEP262000CE", "token": "4", "instrumenttype": "OPTSTK", "expiry": "30SEP2026"},
            {"exch_seg": "BSE", "symbol": "X", "token": "5", "instrumenttype": ""}]
    monkeypatch.setattr(angel.requests, "get", lambda *a, **k: _Resp(rows))
    inst = _client()._get_instruments()
    assert set(inst) == {("NSE", "TCS"), ("NSE", "NIFTY 50"), ("NFO", "TCS30SEP26FUT")}


def _fut(sym, expiry):
    return ("NFO", sym), AngelInstrument(symbol=sym, token=sym, exchange="NFO", expiry=expiry, instrument_type="FUTSTK")


def test_fno_quotes_expiry_by_date_names_rollover_and_baseline():
    c = _client()
    c._instruments = dict([
        _fut("TCS30SEP26FUT", "30SEP2026"), _fut("TCS28OCT26FUT", "28OCT2026"),   # string compare would pick OCT
        _fut("BAJAJ-AUTO30SEP26FUT", "30SEP2026"), _fut("360ONE30SEP26FUT", "30SEP2026"), _fut("M&M30SEP26FUT", "30SEP2026"),
    ])
    oi = {"v": 1000.0}

    def fake_quotes(symbols, exchange="NFO"):
        return {s: {"ltp": 100.0, "oi": oi["v"], "volume": 5, "change_pct": 1.0, "close": 99} for s in symbols}
    c.full_quotes = fake_quotes

    far = date(2026, 9, 10)                                   # 20 days to expiry: no rollover sum
    q = c.fno_quotes(today=far)
    assert set(q) == {"TCS", "BAJAJ-AUTO", "360ONE", "M&M"}
    assert q["TCS"]["oi"] == 1000 and q["TCS"]["oi_change"] == 0 and not q["TCS"]["oi_includes_next_month"]
    oi["v"] = 1100.0
    q = c.fno_quotes(today=far)                               # same day: baseline is the first observation
    assert q["TCS"]["oi_change"] == 100 and round(q["TCS"]["oi_change_pct"], 1) == 10.0

    near = date(2026, 9, 28)                                  # 2 days to expiry: add next-month OI (TCS only has 2 months)
    oi["v"] = 1000.0
    q = c.fno_quotes(today=near)
    assert q["TCS"]["oi"] == 2000 and q["TCS"]["oi_includes_next_month"] is True
    assert q["M&M"]["oi"] == 1000                              # no next-month contract available


def test_intraday_candles_use_ist_session_window(monkeypatch):
    from datetime import datetime
    from utils.time import IST
    c = _client()
    c.instrument = lambda symbol, exchange="NSE": AngelInstrument("TCS", "11536", "NSE")
    c._headers = lambda: {}
    sent = {}

    class R:
        def raise_for_status(self): pass
        def json(self): return {"status": True, "data": [["2026-09-21T09:15:00+0530", 1, 2, 1, 2, 10]]}

    def fake_post(url, headers=None, json=None, timeout=None):
        sent.update(json)
        return R()
    monkeypatch.setattr(angel.requests, "post", fake_post)
    monkeypatch.setattr(angel, "now_ist", lambda: datetime(2026, 9, 21, 11, 5, 30, tzinfo=IST))
    assert len(c.intraday_candles("TCS")) == 1
    assert sent["fromdate"] == "2026-09-21 09:15" and sent["todate"] == "2026-09-21 11:05"
