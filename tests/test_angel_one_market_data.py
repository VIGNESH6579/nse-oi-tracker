from datetime import date

import pytest

from analytics.intraday import candle_vwap
from integrations.angel_one_market_data import AngelInstrument, AngelOneMarketData


def test_candle_vwap_uses_true_ohlcv_volume_weighting():
    candles = [
        {"high": 102, "low": 98, "close": 100, "volume": 10},
        {"high": 112, "low": 108, "close": 110, "volume": 30},
    ]
    assert candle_vwap(candles) == 107.5


def test_angel_client_is_read_only():
    public_methods = set(dir(AngelOneMarketData))
    forbidden = {"_".join((verb, "order")) for verb in ("place", "modify", "cancel")}
    assert not public_methods.intersection(forbidden)


def test_nse_instrument_symbols_normalize_equity_suffix():
    assert AngelOneMarketData._lookup_symbol("RELIANCE-EQ") == "RELIANCE"
    assert AngelOneMarketData._lookup_symbol("RELIANCE") == "RELIANCE"
    assert AngelOneMarketData._lookup_symbol("BANKNIFTY", exchange="NFO") == "BANKNIFTY"


def test_render_angel_variable_aliases_configure_read_only_client(monkeypatch):
    monkeypatch.setenv("ANGEL_API_KEY", "key")
    monkeypatch.setenv("ANGEL_CLIENT_ID", "client")
    monkeypatch.setenv("ANGEL_PASSWORD", "password")
    monkeypatch.setenv("ANGEL_TOTP_SECRET", "totp")
    monkeypatch.delenv("ANGEL_ONE_API_KEY", raising=False)
    monkeypatch.delenv("ANGEL_ONE_CLIENT_CODE", raising=False)
    monkeypatch.delenv("ANGEL_ONE_PASSWORD", raising=False)
    monkeypatch.delenv("ANGEL_ONE_TOTP_SECRET", raising=False)

    client = AngelOneMarketData.from_environment()

    assert client is not None
    assert client.configured is True
    assert client.client_code == "client"
    assert not {"place_order", "modify_order", "cancel_order"}.intersection(dir(client))


def test_fno_quotes_maps_nearest_future_to_underlying(monkeypatch):
    client = AngelOneMarketData(api_key="k", client_code="c", password="p", totp_secret="t")
    instruments = {
        ("NFO", "INDIGO25SEP26FUT"): AngelInstrument(
            "INDIGO25SEP26FUT", "1", "NFO", "25SEP2026", "FUTIDX"
        ),
    }
    monkeypatch.setattr(client, "_login", lambda: None)
    monkeypatch.setattr(client, "_get_instruments", lambda: instruments)
    monkeypatch.setattr(client, "full_quotes", lambda symbols, exchange="NFO": {
        "INDIGO25SEP26FUT": {"ltp": 5000, "oi": 100000, "change_pct": 1.2}
    })
    result = client.fno_quotes(["INDIGO"], today=date(2026, 9, 21))      # fixed clock: contract must not depend on the real date
    assert result["INDIGO"]["oi"] == 100000
    assert result["INDIGO"]["source"] == "angel_one_nfo_futures_fallback"


# ---------------- quote validation: today's range only, never the previous close ----------------
import integrations.angel_one_market_data as angel


def _quote_client(monkeypatch, rows):
    client = AngelOneMarketData(api_key="k", client_code="c", password="p", totp_secret="t")
    client._login = lambda: None
    client._headers = lambda: {}
    client._get_instruments = lambda: {("NSE", "TEST"): AngelInstrument("TEST", "1", "NSE")}

    class Resp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"status": True, "data": {"fetched": rows}}

    monkeypatch.setattr(angel.requests, "post", lambda *a, **k: Resp())
    return client


def _row(**over):
    row = {"symbolToken": "1", "tradingSymbol": "TEST-EQ", "ltp": 105, "open": 101, "high": 106, "low": 100, "close": 99,
           "tradeVolume": 1000, "opnInterest": 0, "percentChange": 6.06}
    row.update(over)
    return row


def test_gap_up_and_hold_quote_is_accepted_even_though_low_is_above_previous_close(monkeypatch):
    # prev close 99, day range 100-106: low (100) > previous close (99). Valid, and exactly the
    # strong gap-and-go mover the scanner exists to find. The old rule dropped it as "malformed".
    out = _quote_client(monkeypatch, [_row()]).full_quotes(["TEST"])
    assert out["TEST"]["ltp"] == 105 and out["TEST"]["close"] == 99


def test_gap_down_and_hold_quote_is_accepted_even_though_high_is_below_previous_close(monkeypatch):
    out = _quote_client(monkeypatch, [_row(ltp=95, open=97, high=98, low=94, close=100)]).full_quotes(["TEST"])
    assert out["TEST"]["ltp"] == 95


@pytest.mark.parametrize("bad", [
    {"ltp": 0},                          # no price
    {"open": 0, "high": 0, "low": 0},    # before the open: no session range yet
    {"high": 104},                       # high below ltp: internally inconsistent
    {"low": 103},                        # low above open: internally inconsistent
])
def test_genuinely_malformed_quotes_are_still_rejected(monkeypatch, bad):
    assert _quote_client(monkeypatch, [_row(**bad)]).full_quotes(["TEST"]) == {}


# ---------------- candle API rate limit: retry once, then escalate the cooldown ----------------
class _Http:
    def __init__(self, status, body=None):
        self.status_code = status
        self.text = "Access denied because of exceeding access rate" if status == 403 else ""
        self._body = body or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP Error {self.status_code}")

    def json(self):
        return self._body


_OK = {"status": True, "data": [["2026-09-21T09:15:00+0530", 100, 101, 99, 100.5, 1000]]}


def _candle_client(monkeypatch, responses):
    client = AngelOneMarketData(api_key="k", client_code="c", password="p", totp_secret="JBSWY3DPEHPK3PXP")
    client._login = lambda: None
    client._headers = lambda: {}
    client.instrument = lambda symbol, exchange="NSE": AngelInstrument("TCS", "11536", "NSE")
    seq = iter(responses)
    calls = []
    monkeypatch.setattr(angel.requests, "post", lambda *a, **k: calls.append(1) or next(seq))
    monkeypatch.setenv("ANGEL_HIST_MIN_INTERVAL", "0")
    monkeypatch.setenv("ANGEL_HIST_QUOTE_GAP_S", "0")
    monkeypatch.setenv("ANGEL_HIST_RETRY_WAIT_S", "0")
    return client, calls


def test_a_single_403_is_retried_and_does_not_trigger_the_cooldown(monkeypatch):
    client, calls = _candle_client(monkeypatch, [_Http(403), _Http(200, _OK), _Http(200, _OK)])
    candles = client.intraday_candles("TCS")
    assert len(candles) == 1 and len(calls) == 2
    assert client._hist_block_until == 0.0 and client._hist_consecutive_403 == 0
    assert len(client.intraday_candles("TCS")) == 1 and len(calls) == 3       # not blocked: the next call proceeds


def test_two_consecutive_403s_start_a_cooldown_that_escalates(monkeypatch):
    client, calls = _candle_client(monkeypatch, [_Http(403)] * 4)
    monkeypatch.setenv("ANGEL_HIST_COOLDOWN_S", "45")
    with pytest.raises(RuntimeError, match="exceeding access rate"):
        client.intraday_candles("TCS")
    assert len(calls) == 2 and client._hist_consecutive_403 == 1          # one try + one retry, then cool down
    first_block = client._hist_block_until
    with pytest.raises(angel.AngelUnavailable):                              # no request leaves while cooling down
        client.intraday_candles("TCS")
    assert len(calls) == 2
    client._hist_block_until = 0.0                                          # cooldown elapsed
    with pytest.raises(RuntimeError, match="exceeding access rate"):
        client.intraday_candles("TCS")
    assert client._hist_consecutive_403 == 2
    import time as _t
    assert client._hist_block_until - _t.monotonic() > 60                   # 45s -> 90s: it escalated
    assert first_block > 0


def test_option_chain_near_atm_shape(monkeypatch):
    client = AngelOneMarketData(api_key="k", client_code="c", password="p", totp_secret="t")
    monkeypatch.setattr(client, "load_option_instruments", lambda underlying: [
        {"symbol": "RELIANCE25000CE", "token": "1", "expiry": "30OCT2026", "strike": 2500.0, "option_type": "CE"},
        {"symbol": "RELIANCE25000PE", "token": "2", "expiry": "30OCT2026", "strike": 2500.0, "option_type": "PE"},
        {"symbol": "RELIANCE25500CE", "token": "3", "expiry": "30OCT2026", "strike": 2550.0, "option_type": "CE"},
        {"symbol": "RELIANCE25500PE", "token": "4", "expiry": "30OCT2026", "strike": 2550.0, "option_type": "PE"},
    ])
    monkeypatch.setattr(client, "quotes_by_token", lambda tokens, exchange="NFO": {
        token: {"ltp": 100 + int(token), "oi": 20000, "volume": 500, "symbol": token}
        for token in tokens
    })
    out = client.option_chain_near_atm("RELIANCE", 2525, strikes_each_side=1)
    assert out["ok"] is True
    assert out["atm_strike"] == 2500.0
    assert out["chain"] and out["chain"][0]["ce"]["ltp"] > 0 and out["chain"][0]["pe"]["ltp"] > 0
