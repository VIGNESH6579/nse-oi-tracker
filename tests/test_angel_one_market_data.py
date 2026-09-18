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
    result = client.fno_quotes(["INDIGO"])
    assert result["INDIGO"]["oi"] == 100000
    assert result["INDIGO"]["source"] == "angel_one_nfo_futures_fallback"
