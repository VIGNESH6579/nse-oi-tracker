import importlib



import app.nse_fetcher as nse_fetcher





def _force_nse_fallback(module, monkeypatch):
    # These tests exercise NSE fallback enrichment, not the production Angel-primary path.
    monkeypatch.setattr(module, "_angel_fno_oi_rows", lambda: [])
    monkeypatch.setattr(module, "_LAST_NSE_SPURTS_AT", 0)
    monkeypatch.setattr(module, "NSE_OI_SPURTS_RETRY_S", 0)


def test_fetch_all_fno_oi_change_enriches_second_snapshot(monkeypatch):

    module = importlib.reload(nse_fetcher)
    _force_nse_fallback(module, monkeypatch)

    payloads = iter([

        {"data": [{"symbol": "ABC", "underlyingValue": "100", "oi": 1000}]},

        {"data": [{"symbol": "ABC", "underlyingValue": "105", "oi": 1000}]},

    ])

    monkeypatch.setattr(module._nse, "get", lambda *args, **kwargs: next(payloads))



    first = module.fetch_all_fno_oi_change()

    second = module.fetch_all_fno_oi_change()



    assert first[0]["symbol"] == "ABC"

    assert "pChange" not in first[0]

    assert second[0]["ltp"] == 105.0

    assert second[0]["change"] == 5.0

    assert second[0]["pChange"] == 5.0

    assert second[0]["price_source"] == "rolling_minute_fallback"





def test_native_price_change_is_never_overwritten_by_rolling_snapshot(monkeypatch):
    """
    Regression test for the core production bug: the app used to
    unconditionally replace NSE's own pChange/change fields with a noisy
    60-second poll-to-poll delta once a previous snapshot existed, even when
    NSE's payload already had a perfectly good (session/day-relative) value.
    That silently broke signal generation on every cycle after the first.
    """
    module = importlib.reload(nse_fetcher)
    _force_nse_fallback(module, monkeypatch)
    payloads = iter([
        # First poll: NSE gives us a native pChange already.
        {"data": [{"symbol": "XYZ", "underlyingValue": "100", "oi": 1000, "pChange": 1.25}]},
        # Second poll: underlying moved a lot in one minute (would produce a
        # huge synthetic pChange under the old buggy logic), but NSE still
        # supplies its own native value ? that must win.
        {"data": [{"symbol": "XYZ", "underlyingValue": "140", "oi": 1000, "pChange": 1.40}]},
    ])
    monkeypatch.setattr(module._nse, "get", lambda *args, **kwargs: next(payloads))

    first = module.fetch_all_fno_oi_change()
    second = module.fetch_all_fno_oi_change()

    assert first[0]["pChange"] == 1.25
    assert first[0]["price_source"] == "nse_native"
    # Must NOT be clobbered with (140-100)/100*100 == 40.0
    assert second[0]["pChange"] == 1.40
    assert second[0]["price_source"] == "nse_native"


def test_fetch_all_fno_oi_change_preserves_rows_with_invalid_price(monkeypatch):

    module = importlib.reload(nse_fetcher)

    monkeypatch.setattr(

        module._nse,

        "get",

        lambda *args, **kwargs: {"data": [{"symbol": "ABC", "underlyingValue": "not-a-number"}]},

    )



    rows = module.fetch_all_fno_oi_change()



    assert rows == [{"symbol": "ABC", "underlyingValue": "not-a-number"}]


def test_absolute_price_change_does_not_mask_percent_change_fallback(monkeypatch):
    module = importlib.reload(nse_fetcher)
    _force_nse_fallback(module, monkeypatch)
    payloads = iter([
        {"data": [{"symbol": "ABC", "underlyingValue": "100", "change": "0.50"}]},
        {"data": [{"symbol": "ABC", "underlyingValue": "102", "change": "2.00"}]},
    ])
    monkeypatch.setattr(module._nse, "get", lambda *args, **kwargs: next(payloads))

    module.fetch_all_fno_oi_change()
    second = module.fetch_all_fno_oi_change()

    assert second[0]["pChange"] == 2.0
    assert second[0]["price_source"] == "rolling_minute_fallback"


