import struct

from integrations.angel_one_stream import (
    AngelOneMarketStream,
    PROTOCOL_PING_INTERVAL_SECONDS,
    LocalFiveMinuteBuilder,
    TEXT_HEARTBEAT_INTERVAL_SECONDS,
    parse_stream_packet,
)


def _packet(*, mode=3, token=b"11536", ts=1727000000000, ltp=12345, volume=1000, oi=250000):
    data = bytearray(147)
    struct.pack_into("<BB", data, 0, mode, 2)
    data[2:27] = token.ljust(25, b"\x00")
    struct.pack_into("<qqq", data, 27, 7, ts, ltp)
    struct.pack_into("<qqq", data, 51, 10, 12340, volume)
    struct.pack_into("<dd", data, 75, 100.0, 200.0)
    struct.pack_into("<qqqq", data, 91, 12000, 12500, 11900, 12100)
    struct.pack_into("<qqq", data, 123, ts, oi, 1250)
    return bytes(data)


def test_stream_parser_reads_official_snap_quote_fields():
    tick = parse_stream_packet(_packet())
    assert tick.exchange_type == 2
    assert tick.token == "11536"
    assert tick.ltp == 123.45
    assert tick.volume == 1000
    assert tick.average_price == 123.40
    assert tick.open == 120.0
    assert tick.high == 125.0
    assert tick.low == 119.0
    assert tick.close == 121.0
    assert tick.oi == 250000
    assert tick.oi_change_pct == 12.5


def test_stream_parser_rejects_truncated_packets():
    try:
        parse_stream_packet(b"\x00" * 50)
    except ValueError as exc:
        assert "truncated" in str(exc)
    else:
        raise AssertionError("expected truncated packet rejection")


def test_local_candle_builds_five_minute_ohlc():
    builder = LocalFiveMinuteBuilder()
    first = parse_stream_packet(_packet(ltp=10000, volume=100))
    second = parse_stream_packet(_packet(ltp=10200, volume=120, ts=first.exchange_timestamp_ms + 60_000))
    candle = builder.add(first)
    candle = builder.add(second)
    assert candle["open"] == 100.0
    assert candle["high"] == 102.0
    assert candle["low"] == 100.0
    assert candle["close"] == 102.0


def test_local_candle_invalidates_volume_after_mid_session_reset():
    builder = LocalFiveMinuteBuilder()
    first = parse_stream_packet(_packet(volume=1000))
    second = parse_stream_packet(_packet(volume=900, ts=first.exchange_timestamp_ms + 60_000))
    builder.add(first)
    candle = builder.add(second)
    assert candle["volume_valid"] is False


def test_stream_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("ANGEL_ONE_STREAM_ENABLED", raising=False)
    stream = AngelOneMarketStream(None)
    assert stream.enabled is False
    assert stream.health()["order_execution"] is False
    assert stream.health()["volume_actionable"] is False


def test_subscription_cap_is_1000():
    stream = AngelOneMarketStream(None, enabled=True)
    added = stream.ensure_subscriptions([(1, str(i)) for i in range(1200)])
    assert added == 1000
    assert len(stream._subscriptions) == 1000


def test_ensure_symbols_accepts_two_digit_futures_expiry():
    class Instrument:
        def __init__(self, token, expiry):
            self.token = token
            self.expiry = expiry

    class MarketData:
        def _get_instruments(self):
            return {
                ("NSE", "RELIANCE"): Instrument("100", "01JAN2099"),
                ("NFO", "RELIANCE01NOV26FUT"): Instrument("200", "01Nov2026"),
            }

    stream = AngelOneMarketStream(MarketData(), enabled=True)
    added = stream.ensure_symbols(["RELIANCE"])
    assert added == 2
    assert (2, "200") in stream._subscriptions


def test_ensure_symbols_seeds_equity_and_nearest_futures():
    class Instrument:
        def __init__(self, token, expiry):
            self.token = token
            self.expiry = expiry

    class MarketData:
        def _get_instruments(self):
            return {
                ("NSE", "RELIANCE"): Instrument("100", "01JAN2099"),
                ("NFO", "RELIANCE01OCT2099FUT"): Instrument("200", "01Oct2099"),
                ("NFO", "RELIANCE01NOV2099FUT"): Instrument("201", "01Nov2099"),
            }

    stream = AngelOneMarketStream(MarketData(), enabled=True)
    added = stream.ensure_symbols(["RELIANCE"])
    assert added == 2
    assert (1, "100") in stream._subscriptions
    assert (2, "200") in stream._subscriptions
    assert (2, "201") not in stream._subscriptions


def test_stream_disables_library_protocol_ping():
    assert PROTOCOL_PING_INTERVAL_SECONDS == 0


def test_subscription_payload_uses_ten_character_correlation_id():
    stream = AngelOneMarketStream(None, enabled=True)

    class FakeWS:
        def __init__(self):
            self.payload = None

        def send(self, value):
            self.payload = value

    ws = FakeWS()
    stream._send_subscribe([(1, "100")], ws=ws)
    import json
    payload = json.loads(ws.payload)
    assert payload["correlationID"] == "nseoi00001"
    assert len(payload["correlationID"]) == 10
    assert payload["action"] == 1
    assert payload["params"]["mode"] == 3


def test_text_heartbeat_interval_is_thirty_seconds():
    assert TEXT_HEARTBEAT_INTERVAL_SECONDS == 30
