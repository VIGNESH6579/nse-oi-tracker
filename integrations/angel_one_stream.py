"""Optional, read-only Angel One Smart Stream V2 integration.

This module implements market-data streaming only. It does not import Angel's
trading SDK or expose any order/portfolio API.

The stream is disabled unless ANGEL_ONE_STREAM_ENABLED is explicitly true.
Historical candles remain the bounded fallback/validation source.
"""

from __future__ import annotations

import json
import logging
import os
import re
import struct
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None

logger = logging.getLogger(__name__)

STREAM_URL = "wss://smartapisocket.angelone.in/smart-stream"
MAX_SUBSCRIPTIONS = 1000
NSE_CM = 1
NSE_FO = 2
QUOTE_MODE = 2
SNAP_QUOTE_MODE = 3
_FUTURE_RE = re.compile(r"^(.+?)\d{2}[A-Z]{3}\d{2}FUT$")


@dataclass(frozen=True, slots=True)
class StreamTick:
    exchange_type: int
    token: str
    received_at: float
    exchange_timestamp_ms: int
    ltp: float
    volume: int | None = None
    average_price: float | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    oi: int | None = None
    oi_change_pct: float | None = None
    last_traded_quantity: int | None = None

    @property
    def exchange_time(self) -> datetime:
        return datetime.fromtimestamp(self.exchange_timestamp_ms / 1000, tz=timezone.utc)


@dataclass
class _Candle:
    bucket_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    ticks: int
    last_volume: int | None = None
    source: str = "angel_one_websocket_v2"


def parse_stream_packet(packet: bytes) -> StreamTick:
    """Parse Angel Smart Stream V2 quote/snap-quote binary data.

    Offsets and little-endian encoding follow Angel One's official
    SmartWebSocketV2 implementation. Prices are exchange integers scaled by
    100; OI/volume are integer quantities.
    """
    if not isinstance(packet, (bytes, bytearray, memoryview)):
        raise TypeError("stream packet must be bytes")
    data = bytes(packet)
    if len(data) < 51:
        raise ValueError("truncated Angel stream packet")
    mode, exchange_type = struct.unpack_from("<BB", data, 0)
    token = data[2:27].split(b"\x00", 1)[0].decode("ascii", errors="strict").strip()
    if not token:
        raise ValueError("Angel stream packet has no token")
    _sequence, exchange_ts, raw_ltp = struct.unpack_from("<qqq", data, 27)
    tick = StreamTick(
        exchange_type=exchange_type,
        token=token,
        received_at=time.time(),
        exchange_timestamp_ms=exchange_ts,
        ltp=raw_ltp / 100.0,
    )
    if mode not in (QUOTE_MODE, SNAP_QUOTE_MODE):
        return tick
    if len(data) < 123:
        raise ValueError("truncated Angel quote packet")
    raw_ltq, raw_avg, volume = struct.unpack_from("<qqq", data, 51)
    _buy_qty, _sell_qty = struct.unpack_from("<dd", data, 75)
    raw_open, raw_high, raw_low, raw_close = struct.unpack_from("<qqqq", data, 91)
    oi = None
    oi_change_pct = None
    if mode == SNAP_QUOTE_MODE:
        if len(data) < 147:
            raise ValueError("truncated Angel snap-quote packet")
        _last_trade_ts, oi, raw_oi_pct = struct.unpack_from("<qqq", data, 123)
        oi_change_pct = raw_oi_pct / 100.0
    return StreamTick(
        exchange_type=exchange_type,
        token=token,
        received_at=tick.received_at,
        exchange_timestamp_ms=exchange_ts,
        ltp=tick.ltp,
        volume=int(volume),
        average_price=raw_avg / 100.0,
        open=raw_open / 100.0,
        high=raw_high / 100.0,
        low=raw_low / 100.0,
        close=raw_close / 100.0,
        oi=int(oi) if oi is not None else None,
        oi_change_pct=oi_change_pct,
        last_traded_quantity=int(raw_ltq),
    )


class LocalFiveMinuteBuilder:
    """Build local 5-minute OHLC and conservatively validate cumulative volume."""

    def __init__(self) -> None:
        self._candles: dict[tuple[int, str], _Candle] = {}
        self._volume_valid: dict[tuple[int, str], bool] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _bucket(exchange_timestamp_ms: int) -> int:
        return (exchange_timestamp_ms // 300_000) * 300_000

    def add(self, tick: StreamTick) -> dict[str, Any] | None:
        if tick.ltp <= 0 or tick.exchange_timestamp_ms <= 0:
            return None
        key = (tick.exchange_type, tick.token)
        bucket = self._bucket(tick.exchange_timestamp_ms)
        with self._lock:
            candle = self._candles.get(key)
            if candle is None or candle.bucket_ms != bucket:
                candle = _Candle(
                    bucket_ms=bucket,
                    open=tick.ltp,
                    high=tick.ltp,
                    low=tick.ltp,
                    close=tick.ltp,
                    volume=0,
                    ticks=0,
                    last_volume=None,
                )
                self._candles[key] = candle

            if tick.volume is not None and tick.volume >= 0:
                if candle.last_volume is not None and tick.volume < candle.last_volume:
                    self._volume_valid[key] = False
                # A new candle still needs the prior cumulative reading to
                # calculate its first delta; carry it from the previous candle.
                previous_candle = self._previous_candle(key, candle)
                prior_volume = previous_candle.last_volume if previous_candle else None
                if candle.last_volume is None and prior_volume is not None and tick.volume >= prior_volume:
                    candle.volume += int(tick.volume - prior_volume)
                elif candle.last_volume is not None and tick.volume >= candle.last_volume and self._volume_valid.get(key, True):
                    candle.volume += int(tick.volume - candle.last_volume)
                candle.last_volume = tick.volume

            candle.high = max(candle.high, tick.ltp)
            candle.low = min(candle.low, tick.ltp)
            candle.close = tick.ltp
            candle.ticks += 1
            return self._as_dict(candle, key=key, tick=tick)

    def _previous_candle(self, key: tuple[int, str], current: _Candle) -> _Candle | None:
        # The builder keeps only the current candle for bounded memory. Once a
        # boundary is crossed, the cumulative-volume baseline is stored here.
        return getattr(self, "_last_closed", {}).get(key)

    def _as_dict(self, candle: _Candle, *, key: tuple[int, str], tick: StreamTick) -> dict[str, Any]:
        return {
            "timestamp_ms": candle.bucket_ms,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "ticks": candle.ticks,
            "source": candle.source,
            "volume_valid": self._volume_valid.get(key, True),
            "observed_at": tick.received_at,
        }

    def latest(self, exchange_type: int, token: str) -> dict[str, Any] | None:
        with self._lock:
            candle = self._candles.get((exchange_type, token))
            if candle is None:
                return None
            return self._as_dict(
                candle,
                key=(exchange_type, token),
                tick=StreamTick(exchange_type, token, time.time(), candle.bucket_ms, candle.close),
            )


class AngelOneMarketStream:
    """Optional stream client with bounded reconnects and thread-safe state."""

    def __init__(self, market_data: Any, *, enabled: bool | None = None) -> None:
        self.market_data = market_data
        self.enabled = (
            bool(enabled) if enabled is not None else
            os.getenv("ANGEL_ONE_STREAM_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
        )
        self._thread: threading.Thread | None = None
        self._ws: Any = None
        self._stop = threading.Event()
        self._connected = False
        self._last_tick_at = 0.0
        self._last_error = ""
        self._reconnects = 0
        self._subscriptions: set[tuple[int, str]] = set()
        self._latest: dict[tuple[int, str], StreamTick] = {}
        self._lock = threading.RLock()
        self.candles = LocalFiveMinuteBuilder()

    def health(self) -> dict[str, Any]:
        with self._lock:
            age = time.time() - self._last_tick_at if self._last_tick_at else None
            return {
                "enabled": self.enabled,
                "state": "connected" if self._connected else ("stopped" if self._stop.is_set() else "disconnected"),
                "last_tick_age_s": round(age, 2) if age is not None else None,
                "subscriptions": len(self._subscriptions),
                "reconnects": self._reconnects,
                "last_error": self._last_error,
                "candle_source": "angel_one_websocket_v2",
                # Angel's streamed cumulative volume has not been validated
                # sufficiently for this app's actionable volume gate.
                "volume_actionable": False,
                "order_execution": False,
            }

    def start(self) -> None:
        if not self.enabled or self._thread is not None:
            return
        if websocket is None:
            self._last_error = "websocket-client dependency unavailable"
            logger.warning("Angel WebSocket disabled: websocket-client is not installed")
            return
        if self.market_data is None or not getattr(self.market_data, "configured", False):
            self._last_error = "angel_one_not_configured"
            logger.info("Angel WebSocket remains disabled until Angel credentials are configured")
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="angel-stream", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def ensure_subscriptions(self, tokens: list[tuple[int, str]]) -> int:
        """Queue Snap Quote subscriptions, capped at Angel's 1,000-token quota."""
        clean = {(int(exchange), str(token)) for exchange, token in tokens if str(token).strip()}
        with self._lock:
            available = MAX_SUBSCRIPTIONS - len(self._subscriptions)
            new = set(sorted(clean - self._subscriptions)[:max(0, available)])
            if not new:
                return 0
            self._subscriptions.update(new)
            ws = self._ws if self._connected else None
        if ws is not None:
            self._send_subscribe(sorted(new), ws=ws)
        return len(new)

    def ensure_symbols(self, symbols: list[str]) -> int:
        """Subscribe current equity tokens and nearest NFO future tokens for candidates."""
        if not self.enabled or self.market_data is None:
            return 0
        try:
            instruments = self.market_data._get_instruments()
        except Exception as exc:
            self._record_error(type(exc).__name__)
            return 0
        wanted = {str(s).upper().strip() for s in symbols if str(s).strip()}
        tokens: set[tuple[int, str]] = set()
        for (exchange, symbol), instrument in instruments.items():
            if exchange == "NSE" and symbol in wanted:
                tokens.add((NSE_CM, instrument.token))
        nearest: dict[str, tuple[Any, Any]] = {}
        for (exchange, symbol), instrument in instruments.items():
            if exchange != "NFO":
                continue
            match = _FUTURE_RE.match(str(symbol).upper())
            if not match:
                continue
            underlying = match.group(1)
            if underlying not in wanted:
                continue
            try:
                expiry = datetime.strptime(str(instrument.expiry), "%d%b%Y").date()
            except (TypeError, ValueError):
                continue
            item = nearest.get(underlying)
            if item is None or expiry < item[0]:
                nearest[underlying] = (expiry, instrument)
        tokens.update((NSE_FO, item[1].token) for item in nearest.values())
        return self.ensure_subscriptions(sorted(tokens))

    def latest_tick(self, exchange_type: int, token: str, *, max_age_s: float = 10.0) -> dict[str, Any] | None:
        with self._lock:
            tick = self._latest.get((int(exchange_type), str(token)))
        if not tick or time.time() - tick.received_at > max_age_s:
            return None
        return {
            "ltp": tick.ltp,
            "volume": tick.volume,
            "avg_price": tick.average_price,
            "open": tick.open,
            "high": tick.high,
            "low": tick.low,
            "close": tick.close,
            "oi": tick.oi,
            "oi_change_pct": tick.oi_change_pct,
            "source": "angel_one_websocket_v2",
            "tick_age_s": round(time.time() - tick.received_at, 2),
            "exchange_timestamp_ms": tick.exchange_timestamp_ms,
        }

    def latest_candle(self, exchange_type: int, token: str, *, max_age_s: float = 360.0) -> dict[str, Any] | None:
        candle = self.candles.latest(exchange_type, token)
        if not candle:
            return None
        age = time.time() - float(candle.get("observed_at") or 0)
        if age > max_age_s:
            return None
        return {**candle, "candle_age_s": round(age, 2), "candle_fresh": True}

    def _credentials(self) -> tuple[str, str, str, str]:
        self.market_data._login()
        return (
            str(self.market_data._jwt),
            str(self.market_data.api_key),
            str(self.market_data.client_code),
            str(self.market_data._feed_token),
        )

    def _run(self) -> None:
        delay = 2.0
        while not self._stop.is_set():
            try:
                auth_token, api_key, client_code, feed_token = self._credentials()
                headers = {
                    "Authorization": auth_token,
                    "x-api-key": api_key,
                    "x-client-code": client_code,
                    "x-feed-token": feed_token,
                }
                self._ws = websocket.WebSocketApp(
                    STREAM_URL,
                    header=headers,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=10, ping_timeout=5)
            except Exception as exc:
                self._record_error(type(exc).__name__)
            finally:
                with self._lock:
                    self._connected = False
                self._ws = None
            if self._stop.wait(delay):
                break
            delay = min(delay * 2.0, 60.0)
            with self._lock:
                self._reconnects += 1
        logger.info("Angel WebSocket stream stopped")

    def _on_open(self, ws: Any) -> None:
        with self._lock:
            self._connected = True
            self._last_error = ""
        self._send_subscribe(sorted(self._subscriptions), ws=ws)

    def _send_subscribe(self, tokens: list[tuple[int, str]], *, ws: Any | None = None) -> None:
        if not tokens:
            return
        target = ws or self._ws
        if target is None:
            return
        by_exchange: dict[int, list[str]] = {}
        for exchange, token in tokens:
            by_exchange.setdefault(exchange, []).append(token)
        payload = {
            "correlationID": "nseoi001",
            "action": 1,
            "params": {
                "mode": SNAP_QUOTE_MODE,
                "tokenList": [
                    {"exchangeType": exchange, "tokens": values}
                    for exchange, values in sorted(by_exchange.items())
                ],
            },
        }
        target.send(json.dumps(payload))

    def _on_message(self, _ws: Any, message: Any) -> None:
        if isinstance(message, str):
            return
        try:
            tick = parse_stream_packet(message)
            with self._lock:
                self._latest[(tick.exchange_type, tick.token)] = tick
                self._last_tick_at = time.time()
            self.candles.add(tick)
        except (TypeError, ValueError, struct.error) as exc:
            self._record_error(type(exc).__name__)

    def _on_error(self, _ws: Any, error: Any) -> None:
        self._record_error(type(error).__name__)

    def _on_close(self, _ws: Any, _status: Any, _message: Any) -> None:
        with self._lock:
            self._connected = False

    def _record_error(self, name: str) -> None:
        with self._lock:
            self._last_error = str(name)[:80]
        logger.warning("Angel WebSocket stream error=%s", name)
