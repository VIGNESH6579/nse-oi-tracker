"""Read-only Angel One SmartAPI market-data integration.

This module intentionally contains authentication, instrument lookup, quotes,
and candles only. No order, GTT, portfolio-mutation, modify, or cancel API is
implemented or imported.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from threading import RLock
from urllib.parse import quote

import pyotp
from curl_cffi import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://apiconnect.angelone.in"
INSTRUMENT_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"


@dataclass(frozen=True, slots=True)
class AngelInstrument:
    symbol: str
    token: str
    exchange: str
    expiry: str | None = None
    instrument_type: str | None = None


class AngelOneMarketData:
    """Small, thread-safe, read-only SmartAPI client."""

    def __init__(self, *, api_key: str, client_code: str, password: str, totp_secret: str,
                 timeout: float = 15.0) -> None:
        self.api_key = api_key
        self.client_code = client_code
        self.password = password
        self.totp_secret = totp_secret
        self.timeout = timeout
        self._jwt: str | None = None
        self._feed_token: str | None = None
        self._login_at = 0.0
        self._instruments: dict[tuple[str, str], AngelInstrument] = {}
        self._last_quote_at = 0.0
        self._lock = RLock()

    @staticmethod
    def _lookup_symbol(symbol: str, *, exchange: str = "NSE") -> str:
        """Normalize dashboard symbols to instrument-master lookup symbols."""
        normalized = symbol.upper().strip()
        if exchange.upper() == "NSE":
            normalized = normalized.removesuffix("-EQ")
        return normalized

    @classmethod
    def from_environment(cls) -> "AngelOneMarketData | None":
        # Accept the names already used by the Render service as well as the
        # documented ANGEL_ONE_* names. This is read-only market data; no
        # order-placement API exists in this integration.
        values = {
            "api_key": (os.getenv("ANGEL_ONE_API_KEY") or os.getenv("ANGEL_API_KEY") or "").strip(),
            "client_code": (os.getenv("ANGEL_ONE_CLIENT_CODE") or os.getenv("ANGEL_CLIENT_ID") or os.getenv("ANGEL_CLIENT_CODE") or "").strip(),
            "password": (os.getenv("ANGEL_ONE_PASSWORD") or os.getenv("ANGEL_PASSWORD") or os.getenv("ANGEL_PIN") or "").strip(),
            "totp_secret": (os.getenv("ANGEL_ONE_TOTP_SECRET") or os.getenv("ANGEL_TOTP_SECRET") or "").strip(),
        }
        if not all(values.values()):
            return None
        return cls(**values)

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.client_code and self.password and self.totp_secret)

    def _headers(self, *, authenticated: bool = True) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-PrivateKey": self.api_key,
            "X-UserType": "USER",
            "X-SourceID": "WEB",
        }
        if authenticated and self._jwt:
            headers["Authorization"] = f"Bearer {self._jwt}"
        return headers

    def _login(self) -> None:
        with self._lock:
            if self._jwt and time.time() - self._login_at < 8 * 60 * 60:
                return
            response = requests.post(
                f"{BASE_URL}/rest/auth/angelbroking/user/v1/loginByPassword",
                headers=self._headers(authenticated=False),
                json={
                    "clientcode": self.client_code,
                    "password": self.password,
                    "totp": pyotp.TOTP(self.totp_secret).now(),
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            body = response.json()
            if not body.get("status") or not body.get("data"):
                raise RuntimeError(f"Angel One login failed: {body.get('message', 'unknown error')}")
            self._jwt = str(body["data"].get("jwtToken") or "")
            self._feed_token = str(body["data"].get("feedToken") or "")
            if not self._jwt:
                raise RuntimeError("Angel One login response did not include jwtToken")
            self._login_at = time.time()

    def _get_instruments(self) -> dict[tuple[str, str], AngelInstrument]:
        with self._lock:
            if self._instruments:
                return self._instruments
            response = requests.get(INSTRUMENT_MASTER_URL, timeout=30)
            response.raise_for_status()
            records = response.json()
            result: dict[tuple[str, str], AngelInstrument] = {}
            for row in records:
                exchange = str(row.get("exch_seg") or "").upper()
                raw_symbol = str(row.get("symbol") or "").upper()
                symbol = self._lookup_symbol(raw_symbol, exchange=exchange)
                token = str(row.get("token") or "")
                if exchange and symbol and token:
                    result.setdefault((exchange, symbol), AngelInstrument(
                        symbol=symbol,
                        token=token,
                        exchange=exchange,
                        expiry=row.get("expiry"),
                        instrument_type=row.get("instrumenttype"),
                    ))
            self._instruments = result
            return result

    def instrument(self, symbol: str, *, exchange: str = "NSE") -> AngelInstrument | None:
        symbol = self._lookup_symbol(symbol, exchange=exchange)
        if exchange.upper() == "NSE":
            symbol = {
                "NIFTY": "NIFTY 50",
                "BANKNIFTY": "NIFTY BANK",
                "FINNIFTY": "NIFTY FIN SERVICE",
                "MIDCPNIFTY": "NIFTY MID SELECT",
            }.get(symbol, symbol)
        instruments = self._get_instruments()
        return instruments.get((exchange.upper(), symbol))

    def full_quotes(self, symbols: list[str], *, exchange: str = "NSE") -> dict[str, dict]:
        """Return read-only full quotes in batches of 50."""
        if not symbols:
            return {}
        self._login()
        instruments = self._get_instruments()
        normalized_exchange = exchange.upper()
        normalized_symbols = [self._lookup_symbol(symbol, exchange=normalized_exchange) for symbol in symbols]
        tokens = [instruments[(normalized_exchange, symbol)].token
                  for symbol in normalized_symbols
                  if (normalized_exchange, symbol) in instruments]
        output: dict[str, dict] = {}
        for offset in range(0, len(tokens), 50):
            batch = tokens[offset:offset + 50]
            # Angel One documents a maximum of 50 symbols and 1 quote request
            # per second. Sleep only between batches so a single-batch scan is
            # not delayed.
            elapsed = time.monotonic() - self._last_quote_at
            if elapsed < 1.0 and self._last_quote_at:
                time.sleep(1.0 - elapsed)
            response = requests.post(
                f"{BASE_URL}/rest/secure/angelbroking/market/v1/quote/",
                headers=self._headers(),
                json={"mode": "FULL", "exchangeTokens": {exchange.upper(): batch}},
                timeout=self.timeout,
            )
            response.raise_for_status()
            body = response.json()
            if not body.get("status"):
                raise RuntimeError(f"Angel One quote request failed: {body.get('message', 'unknown error')}")
            self._last_quote_at = time.monotonic()
            for row in (body.get("data") or {}).get("fetched", []) or []:
                raw_symbol = str(row.get("tradingSymbol") or row.get("symbol") or "").upper()
                symbol = raw_symbol.removesuffix("-EQ")
                if symbol:
                    output[symbol] = {
                        "ltp": float(row.get("ltp") or 0),
                        "open": float(row.get("open") or 0),
                        "high": float(row.get("high") or 0),
                        "low": float(row.get("low") or 0),
                        "close": float(row.get("close") or 0),
                        "volume": int(float(row.get("tradeVolume") or row.get("volume") or 0)),
                        "oi": int(float(row.get("opnInterest") or row.get("openInterest") or 0)),
                        "change_pct": float(row.get("percentChange") or 0),
                        "source": "angel_one_read_only",
                    }
        return output

    def fno_quotes(self, symbols: list[str] | None = None) -> dict[str, dict]:
        """Return nearest-expiry NFO futures quotes keyed by underlying symbol."""
        self._login()
        instruments = self._get_instruments()
        wanted = {str(symbol).upper().strip() for symbol in symbols or () if str(symbol).strip()}
        today = date.today()
        selected: dict[str, AngelInstrument] = {}
        for (exchange, _raw_symbol), instrument in instruments.items():
            if exchange != "NFO" or not str(instrument.symbol).upper().endswith("FUT"):
                continue
            name = str(instrument.symbol).upper()
            match = re.match(r"^([A-Z&]+)\d{2}[A-Z]{3}\d{2}FUT$", name)
            if not match:
                continue
            underlying = match.group(1)
            if wanted and underlying not in wanted:
                continue
            try:
                expiry = datetime.strptime(str(instrument.expiry), "%d%b%Y").date()
            except (TypeError, ValueError):
                continue
            if expiry < today:
                continue
            previous = selected.get(underlying)
            if previous is None or str(instrument.expiry) < str(previous.expiry):
                selected[underlying] = instrument
        if not selected:
            return {}
        raw = self.full_quotes([item.symbol for item in selected.values()], exchange="NFO")
        if not hasattr(self, "_fno_previous_oi"):
            self._fno_previous_oi: dict[str, float] = {}
        result: dict[str, dict] = {}
        for underlying, instrument in selected.items():
            quote = raw.get(instrument.symbol)
            if not quote:
                continue
            current_oi = float(quote.get("oi") or 0)
            previous_oi = self._fno_previous_oi.get(underlying, current_oi)
            self._fno_previous_oi[underlying] = current_oi
            oi_change = current_oi - previous_oi
            result[underlying] = {
                **quote,
                "oi_change": oi_change,
                "oi_change_pct": (oi_change / previous_oi * 100) if previous_oi else 0.0,
                "source": "angel_one_nfo_futures_fallback",
            }
        return result

    def intraday_candles(self, symbol: str, *, interval: str = "FIVE_MINUTE",
                         exchange: str = "NSE", days: int = 1) -> list[dict]:
        """Fetch read-only candles for a mapped instrument."""
        self._login()
        instrument = self.instrument(symbol, exchange=exchange)
        if not instrument:
            return []
        end = datetime.now().replace(second=0, microsecond=0)
        start = end - timedelta(days=max(1, days))
        response = requests.post(
            f"{BASE_URL}/rest/secure/angelbroking/historical/v1/getCandleData",
            headers=self._headers(),
            json={
                "exchange": exchange.upper(),
                "symboltoken": instrument.token,
                "interval": interval,
                "fromdate": start.strftime("%Y-%m-%d %H:%M"),
                "todate": end.strftime("%Y-%m-%d %H:%M"),
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        body = response.json()
        if not body.get("status"):
            raise RuntimeError(f"Angel One candle request failed: {body.get('message', 'unknown error')}")
        candles = []
        for row in body.get("data") or []:
            if len(row) < 6:
                continue
            candles.append({
                "time": row[0], "open": float(row[1]), "high": float(row[2]),
                "low": float(row[3]), "close": float(row[4]), "volume": float(row[5]),
                "source": "angel_one_read_only",
            })
        return candles

    def daily_candles(self, symbol: str, *, days: int = 90, exchange: str = "NSE") -> list[dict]:
        """Fetch bounded daily OHLCV candles for an equity or supported index."""
        return self.intraday_candles(symbol, interval="ONE_DAY", exchange=exchange, days=max(1, days))


__all__ = ["AngelInstrument", "AngelOneMarketData"]
