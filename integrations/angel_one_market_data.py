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
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from threading import RLock
from urllib.parse import quote

from utils.jsonstream import iter_json_array
from utils.time import now_ist

import pyotp
from curl_cffi import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://apiconnect.angelone.in"
INSTRUMENT_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"


class AngelLoginError(RuntimeError):
    """Safe, structured login failure that never contains credentials."""

    def __init__(self, http_status: int, errorcode: str = "", message: str = "") -> None:
        self.http_status = int(http_status)
        self.errorcode = str(errorcode or "")
        self.message = str(message or "")[:240]
        super().__init__(f"Angel login failed ({self.http_status}, {self.errorcode}): {self.message}")


class AngelUnavailable(RuntimeError):
    """Raised while the login circuit breaker is open."""


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
        self.totp_secret = str(totp_secret).strip().replace(" ", "").upper()
        self.timeout = timeout
        self._jwt: str | None = None
        self._feed_token: str | None = None
        self._login_at = 0.0
        self._instruments: dict[tuple[str, str], AngelInstrument] = {}
        self._last_quote_at = 0.0
        self._breaker_until = 0.0
        self._hist_lock = threading.Lock()
        self._last_hist_at = 0.0
        self._hist_block_until = 0.0
        self._breaker_seconds = 15 * 60
        self._last_error_code = ""
        self._lock = RLock()

    _INDEX_ALIASES = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK", "FINNIFTY": "NIFTY FIN SERVICE",
                      "MIDCPNIFTY": "NIFTY MID SELECT", "VIX": "INDIA VIX", "INDIAVIX": "INDIA VIX"}

    @classmethod
    def _lookup_symbol(cls, symbol: str, *, exchange: str = "NSE") -> str:
        """Normalize dashboard symbols to instrument-master lookup symbols."""
        normalized = symbol.upper().strip()
        if exchange.upper() == "NSE":
            normalized = normalized.removesuffix("-EQ")
            normalized = cls._INDEX_ALIASES.get(normalized, normalized)
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
            "totp_secret": (os.getenv("ANGEL_ONE_TOTP_SECRET") or os.getenv("ANGEL_TOTP_SECRET") or "").strip().replace(" ", "").upper(),
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
            "X-ClientLocalIP": os.getenv("ANGEL_ONE_CLIENT_LOCAL_IP", "127.0.0.1"),
            "X-ClientPublicIP": os.getenv("ANGEL_ONE_CLIENT_PUBLIC_IP", "127.0.0.1"),
            "X-MACAddress": os.getenv("ANGEL_ONE_MAC", "00:00:00:00:00:00"),
        }
        if authenticated and self._jwt:
            headers["Authorization"] = f"Bearer {self._jwt}"
        return headers

    def _login(self) -> None:
        with self._lock:
            if time.time() < self._breaker_until:
                raise AngelUnavailable("Angel login circuit breaker is open")
            if self._jwt and time.time() - self._login_at < 8 * 60 * 60:
                return
            try:
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
                try:
                    body = response.json()
                except Exception:
                    body = {}
                if int(getattr(response, "status_code", 200)) != 200 or not body.get("status") or not body.get("data"):
                    error = AngelLoginError(getattr(response, "status_code", 0), body.get("errorcode"), body.get("message"))
                    self._last_error_code = error.errorcode
                    self._breaker_until = time.time() + self._breaker_seconds
                    self._breaker_seconds = min(self._breaker_seconds * 2, 60 * 60)
                    logger.warning("Angel login rejected: http_status=%s errorcode=%s message=%s", error.http_status, error.errorcode, error.message)
                    raise error
                self._jwt = str(body["data"].get("jwtToken") or "")
                self._feed_token = str(body["data"].get("feedToken") or "")
                if not self._jwt:
                    raise AngelLoginError(getattr(response, "status_code", 200), body.get("errorcode"), "missing jwtToken")
                self._login_at = time.time()
                self._breaker_until = 0.0
                self._breaker_seconds = 15 * 60
                self._last_error_code = ""
            except AngelLoginError:
                raise
            except Exception as exc:
                self._last_error_code = type(exc).__name__
                self._breaker_until = time.time() + self._breaker_seconds
                self._breaker_seconds = min(self._breaker_seconds * 2, 60 * 60)
                logger.warning("Angel login failed: %s", type(exc).__name__)
                raise AngelUnavailable("Angel login unavailable") from exc

    def health(self) -> dict[str, object]:
        retry_at = self._breaker_until if self._breaker_until > time.time() else None
        return {"state": "breaker_open" if retry_at else ("authenticated" if self._jwt else "not_authenticated"),
                "last_error_code": self._last_error_code, "retry_at": retry_at}

    _INDEX_NAMES = frozenset({"NIFTY 50", "NIFTY BANK", "NIFTY FIN SERVICE", "NIFTY MID SELECT", "NIFTY NEXT 50", "INDIA VIX"})

    def _get_instruments(self) -> dict[tuple[str, str], AngelInstrument]:
        """Load only the rows this app uses (NSE equities/indices, NFO stock/index futures).

        The full master has >100k rows (mostly options). It is streamed and
        filtered row by row so peak memory stays small on a 512 MB instance.
        """
        with self._lock:
            if self._instruments:
                return self._instruments
            response = requests.get(INSTRUMENT_MASTER_URL, timeout=60)
            response.raise_for_status()
            text = response.text
            del response
            result: dict[tuple[str, str], AngelInstrument] = {}
            for row in iter_json_array(text):
                if not isinstance(row, dict):
                    continue
                exchange = str(row.get("exch_seg") or "").upper()
                raw_symbol = str(row.get("symbol") or "").upper()
                instrument_type = str(row.get("instrumenttype") or "").upper()
                if exchange == "NFO":
                    if instrument_type not in ("FUTSTK", "FUTIDX"):
                        continue
                elif exchange == "NSE":
                    if not (raw_symbol.endswith("-EQ") or instrument_type == "AMXIDX" or raw_symbol in self._INDEX_NAMES):
                        continue
                else:
                    continue
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
            del text
            self._instruments = result
            return result

    def instrument(self, symbol: str, *, exchange: str = "NSE") -> AngelInstrument | None:
        symbol = self._lookup_symbol(symbol, exchange=exchange)
        instruments = self._get_instruments()
        return instruments.get((exchange.upper(), symbol))

    def full_quotes(self, symbols: list[str], *, exchange: str = "NSE") -> dict[str, dict]:
        """Return read-only full quotes in batches of 50."""
        if not symbols:
            return {}
        self._login()
        instruments = self._get_instruments()
        normalized_exchange = exchange.upper()
        # Map each ORIGINAL requested symbol (e.g. "NIFTY") to Angel's token, and remember which
        # original key it was: Angel's response is keyed by its own tradingSymbol ("NIFTY 50"),
        # which would never match a caller looking up quotes.get("NIFTY").
        token_to_original: dict[str, str] = {}
        for original in symbols:
            normalized = self._lookup_symbol(original, exchange=normalized_exchange)
            instrument = instruments.get((normalized_exchange, normalized))
            if instrument:
                token_to_original.setdefault(instrument.token, original.upper().strip())
        tokens = list(token_to_original)
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
                token = str(row.get("symbolToken") or row.get("symboltoken") or "")
                symbol = token_to_original.get(token)
                if not symbol:
                    raw_symbol = str(row.get("tradingSymbol") or row.get("symbol") or "").upper()
                    symbol = raw_symbol.removesuffix("-EQ")
                if symbol:
                    ltp = float(row.get("ltp") or 0)
                    open_price = float(row.get("open") or 0)
                    high = float(row.get("high") or 0)
                    low = float(row.get("low") or 0)
                    close = float(row.get("close") or 0)
                    if (min(ltp, open_price, high, low, close) <= 0
                            or high < max(ltp, open_price, low, close)
                            or low > min(ltp, open_price, high, close)):
                        logger.warning("Ignoring malformed Angel quote for %s: invalid OHLC range", symbol)
                        continue
                    output[symbol] = {
                        "ltp": ltp,
                        "open": open_price,
                        "high": high,
                        "low": low,
                        "close": close,
                        "volume": int(float(row.get("tradeVolume") or row.get("volume") or 0)),
                        "oi": int(float(row.get("opnInterest") or row.get("openInterest") or 0)),
                        "change_pct": float(row.get("percentChange") or 0),
                        "avg_price": float(row.get("avgPrice") or 0),      # exchange VWAP for the session
                        "source": "angel_one_read_only",
                    }
        return output

    _FUT_RE = re.compile(r"^(.+?)\d{2}[A-Z]{3}\d{2}FUT$")

    def fno_quotes(self, symbols: list[str] | None = None, *, today: date | None = None) -> dict[str, dict]:
        """Nearest-expiry NFO futures quotes keyed by underlying.

        * Expiry is chosen by parsed DATE (the old string comparison could pick
          next month: "28OCT2026" < "30SEP2026" as text).
        * Within ``ANGEL_ROLL_DAYS`` (default 5) of expiry the next-month OI is
          added so rollover does not look like OI unwinding.
        * ``oi_change`` is measured from the first OI seen today (session
          baseline), not from the previous 2-minute scan.
        """
        self._login()
        instruments = self._get_instruments()
        wanted = {str(symbol).upper().strip() for symbol in symbols or () if str(symbol).strip()}
        today = today or now_ist().date()
        roll_days = int(os.getenv("ANGEL_ROLL_DAYS", "5"))
        by_underlying: dict[str, list[tuple[date, AngelInstrument]]] = {}
        for (exchange, _raw_symbol), instrument in instruments.items():
            if exchange != "NFO" or not str(instrument.symbol).upper().endswith("FUT"):
                continue
            match = self._FUT_RE.match(str(instrument.symbol).upper())
            if not match:
                continue
            underlying = match.group(1)
            if "NSETEST" in underlying:
                continue
            if wanted and underlying not in wanted:
                continue
            try:
                expiry = datetime.strptime(str(instrument.expiry), "%d%b%Y").date()
            except (TypeError, ValueError):
                continue
            if expiry < today:
                continue
            by_underlying.setdefault(underlying, []).append((expiry, instrument))
        if not by_underlying:
            return {}
        plan: dict[str, tuple[AngelInstrument, AngelInstrument | None]] = {}
        for underlying, contracts in by_underlying.items():
            contracts.sort(key=lambda item: item[0])
            near_expiry, near = contracts[0]
            nxt = contracts[1][1] if len(contracts) > 1 and (near_expiry - today).days <= roll_days else None
            plan[underlying] = (near, nxt)
        wanted_symbols = [inst.symbol for near, nxt in plan.values() for inst in (near, nxt) if inst is not None]
        raw = self.full_quotes(wanted_symbols, exchange="NFO")
        if not hasattr(self, "_fno_open_oi"):
            self._fno_open_oi: dict[str, tuple[date, float]] = {}
        result: dict[str, dict] = {}
        for underlying, (near, nxt) in plan.items():
            quote = raw.get(near.symbol)
            if not quote:
                continue
            current_oi = float(quote.get("oi") or 0)
            if nxt is not None:
                current_oi += float((raw.get(nxt.symbol) or {}).get("oi") or 0)
            baseline = self._fno_open_oi.get(underlying)
            if baseline is None or baseline[0] != today:
                baseline = (today, current_oi)
                self._fno_open_oi[underlying] = baseline
            oi_change = current_oi - baseline[1]
            result[underlying] = {
                **quote,
                "oi": current_oi,
                "oi_change": oi_change,
                "oi_change_pct": (oi_change / baseline[1] * 100) if baseline[1] else 0.0,
                "oi_includes_next_month": nxt is not None,
                "source": "angel_one_nfo_futures_fallback",
            }
        return result

    def _hist_wait(self) -> None:
        """Serialise historical-candle calls (>= ANGEL_HIST_MIN_INTERVAL s apart) and honour a 403 cooldown."""
        with self._hist_lock:
            now = time.monotonic()
            if now < self._hist_block_until:
                raise AngelUnavailable(f"historical API cooling down for {self._hist_block_until - now:.0f}s after HTTP 403")
            wait = self._last_hist_at + float(os.getenv("ANGEL_HIST_MIN_INTERVAL", "1.1")) - now
            if wait > 0:
                time.sleep(wait)
            self._last_hist_at = time.monotonic()

    def intraday_candles(self, symbol: str, *, interval: str = "FIVE_MINUTE",
                         exchange: str = "NSE", days: int = 1) -> list[dict]:
        """Fetch read-only candles for a mapped instrument."""
        self._login()
        instrument = self.instrument(symbol, exchange=exchange)
        if not instrument:
            return []
        self._hist_wait()
        # Angel expects IST wall-clock times. The old naive datetime.now() used the
        # server's UTC clock, so intraday requests covered the wrong window (often
        # yesterday's session) and any "session VWAP" was built from stale candles.
        end = now_ist().replace(second=0, microsecond=0, tzinfo=None)
        if interval != "ONE_DAY" and days <= 1:
            start = end.replace(hour=9, minute=15)      # today's session only
            if start >= end:
                start = end - timedelta(minutes=10)
        else:
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
        if getattr(response, "status_code", 200) in (403, 429):
            # Angel answers 403 when the historical-data rate limit is exceeded: cool down
            # instead of hammering (logs showed ~55% of candle calls failing this way).
            self._hist_block_until = time.monotonic() + float(os.getenv("ANGEL_HIST_COOLDOWN_S", "45"))
            snippet = " ".join(str(getattr(response, "text", "") or "").split())[:120]
            raise RuntimeError(f"HTTP {response.status_code} from Angel candle API: {snippet or 'empty body'}")
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
