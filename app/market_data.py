"""Read-only Angel One market quotes for the company-news monitor.

Only the SmartAPI login and market quote endpoints are used. No order, GTT,
modify, cancel, position, or holdings endpoints are present in this module.
The dashboard retains only price/volume fields; OI is deliberately discarded.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from threading import RLock

import pyotp
import requests

log = logging.getLogger(__name__)
BASE = "https://apiconnect.angelone.in/rest"
MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

class AngelMarketData:
    def __init__(self):
        self.api_key = os.getenv("ANGEL_ONE_API_KEY") or os.getenv("ANGEL_API_KEY", "")
        self.client_code = os.getenv("ANGEL_ONE_CLIENT_CODE") or os.getenv("ANGEL_CLIENT_CODE", "")
        self.password = os.getenv("ANGEL_ONE_PASSWORD") or os.getenv("ANGEL_PASSWORD") or os.getenv("ANGEL_PIN", "")
        self.totp_secret = (os.getenv("ANGEL_ONE_TOTP_SECRET") or os.getenv("ANGEL_TOTP_SECRET", "")).replace(" ", "")
        self._jwt = ""
        self._login_at = 0.0
        self._tokens = {}
        self._token_master_loaded = False
        self._lock = RLock()
        self._last_quote_at = 0.0
        self.last_missing_symbols = []
        self.last_unquoted_symbols = []
        try:
            configured = json.loads(os.getenv("ANGEL_SYMBOL_TOKENS", "{}"))
            if isinstance(configured, dict):
                self._tokens = {str(k).upper().removesuffix("-EQ"): str(v) for k, v in configured.items() if v}
        except (ValueError, TypeError):
            log.warning("ANGEL_SYMBOL_TOKENS is not valid JSON; will use instrument master")

    @property
    def configured(self):
        return bool(self.api_key and self.client_code and self.password and self.totp_secret)

    def _headers(self, authenticated=True):
        h = {
            "Content-Type": "application/json", "Accept": "application/json",
            "X-PrivateKey": self.api_key, "X-UserType": "USER", "X-SourceID": "WEB",
            "X-ClientLocalIP": os.getenv("ANGEL_ONE_CLIENT_LOCAL_IP", "127.0.0.1"),
            "X-ClientPublicIP": os.getenv("ANGEL_ONE_CLIENT_PUBLIC_IP", "127.0.0.1"),
            "X-MACAddress": os.getenv("ANGEL_ONE_MAC", "00:00:00:00:00:00"),
        }
        if authenticated and self._jwt:
            h["Authorization"] = "Bearer " + self._jwt
        return h

    def _login(self):
        if not self.configured:
            raise RuntimeError("Angel One credentials are not configured")
        with self._lock:
            if self._jwt and time.time() - self._login_at < 7 * 3600:
                return
            response = requests.post(
                BASE + "/auth/angelbroking/user/v1/loginByPassword",
                headers=self._headers(False),
                json={"clientcode": self.client_code, "password": self.password,
                      "totp": pyotp.TOTP(self.totp_secret).now()},
                timeout=15,
            )
            body = response.json()
            data = body.get("data") or {}
            token = data.get("jwtToken")
            if response.status_code != 200 or not body.get("status") or not token:
                # Never include credential values or request headers in logs.
                raise RuntimeError("Angel One read-only market-data login failed: " + str(body.get("message", "unknown response"))[:120])
            self._jwt = str(token)
            self._login_at = time.time()

    def _load_tokens(self, symbols):
        wanted = {str(s).upper().strip() for s in symbols}
        missing = wanted - set(self._tokens)
        if not missing or self._token_master_loaded:
            return
        response = requests.get(MASTER_URL, timeout=45)
        response.raise_for_status()
        # The Angel instrument master is a public JSON array. Keep only requested
        # NSE cash-equity tokens and the NIFTY 50 index token, not derivatives.
        rows = response.json()
        found = {}
        for row in rows:
            if str(row.get("exch_seg", "")).upper() != "NSE":
                continue
            raw = str(row.get("symbol", "")).upper()
            symbol = raw.removesuffix("-EQ")
            name = str(row.get("name", "")).upper()
            token = str(row.get("token", ""))
            if raw.endswith("-EQ") and token:
                # Cache every NSE cash-equity token, not just symbols requested
                # during the first poll. New event-linked stocks then resolve too.
                found[symbol] = token
            elif (name == "NIFTY 50" or raw == "NIFTY 50") and token:
                found["NIFTY"] = token
        # Explicit ANGEL_SYMBOL_TOKENS values take precedence over the master file.
        for symbol, token in found.items():
            self._tokens.setdefault(symbol, token)
        self._token_master_loaded = True

    def quotes(self, symbols):
        """Fetch price/volume snapshots; intentionally discard OI and depth."""
        if not symbols:
            self.last_missing_symbols = []
            self.last_unquoted_symbols = []
            return {}
        requested_symbols = {str(s).upper().strip() for s in symbols if str(s).strip()}
        self._login()
        self._load_tokens(list(requested_symbols) + ["NIFTY"])
        self.last_missing_symbols = sorted(
            symbol for symbol in requested_symbols if not self._tokens.get(symbol)
        )
        wanted = [symbol for symbol in sorted(requested_symbols) if self._tokens.get(symbol)]
        # Always request NIFTY 50 as the market benchmark.
        if self._tokens.get("NIFTY") and "NIFTY" not in wanted:
            wanted.append("NIFTY")
        token_to_symbol = {self._tokens[s]: s for s in wanted}
        tokens = list(token_to_symbol)
        out = {}
        for offset in range(0, len(tokens), 50):
            if self._last_quote_at:
                delay = 1.05 - (time.monotonic() - self._last_quote_at)
                if delay > 0:
                    time.sleep(delay)
            batch = tokens[offset:offset+50]
            response = requests.post(
                BASE + "/secure/angelbroking/market/v1/quote/",
                headers=self._headers(),
                json={"mode": "FULL", "exchangeTokens": {"NSE": batch}},
                timeout=15,
            )
            response.raise_for_status()
            body = response.json()
            if not body.get("status"):
                raise RuntimeError("Angel One quote request failed: " + str(body.get("message", "unknown response"))[:120])
            fetched_at = datetime.now(timezone.utc).isoformat()
            self._last_quote_at = time.monotonic()
            for row in (body.get("data") or {}).get("fetched", []) or []:
                token = str(row.get("symbolToken") or row.get("symboltoken") or "")
                symbol = token_to_symbol.get(token)
                if not symbol:
                    continue
                ltp = float(row.get("ltp") or 0)
                if ltp <= 0:
                    continue
                out[symbol] = {
                    "symbol": symbol, "ltp": ltp,
                    "change_pct": float(row.get("percentChange") or 0),
                    "open": float(row.get("open") or 0),
                    "high": float(row.get("high") or 0),
                    "low": float(row.get("low") or 0),
                    "previous_close": float(row.get("close") or 0),
                    "volume": int(float(row.get("tradeVolume") or row.get("volume") or 0)),
                    "source": "Angel One SmartAPI",
                    "fetched_at": fetched_at,
                }
        self.last_unquoted_symbols = sorted(requested_symbols - set(out))
        return out

angel_market_data = AngelMarketData()
