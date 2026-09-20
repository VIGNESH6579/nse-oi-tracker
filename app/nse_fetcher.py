# nse_fetcher.py ? Production NSE fetcher using curl-cffi Chrome impersonation
#
# WHY curl-cffi?
# NSE uses Akamai Bot Manager (+ Cloudflare) for bot protection.
# Both check the TLS fingerprint (JA3 hash) at the TCP level ? before
# any cookies or JS challenge. Standard requests/urllib3/cloudscraper
# produce a non-browser JA3 hash that Akamai instantly flags as a bot.
#
# curl-cffi uses Chrome's own BoringSSL library to produce an IDENTICAL
# TLS fingerprint to a real Chrome browser. Akamai cannot distinguish
# our requests from a real user. This fixes option chain 403/HTML blocks.
#
# Reference: https://github.com/yifeikong/curl-cffi

import time
import logging
import random
import os
import sqlite3
from pathlib import Path
from collections import OrderedDict
from threading import RLock
from urllib.parse import quote
from datetime import date, datetime
from curl_cffi import requests as cffi_requests
from app.config import SESSION_REFRESH_SECONDS
from integrations.angel_one_market_data import AngelOneMarketData
from utils.time import now_ist
logger   = logging.getLogger(__name__)
NSE_BASE = "https://www.nseindia.com"


def _proxy_kwargs() -> dict[str, str]:
    """Return curl-cffi proxy settings without logging the proxy credential."""
    proxy = os.getenv("NSE_OI_PROXY_URL", "").strip()
    if not proxy:
        return {}
    if proxy in {
        "http://user:password@india-residential-proxy:port",
        "socks5h://user:password@india-residential-proxy:port",
    }:
        logger.warning("Ignoring placeholder NSE_OI_PROXY_URL; configure a real proxy URL")
        return {}
    if not proxy.startswith(("http://", "https://", "socks5://", "socks5h://")):
        raise ValueError("NSE_OI_PROXY_URL must be an HTTP(S) or SOCKS5 URL")
    return {"http": proxy, "https": proxy}

# Chrome impersonation target (curl-cffi supports many versions)
CHROME = "chrome120"

# Base headers for all requests (clean, realistic Chrome headers)
BASE_HEADERS = {
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
}

# JSON API-specific headers (added on top of BASE_HEADERS for API calls)
API_EXTRA = {
    "Accept":           "application/json, text/plain, */*",
    "X-Requested-With": "XMLHttpRequest",
    "Sec-Fetch-Dest":   "empty",
    "Sec-Fetch-Mode":   "cors",
    "Sec-Fetch-Site":   "same-origin",
}

# Page navigation headers (for seed visits - natural browser navigation)
NAV_EXTRA = {
    "Accept":         "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

# Seed pages visited on session startup (homepage sets Akamai cookies, then oi-spurts and option-chain)
SEED_PAGES = [
    ("https://www.nseindia.com/", "https://www.google.com/"),
    ("https://www.nseindia.com/market-data/oi-spurts", "https://www.nseindia.com/"),
    ("https://www.nseindia.com/option-chain", "https://www.nseindia.com/"),
]


class NSESession:
    """
    Chrome-impersonating NSE session using curl-cffi.

    curl-cffi produces the exact same TLS fingerprint (JA3 hash) as
    Chrome 120. This bypasses Akamai Bot Manager at the TLS level,
    before any cookie or JavaScript challenge is even considered.

    Session lifecycle:
    - Built on first use (visits derivatives page + option chain)
    - Auto-refreshed every SESSION_REFRESH_SECONDS (10 min)
    - Re-seeded on 401/403/429 with 45s cooldown to prevent rebuild storms
    """

    def __init__(self):
        self._sess                 = None
        self._lock                 = RLock()
        self._last_init            = 0.0
        self._last_rebuild_attempt = 0.0

    def _new_session(self) -> cffi_requests.Session:
        """Create a fresh curl-cffi session impersonating Chrome."""
        s = cffi_requests.Session(impersonate=CHROME)
        s.headers.update(BASE_HEADERS)
        proxies = _proxy_kwargs()
        if proxies:
            s.proxies.update(proxies)
        return s

    def _build(self):
        """Seed a new session by visiting NSE pages in browser-like order."""
        logger.info("Building NSE Chrome-impersonation session...")
        sess = self._new_session()

        seeded_ok = False
        for url, referer in SEED_PAGES:
            nav_headers = {**NAV_EXTRA}
            if referer:
                nav_headers["Referer"] = referer
                nav_headers["Sec-Fetch-Site"] = "cross-site" if "google" in referer else "same-origin"
            else:
                nav_headers["Sec-Fetch-Site"] = "none"
            try:
                r = sess.get(url, headers=nav_headers, timeout=20)
                logger.info(f"  seed {r.status_code} {url}")
                if r.status_code == 200:
                    seeded_ok = True
            except Exception as e:
                logger.warning(f"  seed failed {url}: {e}")
            time.sleep(1.5)

        # Clear navigation-only headers from session base
        for h in ("Upgrade-Insecure-Requests", "Sec-Fetch-User", "Cache-Control", "Pragma", "Sec-Fetch-Dest", "Sec-Fetch-Mode", "Sec-Fetch-Site"):
            sess.headers.pop(h, None)
        sess.headers.update(BASE_HEADERS)

        if seeded_ok or sess.cookies:
            logger.info("NSE session ready (Chrome TLS fingerprint).")
            return sess
        logger.warning("NSE session seeding failed to acquire cookies.")
        return None

    def _ensure(self):
        """Rebuild session if missing or older than SESSION_REFRESH_SECONDS."""
        now = time.time()
        if self._sess is None or (now - self._last_init) > SESSION_REFRESH_SECONDS:
            new_sess = self._build()
            if new_sess is not None:
                self._sess = new_sess
                self._last_init = time.time()
                self._last_rebuild_attempt = time.time()

    def _safe_json(self, resp, label: str):
        """Parse JSON from response. Returns None if body is empty or HTML."""
        if resp is None or not resp.content:
            logger.warning(f"Empty response: {label}")
            return None
        ct = resp.headers.get("content-type", "")
        if "html" in ct.lower():
            snip = resp.text[:150].replace("\n", " ")
            logger.warning(f"HTML body from {label}: {snip!r}")
            return None
        try:
            return resp.json()
        except Exception as e:
            logger.warning(f"JSON parse error {label}: {e}")
            return None

    @staticmethod
    def _retry_delay(attempt: int) -> float:
        """Bounded exponential backoff with jitter for rate-limit recovery."""
        return min(20.0, 2.0 ** attempt) + random.uniform(0.0, 0.5)

    def _api_get(self, url: str, referer: str):
        """Raw API GET with JSON headers. Must hold lock."""
        if self._sess is None:
            return None
        for h in ("Upgrade-Insecure-Requests", "Sec-Fetch-User", "Cache-Control", "Pragma"):
            self._sess.headers.pop(h, None)
        self._sess.headers.update({**BASE_HEADERS, **API_EXTRA, "Referer": referer})
        try:
            return self._sess.get(url, timeout=25)
        except Exception as e:
            logger.error(f"Request error {url}: {e}")
            return None

    def _nav_get(self, url: str, referer: str):
        """Raw navigation GET (page visit). Must hold lock."""
        if self._sess is None:
            return None
        nav_headers = {**NAV_EXTRA}
        if referer:
            nav_headers["Referer"] = referer
            nav_headers["Sec-Fetch-Site"] = "same-origin"
        else:
            nav_headers["Sec-Fetch-Site"] = "none"
        try:
            return self._sess.get(url, headers=nav_headers, timeout=20)
        except Exception as e:
            logger.warning(f"Nav error {url}: {e}")
            return None

    # ?? Public API ?????????????????????????????????????????????????????????????

    def get(self, url: str, referer: str, retries: int = 2) -> dict | None:
        """Standard JSON API fetch with auto-retry on 4xx."""
        with self._lock:
            self._ensure()
            if self._sess is None:
                return None
            for attempt in range(retries):
                resp = self._api_get(url, referer)
                if resp is None:
                    time.sleep(2)
                    continue
                code = resp.status_code
                if code == 200:
                    return self._safe_json(resp, url)
                if code in (401, 403, 429):
                    logger.warning(f"HTTP {code} attempt {attempt+1} ? rebuilding session: {url}")
                    now = time.time()
                    if (now - self._last_rebuild_attempt) > 45:
                        self._last_rebuild_attempt = now
                        new_sess = self._build()
                        if new_sess is not None:
                            self._sess = new_sess
                            self._last_init = time.time()
                    time.sleep(self._retry_delay(attempt + 1))
                elif code == 404:
                    logger.warning(f"HTTP 404 (endpoint removed): {url}")
                    return None
                else:
                    logger.warning(f"HTTP {code}: {url}")
                    return None
            return None

    def get_text(self, url: str, referer: str, retries: int = 2) -> str | None:
        """Fetch a public text asset, retaining the same session/retry policy."""
        with self._lock:
            self._ensure()
            for attempt in range(retries):
                resp = self._api_get(url, referer)
                if resp is None:
                    time.sleep(self._retry_delay(attempt + 1))
                    continue
                if resp.status_code == 200 and resp.content:
                    return resp.text
                if resp.status_code in (401, 403, 429, 503):
                    logger.warning("HTTP %s fetching text asset; retrying %s", resp.status_code, url)
                    time.sleep(self._retry_delay(attempt + 1))
                    continue
                logger.warning("HTTP %s fetching text asset: %s", resp.status_code, url)
                return None
            return None

    def get_archive_text(self, url: str, referer: str, retries: int = 2) -> str | None:
        """Fetch a static NSE archive without the expensive live-page seed cycle."""
        headers = {**BASE_HEADERS, "Referer": referer, "Accept": "text/csv,text/plain,*/*"}
        for attempt in range(retries):
            try:
                request_kwargs = {"headers": headers, "impersonate": CHROME, "timeout": 25}
                proxies = _proxy_kwargs()
                if proxies:
                    request_kwargs["proxies"] = proxies
                response = cffi_requests.get(url, **request_kwargs)
                if response.status_code == 200 and response.content:
                    return response.text
                logger.warning("HTTP %s fetching archive %s", response.status_code, url)
            except Exception as exc:
                logger.warning("Archive fetch failed (attempt %s) %s: %s", attempt + 1, url, exc)
            if attempt + 1 < retries:
                time.sleep(self._retry_delay(attempt + 1))
        return None

    def get_seeded(self, seed_url: str, seed_referer: str,
                   api_url: str, api_referer: str,
                   retries: int = 3) -> dict | None:
        """
        Visit seed_url first (sets fresh per-symbol cookies), then call api_url.
        Both in one lock acquisition ? safe because RLock is re-entrant.

        Critical for option chain: NSE checks that the exact symbol page was
        visited right before the option chain API call.
        """
        with self._lock:
            self._ensure()
            for attempt in range(retries):
                # Step 1: Visit the seed page (browser navigation)
                logger.info(f"  seeding for option chain: {seed_url}")
                seed_resp = self._nav_get(seed_url, seed_referer)
                sc = seed_resp.status_code if seed_resp else "failed"
                logger.info(f"  seed status: {sc}")
                time.sleep(1.5)

                # Step 2: Call the JSON API
                api_resp = self._api_get(api_url, api_referer)
                if api_resp is None:
                    time.sleep(2)
                    continue

                code = api_resp.status_code
                if code == 200:
                    data = self._safe_json(api_resp, api_url)
                    if data is not None:
                        return data
                    # 200 but HTML ? session stale, rebuild
                    logger.warning("200 but HTML body ? checking session rebuild")
                    now = time.time()
                    if (now - self._last_rebuild_attempt) > 45:
                        self._last_rebuild_attempt = now
                        new_sess = self._build()
                        if new_sess is not None:
                            self._sess = new_sess
                            self._last_init = time.time()
                    time.sleep(self._retry_delay(attempt + 1))
                elif code in (401, 403, 429):
                    logger.warning(f"HTTP {code} option-chain attempt {attempt+1} ? checking rebuild")
                    now = time.time()
                    if (now - self._last_rebuild_attempt) > 45:
                        self._last_rebuild_attempt = now
                        new_sess = self._build()
                        if new_sess is not None:
                            self._sess = new_sess
                            self._last_init = time.time()
                    time.sleep(self._retry_delay(attempt + 1))
                elif code == 404:
                    logger.warning(f"HTTP 404 option-chain: {api_url}")
                    return None
                else:
                    logger.warning(f"HTTP {code} option-chain: {api_url}")
                    return None

            logger.error(f"Option chain failed after {retries} attempts: {api_url}")
            return None


# ?? Singleton ??????????????????????????????????????????????????????????????????
_nse = NSESession()
_angel_fno = AngelOneMarketData.from_environment()
_price_snapshots: OrderedDict[str, float] = OrderedDict()
_price_snapshot_lock = RLock()
MAX_PRICE_SNAPSHOTS = 1_000
_INDEX_PREVIOUS_CLOSES: dict[str, float] = {}
_INDEX_PREVIOUS_CLOSES_AT = 0.0
INDEX_PREVIOUS_CLOSE_TTL_SECONDS = 600
_INDEX_NAMES = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK", "FINNIFTY": "NIFTY FINANCIAL SERVICES", "MIDCPNIFTY": "NIFTY MIDCAP SELECT"}


def _index_previous_closes() -> dict[str, float]:
    global _INDEX_PREVIOUS_CLOSES, _INDEX_PREVIOUS_CLOSES_AT
    now = time.time()
    if now - _INDEX_PREVIOUS_CLOSES_AT < INDEX_PREVIOUS_CLOSE_TTL_SECONDS:
        return _INDEX_PREVIOUS_CLOSES
    try:
        payload = _nse.get(f"{NSE_BASE}/api/allIndices", referer="https://www.nseindia.com/market-data/live-equity-market") or {}
        rows = payload.get("data") or []
        names = {name: key for key, name in _INDEX_NAMES.items()}
        _INDEX_PREVIOUS_CLOSES = {key: float(row["previousClose"]) for row in rows if (key := names.get(str(row.get("index") or "").strip().upper())) and float(row.get("previousClose") or 0) > 0}
        _INDEX_PREVIOUS_CLOSES_AT = now
    except (TypeError, ValueError, AttributeError, KeyError):
        logger.warning("NSE allIndices previous-close lookup failed")
    return _INDEX_PREVIOUS_CLOSES


# ?? Public data functions ??????????????????????????????????????????????????????

# Field names that mean "NSE already told us the price change for this row".
# If ANY of these are present, we trust NSE's own number ? it's almost
# certainly a proper session/day-relative % change, which is the correct
# basis for signal generation. We must never clobber it.
_NATIVE_PRICE_CHANGE_FIELDS = (
    "pChange", "perChange", "changePer", "change_p", "perchange",
    "pchange", "percentChange",
)


PREVIOUS_CLOSE_MAX_AGE_DAYS = 10


def _previous_close_database_path() -> Path:
    """Resolve the bhavcopy database exactly like config.settings does."""
    project_root = Path(__file__).resolve().parents[1]
    configured_dir = Path(os.getenv("NSE_OI_DATA_DIR", "data"))
    data_dir = configured_dir if configured_dir.is_absolute() else project_root / configured_dir
    database = Path(os.getenv("NSE_OI_DATABASE", "nse_oi_tracker.sqlite3"))
    return database if database.is_absolute() else data_dir / database


def _stored_previous_close(symbol: str, *, today: date | None = None) -> float | None:
    """Read a recent NSE bhavcopy close; reject stale data as unsafe."""
    try:
        con = sqlite3.connect(_previous_close_database_path())
        try:
            row = con.execute(
                "SELECT trade_date, close FROM daily_equity_bars WHERE symbol=? ORDER BY trade_date DESC LIMIT 1",
                (symbol.upper().strip(),),
            ).fetchone()
        finally:
            con.close()
        if not row or float(row[1] or 0) <= 0:
            return None
        trade_date = date.fromisoformat(str(row[0]))
        reference_date = today or now_ist().date()
        age_days = (reference_date - trade_date).days
        if age_days > PREVIOUS_CLOSE_MAX_AGE_DAYS or age_days < 0:
            logger.warning("Ignoring invalid previous close for %s: %s", symbol, trade_date)
            return None
        return float(row[1])
    except (OSError, sqlite3.Error, TypeError, ValueError):
        return None


def fetch_all_fno_oi_change() -> list[dict]:
    """
    Fetch OI + price data for ALL F&O underlyings.

    IMPORTANT: this used to unconditionally overwrite NSE's own price-change
    fields with a 60-second poll-to-poll delta computed from an in-memory
    snapshot. That silently downgraded a proper day-relative % change into
    minute-level noise on every cycle after the first, which starved the
    HIGH-confidence signal filter (it compares that price delta against an
    OI-change % that IS day/session relative ? comparing two different
    timeframes made the whole signal matrix statistically broken).

    Fixed behavior: NSE's own price-change fields are used whenever present.
    The rolling snapshot below is now only a FALLBACK for rows where NSE's
    payload genuinely omits any price-change field (this does happen for
    some underlyings on this endpoint) ? in that case only, we fall back to
    a same-poll-interval delta so the row isn't dropped outright.
    """
    data = _nse.get(
        f"{NSE_BASE}/api/live-analysis-oi-spurts-underlyings",
        referer=f"{NSE_BASE}/market-data/oi-spurts",
    )
    if not data or not (data.get("data")):
        logger.info("OI spurts direct get empty or challenged, attempting seeded fetch")
        data = _nse.get_seeded(
            seed_url=f"{NSE_BASE}/market-data/oi-spurts",
            seed_referer=f"{NSE_BASE}/",
            api_url=f"{NSE_BASE}/api/live-analysis-oi-spurts-underlyings",
            api_referer=f"{NSE_BASE}/market-data/oi-spurts",
        )
    if not data:
        if _angel_fno and _angel_fno.configured:
            try:
                fallback = _angel_fno.fno_quotes()
                if fallback:
                    logger.warning("Using Angel One NFO futures fallback for %s OI rows", len(fallback))
                    return [
                        {
                            "symbol": symbol,
                            "ltp": quote.get("ltp", 0),
                            "underlyingValue": quote.get("ltp", 0),
                            "oi": quote.get("oi", 0),
                            "oiChange": quote.get("oi_change", 0),
                            "oiChangePct": quote.get("oi_change_pct", 0),
                            "pChange": quote.get("change_pct", 0),
                            "change": 0,
                            "_data_source": quote.get("source"),
                        }
                        for symbol, quote in fallback.items()
                    ]
            except Exception:
                logger.exception("Angel One NFO futures OI fallback failed")
        return []
    rows = data.get("data", [])
    enriched_rows: list[dict] = []
    with _price_snapshot_lock:
        for row in rows:
            symbol = str(row.get("symbol") or row.get("underlying") or "").upper().strip()
            current_price = row.get("underlyingValue") or row.get("lastPrice") or row.get("ltp")
            try:
                current_price = float(current_price or 0)
            except (TypeError, ValueError):
                current_price = 0.0

            enriched = dict(row)
            has_native_price_change = any(
                row.get(field) not in (None, "", "-") for field in _NATIVE_PRICE_CHANGE_FIELDS
            )
            previous_price = _price_snapshots.get(symbol) if symbol else None
            stored_close = _stored_previous_close(symbol) if symbol else None

            if symbol and current_price > 0:
                if has_native_price_change:
                    enriched["price_source"] = "nse_native"
                elif stored_close and stored_close > 0:
                    enriched["change"] = current_price - stored_close
                    enriched["pChange"] = ((current_price - stored_close) / stored_close) * 100
                    enriched["price_source"] = "previous_close_day_relative"
                elif symbol in _INDEX_NAMES and (index_close := _index_previous_closes().get(symbol)):
                    enriched["change"] = current_price - index_close
                    enriched["pChange"] = ((current_price - index_close) / index_close) * 100
                    enriched["price_source"] = "all_indices_previous_close"
                elif previous_price and previous_price > 0:
                    enriched["ltp"] = current_price
                    enriched["change"] = current_price - previous_price
                    enriched["pChange"] = ((current_price - previous_price) / previous_price) * 100
                    enriched["price_source"] = "rolling_minute_fallback"
                _price_snapshots[symbol] = current_price
                _price_snapshots.move_to_end(symbol)
                while len(_price_snapshots) > MAX_PRICE_SNAPSHOTS:
                    _price_snapshots.popitem(last=False)
            enriched_rows.append(enriched)

    logger.info(f"OI spurts: {len(enriched_rows)} F&O rows received")
    return enriched_rows


def fetch_fno_holiday_calendar() -> dict[int, set]:
    """Fetch the public NSE F&O holiday master and group dates by year.

    NSE labels the F&O segment `FO`. Weekend entries are intentionally kept;
    callers check weekends first and may still want the full published record.
    """
    data = _nse.get(
        f"{NSE_BASE}/api/holiday-master?type=trading",
        referer="https://www.nseindia.com/resources/exchange-communication-holidays",
    )
    if not isinstance(data, dict):
        return {}
    dates_by_year: dict[int, set] = {}
    for row in data.get("FO", []):
        raw_date = str(row.get("tradingDate") or "").strip()
        try:
            parsed = datetime.strptime(raw_date, "%d-%b-%Y").date()
        except ValueError:
            logger.warning("Unexpected NSE F&O holiday date: %r", raw_date)
            continue
        dates_by_year.setdefault(parsed.year, set()).add(parsed)
    return dates_by_year


def fetch_equity_bhavcopy(trade_date: date) -> str | None:
    """Fetch NSE's public full-equity bhavcopy for one trading date.

    One archive file contains all equities, so callers should download it once
    per day and persist the parsed rows rather than issue one request per
    symbol. NSE does not publish a bhavcopy on market holidays.
    """
    filename = f"sec_bhavdata_full_{trade_date.strftime('%d%m%Y')}.csv"
    return _nse.get_archive_text(
        f"https://nsearchives.nseindia.com/products/content/{filename}",
        referer="https://www.nseindia.com/market-data/all-upcoming-issues-ipo",
    )


def fetch_nse_archive_text(url: str, referer: str | None = None) -> str | None:
    """Generic NSE archive download through the working session/TLS-impersonating client."""
    return _nse.get_archive_text(url, referer=referer or f"{NSE_BASE}/all-reports-derivatives")




def fetch_index_history(index_type: str, from_date: date, to_date: date) -> list[dict]:
    """Fetch and normalize public NSE historical index OHLC rows."""
    encoded = quote(index_type, safe="")
    data = _nse.get(f"{NSE_BASE}/api/historical/indicesHistory?indexType={encoded}&from={from_date:%d-%m-%Y}&to={to_date:%d-%m-%Y}", referer=f"{NSE_BASE}/market-data/historical-indices") or {}
    raw = data.get("data") if isinstance(data, dict) else []
    if isinstance(raw, dict):
        rows = raw.get("indexCloseOnlineRecords") or raw.get("data") or raw.get("records") or []
    else:
        rows = raw or []
    out=[]
    for row in rows or []:
        def val(*keys):
            for key in keys:
                if row.get(key) not in (None, "", "-"): return row.get(key)
            return None
        raw_date=val("EOD_TIMESTAMP","TIMESTAMP","date","Date")
        raw_date = str(raw_date or "").replace(" ", "-")
        try: trade_date=datetime.strptime(str(raw_date)[:10], "%d-%b-%Y").date()
        except ValueError:
            try: trade_date=date.fromisoformat(str(raw_date)[:10])
            except ValueError: continue
        try:
            values=[float(val(*keys) or 0) for keys in (("EOD_OPEN_INDEX_VAL","OPEN_INDEX_VAL","open"),("EOD_HIGH_INDEX_VAL","HIGH_INDEX_VAL","high"),("EOD_LOW_INDEX_VAL","LOW_INDEX_VAL","low"),("EOD_CLOSE_INDEX_VAL","CLOSE_INDEX_VAL","close"))]
        except (TypeError,ValueError): continue
        if values[-1] > 0: out.append({"trade_date":trade_date.isoformat(),"open":values[0],"high":values[1],"low":values[2],"close":values[3]})
    return out


def fetch_market_indices() -> dict | None:
    """Fetch the public NSE broad-index feed, including INDIA VIX/breadth."""
    return _nse.get(
        f"{NSE_BASE}/api/allIndices",
        referer=f"{NSE_BASE}/market-data/live-market-indices",
    )












def test_nse_connectivity() -> dict:
    """
    Diagnostic ? test all key endpoints. Accessible via /api/debug.

    Note: whether curl-cffi's Chrome TLS impersonation actually bypasses
    Akamai depends partly on the deployment's egress region matching
    what was tested. Don't trust a comment claiming "confirmed working
    from <region>" unless render.yaml actually pins that region ? see
    render.yaml's `region:` field, which this app now sets explicitly.
    """
    results = {}
    tests = [
        ("oi_spurts",    lambda: fetch_all_fno_oi_change()),
        ("all_indices",  lambda: fetch_market_indices()),
    ]
    for name, fn in tests:
        try:
            data = fn()
            if data is None and name == "quote_deriv":
                fallback_rows = fetch_all_fno_oi_change()
                results[name] = (
                    f"fallback ok via oi_spurts ({len(fallback_rows)} rows)"
                    if fallback_rows else "blocked/empty"
                )
            elif data is None:
                results[name] = "blocked/empty"
            elif isinstance(data, list):
                results[name] = f"ok ({len(data)} rows)"
            else:
                keys = list(data.keys())[:5]
                results[name] = f"ok ? keys: {keys}"
        except Exception as e:
            results[name] = f"error: {e}"
    return results
