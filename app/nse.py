import csv
import hashlib
import io
import logging
import time
from datetime import datetime
from urllib.parse import urljoin
import requests
from .config import settings

log = logging.getLogger(__name__)
BASE = "https://www.nseindia.com"
ANNOUNCEMENTS = BASE + "/api/corporate-announcements?index=equities"

def _session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BASE + "/",
        "Connection": "keep-alive",
    })
    return s

def get_fno_symbols(session=None):
    if settings.fno_symbols:
        return set(settings.fno_symbols), "environment"
    s = session or _session()
    r = s.get(settings.universe_csv_url, timeout=settings.nse_timeout)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    symbols = set()
    for row in reader:
        for key, value in row.items():
            if key and key.strip().upper() in {"SYMBOL", "UNDERLYING"} and value:
                symbols.add(value.strip().upper())
                break
    if not symbols:
        raise RuntimeError("Could not parse symbols from official NSE F&O market-lot CSV; set FNO_SYMBOLS explicitly")
    return symbols, "nse-market-lot-csv"

def fetch_announcements():
    last_error = None
    for attempt in range(settings.nse_retries):
        s = _session()
        try:
            # NSE requires a session cookie before API requests.
            s.get(BASE, timeout=settings.nse_timeout)
            r = s.get(ANNOUNCEMENTS, timeout=settings.nse_timeout)
            if r.status_code == 403:
                raise PermissionError("NSE returned HTTP 403 (Akamai/access restriction)")
            r.raise_for_status()
            payload = r.json()
            if not isinstance(payload, list):
                raise ValueError("Unexpected NSE announcements response shape")
            return payload
        except Exception as exc:
            last_error = exc
            log.warning("NSE announcement poll failed (attempt %s/%s): %s", attempt+1, settings.nse_retries, exc)
            if attempt + 1 < settings.nse_retries:
                time.sleep(min(2 ** attempt, 8))
        finally:
            s.close()
    raise RuntimeError(str(last_error))

def _pick(row, *keys):
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""

def classify(headline: str, details: str):
    text = (headline + " " + details).lower()
    rules = [
        ("Financial Results", ("financial results","unaudited financial","audited financial","quarterly results","board meeting to consider")),
        ("Board Meeting", ("board meeting","meeting of board")),
        ("Order / Contract", ("order","contract","letter of award","loa","work order")),
        ("Dividend", ("dividend","interim dividend","final dividend")),
        ("Buyback", ("buyback","buy-back","share repurchase")),
        ("Acquisition / Merger", ("acquisition","acquire","merger","amalgamation","subsidiary")),
        ("Fundraising", ("fund raising","fundraise","preferential issue","qip","rights issue","qualified institutions")),
        ("Regulatory / Legal", (" sebi "," sebi","nclt","court","regulatory","penalty","investigation","show cause")),
        ("Credit Rating", ("credit rating","rating rationale","rating action")),
    ]
    for category, terms in rules:
        if any(term in text for term in terms):
            break
    else:
        category = "Corporate Announcement"
    positive = ("large order","order received","dividend","upgrade","acquisition of","profit increased","record profit")
    negative = ("default","fraud","resignation","penalty","downgrade","loss widened","insolvency","rating downgraded")
    pos = any(term in text for term in positive)
    neg = any(term in text for term in negative)
    impact = "mixed" if pos and neg else "potentially positive" if pos else "potentially negative" if neg else "uncertain"
    return category, impact

def normalize(row: dict, allowed: set[str]):
    symbol = _pick(row, "symbol", "sm_symbol", "symbol_name").upper()
    if not symbol or symbol not in allowed:
        return None
    headline = _pick(row, "desc", "subject", "sm_name", "headline") or "NSE corporate announcement"
    details = _pick(row, "attchmntText", "details", "description")
    filed = _pick(row, "an_dt", "broadcast_date", "timestamp", "date")
    raw_url = _pick(row, "attchmntFile", "attachment", "url")
    source = urljoin(BASE, raw_url) if raw_url else BASE + "/companies-listing/corporate-filings-announcements"
    stable = "|".join((symbol, filed, headline, raw_url))
    event_id = hashlib.sha256(stable.encode("utf-8")).hexdigest()
    category, impact = classify(headline, details)
    return {"event_id":event_id,"symbol":symbol,"company":_pick(row,"sm_name","company_name"),
            "category":category,"impact":impact,"headline":headline,"details":details,
            "filed_at":filed,"source_url":source}

