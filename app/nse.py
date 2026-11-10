import csv, hashlib, io, logging, re, time, xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo
import requests
from .config import settings

log = logging.getLogger(__name__)
NSE_ARCHIVE = "https://nsearchives.nseindia.com"
NSE_SITE = "https://www.nseindia.com"
NSE_BOARD_MEETINGS_API = NSE_SITE + "/api/corporate-board-meetings"
FEEDS = {
    "announcements": NSE_ARCHIVE + "/content/RSS/Online_announcements.xml",
    "board_meetings": NSE_ARCHIVE + "/content/RSS/Board_Meetings.xml",
    "financial_results": NSE_ARCHIVE + "/content/RSS/Financial_Results.xml",
    # Independent exchange feeds: useful when NSE's JSON endpoints are blocked
    # from a hosting provider. These remain public BSE RSS endpoints.
    "bse_announcements": "https://www.bseindia.com/data/xml/announcements.aspx",
    "bse_board_meetings": "https://beta.bseindia.com/Data/XML/BoardMeetingsFeed.xml",
    "bse_financial_results": "https://www.bseindia.com/Data/XML/FinancialResultsFeed.aspx",
}

def _session():
    s=requests.Session()
    # Use a normal browser-compatible session for official NSE static files and
    # JSON endpoints. This improves request compatibility but does not bypass IP-based NSE blocks.
    s.headers.update({
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,*/*;q=0.8",
        "Accept-Language":"en-US,en;q=0.9",
        "Connection":"keep-alive",
    })
    return s

def _valid_stock_symbol(value):
    symbol = str(value or "").strip().upper()
    # NSE equity trading symbols do not contain spaces. Keep common valid
    # characters such as &, hyphen and leading digits (e.g. M&M, BAJAJ-AUTO,
    # 360ONE); reject company and index display names.
    if symbol in {"SYMBOL", "UNDERLYING", "CONTRACT"}:
        return ""
    return symbol if re.fullmatch(r"[A-Z0-9&-]{1,20}", symbol) else ""


def _is_index_underlying(value):
    name = re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()
    return name.startswith("NIFTY ") or name in {
        "NIFTY", "NIFTY 50", "NIFTY BANK", "NIFTY FINANCIAL SERVICES",
        "NIFTY MIDCAP SELECT", "NIFTY NEXT 50",
    }


_FNO_SYMBOLS_CACHE = set()
_FNO_SYMBOLS_RETRY_AFTER = 0.0


def get_fno_symbols(session=None):
    global _FNO_SYMBOLS_CACHE, _FNO_SYMBOLS_RETRY_AFTER
    # The official file has both UNDERLYING (display name) and SYMBOL (ticker).
    # Always use SYMBOL. The current file places UNDERLYING first, so reading
    # the first matching column silently loads company names instead of tickers.
    # FNO_SYMBOLS is additive, but malformed names are never accepted as symbols.
    s=session or _session()
    configured = {
        valid for item in settings.fno_symbols
        if (valid := _valid_stock_symbol(item))
    }
    fallback = {
        valid for item in getattr(settings, "fallback_fno_symbols", ())
        if (valid := _valid_stock_symbol(item))
    }
    now = time.monotonic()
    if session is None and now < _FNO_SYMBOLS_RETRY_AFTER:
        if _FNO_SYMBOLS_CACHE:
            cached = set(_FNO_SYMBOLS_CACHE) | configured
            return cached, "in-memory last-known-good F&O universe (NSE retry cooldown)"
        if configured:
            return configured, "FNO_SYMBOLS environment fallback (NSE retry cooldown)"
        if fallback:
            return fallback, "FNO_FALLBACK_SYMBOLS static recovery universe (NSE retry cooldown)"
    try:
        r=s.get(settings.universe_csv_url,headers={
            "Accept":"text/csv,application/csv,*/*;q=0.8",
            "Referer":"https://www.nseindia.com/static/products-services/equity-derivatives-contract-information",
        },timeout=settings.nse_timeout); r.raise_for_status()
        reader=csv.DictReader(io.StringIO(r.text))
        fields = {
            str(field or "").strip().lstrip("\\ufeff").upper(): field
            for field in (reader.fieldnames or [])
        }
        symbol_field = fields.get("SYMBOL")
        if not symbol_field:
            raise RuntimeError("NSE F&O market-lot CSV has no SYMBOL column; refusing to use display-name UNDERLYING")
        underlying_field = fields.get("UNDERLYING")
        symbols=set()
        for row in reader:
            symbol = _valid_stock_symbol(row.get(symbol_field))
            underlying = row.get(underlying_field) if underlying_field else ""
            if not symbol or _is_index_underlying(underlying):
                continue
            symbols.add(symbol)
        if not symbols:
            raise RuntimeError("NSE F&O market-lot CSV parsed but contained no valid stock symbols")
        symbols.update(configured)
        _FNO_SYMBOLS_CACHE = set(symbols)
        _FNO_SYMBOLS_RETRY_AFTER = 0.0
        source="NSE F&O market-lot CSV (SYMBOL column; stock underlyings only)"
        if configured:
            source+=" + valid FNO_SYMBOLS additions"
        return symbols,source
    except Exception as exc:
        response = getattr(exc, "response", None)
        delay = 1800 if getattr(response, "status_code", None) == 403 else 300
        _FNO_SYMBOLS_RETRY_AFTER = now + delay
        if configured:
            return configured,"FNO_SYMBOLS environment fallback (NSE CSV unavailable; valid tickers only)"
        if fallback:
            return fallback,"FNO_FALLBACK_SYMBOLS static recovery universe (NSE CSV unavailable)"
        raise

def _tag(el,name):
    for child in el:
        if child.tag.split("}")[-1].lower()==name.lower():
            return (child.text or "").strip()
    return ""



# Per-process source backoff and short-lived cache. A blocked exchange endpoint
# should not be hammered every five minutes; cached RSS items remain usable
# for up to one hour while the source is retried periodically.
_SOURCE_CACHE = {}
_SOURCE_RETRY_AFTER = {}
_SOURCE_FAILURES = {}
_SOURCE_CACHE_MAX_AGE = 3600


def _fetch_source(name, fetcher):
    now = time.monotonic()
    cached = _SOURCE_CACHE.get(name)
    retry_at = _SOURCE_RETRY_AFTER.get(name, 0.0)
    if now < retry_at:
        if cached and now - cached["at"] <= _SOURCE_CACHE_MAX_AGE:
            items = [dict(item) for item in cached["items"]]
            return items, {
                "ok": False, "stale": True, "items": len(items),
                "error": "Using recent cached feed during source retry cooldown",
                "retry_in_seconds": max(1, int(retry_at - now)),
            }
        return [], {
            "ok": False, "items": 0,
            "error": "Source retry cooldown active; no recent cached feed",
            "retry_in_seconds": max(1, int(retry_at - now)),
        }
    try:
        items = fetcher()
        copied = [dict(item) for item in items]
        _SOURCE_CACHE[name] = {"at": now, "items": copied}
        _SOURCE_FAILURES[name] = 0
        _SOURCE_RETRY_AFTER.pop(name, None)
        return items, {"ok": True, "stale": False, "items": len(items)}
    except Exception as exc:
        failures = _SOURCE_FAILURES.get(name, 0) + 1
        _SOURCE_FAILURES[name] = failures
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        # 403s from exchange edge protection are stable for long periods.
        # Retry every 30 minutes rather than on every poll. Other errors use
        # bounded exponential backoff.
        delay = 1800 if status_code == 403 else min(900, 30 * (2 ** min(failures - 1, 5)))
        _SOURCE_RETRY_AFTER[name] = now + delay
        if cached and now - cached["at"] <= _SOURCE_CACHE_MAX_AGE:
            items = [dict(item) for item in cached["items"]]
            log.warning("%s failed; retaining %d cached items for up to one hour: %s",
                        name, len(items), exc)
            return items, {
                "ok": False, "stale": True, "items": len(items),
                "error": str(exc), "retry_in_seconds": delay,
            }
        log.warning("%s failed; retrying in %s seconds: %s", name, delay, exc)
        return [], {"ok": False, "stale": False, "items": 0,
                    "error": str(exc), "retry_in_seconds": delay}


def fetch_feed(session,url):
    r=session.get(url,timeout=settings.nse_timeout); r.raise_for_status()
    root=ET.fromstring(r.content)
    items=[]
    for item in root.iter():
        if item.tag.split("}")[-1].lower() not in {"item","entry"}: continue
        title=_tag(item,"title")
        desc=_tag(item,"description") or _tag(item,"summary") or _tag(item,"content")
        link=_tag(item,"link")
        if not link:
            for child in item:
                if child.tag.split("}")[-1].lower()=="link":
                    link=child.attrib.get("href",""); break
        pub=_tag(item,"pubDate") or _tag(item,"published") or _tag(item,"updated")
        items.append({"title":title,"description":re.sub("<[^>]+>"," ",desc),
                      "link":urljoin(url,link),"published":pub})
    return items

def _board_meeting_api_items(payload, allowed):
    records = payload if isinstance(payload, list) else (
        (payload or {}).get("data") or (payload or {}).get("records") or []
    )
    items = []
    for row in records:
        if not isinstance(row, dict):
            continue
        symbol = _valid_stock_symbol(row.get("bm_symbol"))
        if not symbol or symbol not in allowed:
            continue
        purpose = str(row.get("bm_purpose") or "").strip()
        meeting_date = str(row.get("bm_date") or "").strip()
        if meeting_date:
            purpose = (purpose + " | " if purpose else "") + "Meeting date: " + meeting_date
        items.append({
            "title": symbol + " " + str(row.get("bm_desc") or "Board Meeting Intimation"),
            "description": purpose,
            "link": str(row.get("attachment") or ""),
            "published": str(row.get("bm_timestamp") or ""),
            "feed_type": "board_meetings",
            "source_name": "NSE Corporate Board Meetings",
        })
    return items


def fetch_board_meetings_api(allowed):
    # Official NSE board-meeting API supplements RSS because the RSS list can
    # omit older notices for future meetings. Use a normal cookie/session
    # handshake only; no proxy, challenge bypass or scraping workaround.
    today = datetime.now(ZoneInfo("Asia/Kolkata")).date()
    params = {
        "index": "equities",
        "fo_sec": "true",
        # NSE's board-meeting listing can retain a future meeting notice
        # whose broadcast date is weeks earlier (e.g. 22-Oct for a 10-Nov
        # meeting), so query a look-back window as well as future meeting dates.
        "from_date": (today - timedelta(days=60)).strftime("%d-%m-%Y"),
        "to_date": (today + timedelta(days=60)).strftime("%d-%m-%Y"),
    }
    # Request the JSON resource directly first. A homepage cookie handshake is
    # counterproductive on Render when NSE blocks the homepage with HTTP 403:
    # it prevents even attempting the actual API endpoint.
    with _session() as session:
        session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Referer": NSE_SITE + "/companies-listing/corporate-filings-board-meetings",
            "Origin": NSE_SITE,
        })
        response = session.get(NSE_BOARD_MEETINGS_API, params=params, timeout=settings.nse_timeout)
        response.raise_for_status()
        return _board_meeting_api_items(response.json(), allowed)




def _event_calendar_items(payload, allowed):
    records = payload if isinstance(payload, list) else (
        (payload or {}).get("data") or (payload or {}).get("records") or
        (payload or {}).get("events") or []
    )
    items = []
    for row in records:
        if not isinstance(row, dict):
            continue
        symbol = _valid_stock_symbol(row.get("symbol") or row.get("SYMBOL"))
        if not symbol or symbol not in allowed:
            continue
        purpose = str(row.get("purpose") or row.get("PURPOSE") or "").strip()
        details = str(row.get("details") or row.get("DETAILS") or "").strip()
        company = str(row.get("company") or row.get("COMPANY") or "").strip()
        meeting_date = str(row.get("date") or row.get("DATE") or row.get("event_date") or
                           row.get("meeting_date") or row.get("bm_date") or "").strip()
        if not meeting_date:
            continue
        agenda = " | ".join(x for x in (purpose, details) if x)
        items.append({
            "title": " ".join(x for x in (symbol, company, purpose or "Board Meeting") if x),
            "description": (agenda + " | " if agenda else "") + "Meeting date: " + meeting_date,
            "link": NSE_SITE + "/companies-listing/corporate-filings-event-calendar",
            "published": "",
            "feed_type": "event_calendar",
            "source_name": "NSE Corporate Event Calendar",
        })
    return items

def fetch_event_calendar_api(allowed):
    """Fetch NSE's separate corporate event calendar.

    Unlike the board-meetings listing, this calendar is designed to retain
    upcoming meeting dates. It may not expose the original filing timestamp;
    in that case filed_at stays empty and the UI must say that time was not
    supplied rather than substituting the poll time.
    """
    today = datetime.now(ZoneInfo("Asia/Kolkata")).date()
    params = {
        "index": "equities",
        "from_date": (today - timedelta(days=60)).strftime("%d-%m-%Y"),
        "to_date": (today + timedelta(days=90)).strftime("%d-%m-%Y"),
        "fo_sec": "true",
    }
    with _session() as session:
        session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Referer": NSE_SITE + "/companies-listing/corporate-filings-event-calendar",
            "Origin": NSE_SITE,
        })
        response = session.get(NSE_SITE + "/api/event-calendar", params=params,
                               timeout=settings.nse_timeout)
        response.raise_for_status()
        payload = response.json()

    return _event_calendar_items(payload, allowed)

def fetch_all_feeds(allowed=None, company_map=None):
    session=_session(); output=[]; health={}
    try:
        for name,url in FEEDS.items():
            is_bse = name.startswith("bse_")
            if is_bse:
                session.headers.update({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36",
                    "Referer": "https://www.bseindia.com/",
                    "Origin": "https://www.bseindia.com",
                })
            else:
                # NSE's edge protection rejects generic bot-style RSS user agents.
                # Keep a mainstream browser UA and identify the NSE site as the referrer.
                session.headers.update({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
                    "Referer": "https://www.nseindia.com/",
                    "Origin": "https://www.nseindia.com",
                    "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
                })
            items, feed_health = _fetch_source(
                name, lambda url=url: fetch_feed(session, url)
            )
            health[name] = feed_health
            feed_kind = ("board_meetings" if name in {"board_meetings", "bse_board_meetings"} else ("financial_results" if name in {"financial_results", "bse_financial_results"} else "announcements"))
            for x in items:
                x["feed_type"]=feed_kind
                x["source_name"]="BSE RSS" if is_bse else "NSE RSS"
            output.extend(items)
    finally: session.close()

    # Structured board-meetings API supplies filing timestamps when available.
    # The separate NSE Event Calendar is a second official schedule source; it
    # can preserve future meeting dates even when a board-meetings RSS/API view
    # has a shorter retention window. Neither source's poll time is a filing time.
    if allowed:
        known = set()
        if company_map is not None:
            for raw in output:
                event = normalize(raw, allowed, company_map)
                if event and event.get("event_date"):
                    known.add((event["symbol"], event["event_date"], event["category"]))
        for source_key, fetcher in (
            ("board_meetings_api", fetch_board_meetings_api),
            ("event_calendar_api", fetch_event_calendar_api),
        ):
            fetched, source_status = _fetch_source(
                source_key, lambda fetcher=fetcher: fetcher(allowed)
            )
            unique_items = []
            for raw in fetched:
                event = normalize(raw, allowed, company_map or {})
                key = (event["symbol"], event["event_date"], event["category"]) if event and event.get("event_date") else None
                if key and key in known:
                    continue
                if key:
                    known.add(key)
                unique_items.append(raw)
            output.extend(unique_items)
            health[source_key] = dict(source_status)
            health[source_key]["items"] = len(unique_items)

    # A successful empty response is a valid quiet-feed result, not an outage.
    # Raise only when every RSS and supplemental source failed and no cache exists.
    if not output and not any(feed.get("ok") for feed in health.values()):
        raise RuntimeError("All official exchange feeds failed: "+str(health))
    return output,health

def _date(value):
    if not value: return None
    raw=str(value).strip()
    # NSE board-meeting API supplies local IST broadcast timestamps in
    # "DD-Mon-YYYY HH:MM:SS" format; convert them to UTC for consistent storage.
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=ZoneInfo("Asia/Kolkata"))
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    # Preserve date-only feed values as dates; do not invent midnight UTC.
    for fmt in ("%d-%b-%Y","%d-%m-%Y","%d/%m/%Y","%Y-%m-%d"):
        try: return datetime.strptime(raw,fmt).date().isoformat()
        except ValueError: pass
    try:
        parsed=parsedate_to_datetime(raw)
        if parsed.tzinfo is None: parsed=parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def _parse_event_date(text):
    """Extract a board/result meeting date only when the context indicates a meeting."""
    cleaned = re.sub(r"\b(\d{1,2})(st|nd|rd|th)\b", r"\1", text or "", flags=re.I)
    month = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    patterns = [
        r"\b\d{4}-\d{1,2}-\d{1,2}\b",
        r"\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b",
        rf"\b\d{{1,2}}(?:st|nd|rd|th)?[\s-]+{month}[\s,.-]+\d{{4}}\b",
        rf"\b{month}\s+\d{{1,2}}(?:st|nd|rd|th)?[,]?\s+\d{{4}}\b",
    ]
    candidates = []
    formats = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y",
               "%d %b %Y", "%d %B %Y", "%d-%b-%Y", "%d-%B-%Y",
               "%B %d %Y", "%b %d %Y")
    for pattern in patterns:
        for match in re.finditer(pattern, cleaned, flags=re.I):
            raw = re.sub(r"(?i)(\d)(st|nd|rd|th)\b", r"\1", match.group(0)).replace(",", " ")
            parsed = None
            for fmt in formats:
                try:
                    parsed = datetime.strptime(" ".join(raw.split()), fmt).date()
                    break
                except ValueError:
                    continue
            if parsed is None:
                continue
            before = cleaned[max(0, match.start()-100):match.start()].lower()
            after = cleaned[match.end():min(len(cleaned), match.end()+50)].lower()
            context = before + " " + after
            score = 0
            if re.search(r"(board|committee).{0,90}(meeting|meet)|meeting.{0,90}$", before):
                score += 3
            if re.search(r"(held|scheduled|convene|conduct|take place).{0,50}$", before):
                score += 3
            if re.search(r"^(?:\s|,|:|-)*(?:as scheduled|for the meeting|meeting)", after):
                score += 1
            if re.search(r"quarter ended|year ended|financial year|record date", context):
                score -= 3
            candidates.append((score, match.start(), parsed))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    score, _, selected = candidates[0]
    return selected.isoformat() if score >= 3 else None


def _dedupe_key(symbol, category, headline, event_date=None):
    # Same-date copies from different feeds dedupe; revised meeting dates remain distinct.
    normalized = re.sub(r"[^a-z0-9]+", " ", (headline or "").lower()).strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return hashlib.sha256(f"{symbol.upper()}|{normalized}|{event_date or ''}".encode()).hexdigest()

def _classify(title,details,feed_type,event_date=None):
    text=(title+" "+details).lower()
    is_board = feed_type in {"board_meetings", "event_calendar"} or "board meeting" in text or "meeting of the board" in text
    result_terms = ("financial results","quarterly results","audited results","unaudited results","standalone results","consolidated results")
    has_result = any(x in text for x in result_terms) or bool(re.search(r"\bq[1-4]\s*(?:fy\s*)?(?:20\d{2}|\d{2})?\s*results\b", text))
    result_agenda = has_result and (feed_type=="event_calendar" or any(x in text for x in ("consider","approve","approval","to take on record","meeting","scheduled")))
    is_outcome = any(x in text for x in ("outcome of board meeting","results declared","financial results approved","approved the financial results","statement of standalone and consolidated"))
    result_release = (feed_type=="financial_results") or (has_result and (is_outcome or
        ("financial results" in text and not result_agenda and not is_board)))
    if is_board and result_agenda and not is_outcome: category="Financial Results"
    elif has_result: category="Financial Results"
    elif any(x in text for x in ("dividend","record date","split","bonus issue")): category="Corporate Action"
    elif any(x in text for x in ("order","letter of award","work order","contract")): category="Order / Contract"
    elif any(x in text for x in ("resignation","appointed as","ceo","managing director","key managerial")): category="Management Change"
    elif any(x in text for x in ("sebi","nclt","court","penalty","investigation","show cause","default")): category="Regulatory / Legal"
    elif any(x in text for x in ("acquisition","merger","amalgamation","buyback","fund raising","qip","rights issue","credit rating")): category="Corporate Action"
    elif is_board: category="Board Meeting"
    else: category="Corporate Announcement"

    direction="unassessed"
    rationale="Direction not assessed: the exchange feed did not include comparable financial figures. Do not infer a trade direction from the filing headline alone."
    if result_release:
        profit_down = bool(re.search(r"(?:net profit|profit after tax|\bpat\b).{0,70}(?:down|declin\w*|fell|decreas\w*|lower).{0,20}\d+(?:\.\d+)?\s*%", text))
        profit_up = bool(re.search(r"(?:net profit|profit after tax|\bpat\b).{0,70}(?:up|increas\w*|grew|higher).{0,20}\d+(?:\.\d+)?\s*%", text))
        revenue_down = bool(re.search(r"(?:revenue|sales).{0,70}(?:down|declin\w*|fell|decreas\w*|lower).{0,20}\d+(?:\.\d+)?\s*%", text))
        revenue_up = bool(re.search(r"(?:revenue|sales).{0,70}(?:up|increas\w*|grew|higher).{0,20}\d+(?:\.\d+)?\s*%", text))
        if profit_down and revenue_down: direction="bearish"; rationale="Net profit and revenue are both reported lower in the available comparison. This is a negative results signal, not a price prediction."
        elif profit_up and revenue_up: direction="bullish"; rationale="Net profit and revenue are both reported higher in the available comparison. This is a positive results signal, not a price prediction."
        elif profit_down and revenue_up or profit_up and revenue_down: direction="mixed"; rationale="Revenue and profit signals conflict in the available comparison; treat the result as mixed."
        elif profit_down: direction="bearish"; rationale="Net profit is reported lower in the available comparison. This is a negative results signal, not a price prediction."
        elif profit_up: direction="bullish"; rationale="Net profit is reported higher in the available comparison. This is a positive results signal, not a price prediction."
        state="RESULT PUBLISHED"
    elif any(x in text for x in ("rescheduled","postponed","cancelled","canceled")):
        direction="unassessed"; rationale="Schedule change detected. Check the revised or cancelled date in the original filing."
        state="SCHEDULE CHANGE"
    elif is_board and result_agenda:
        rationale="The board notice says financial results are on the agenda. This is a scheduled announcement, not a published result."
        if event_date:
            from zoneinfo import ZoneInfo
            today=datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
            state="RESULT SCHEDULED TODAY" if event_date==today else ("RESULT SCHEDULED" if event_date>today else "BOARD MEETING NOTICE")
        else:
            state="RESULT DATE NOT CONFIRMED"
    elif is_board:
        rationale="Routine board meeting notice; not an alert-worthy result or sudden event."
        state="BOARD MEETING NOTICE"
    else:
        positive = ("secured order","bagged order","awarded contract","letter of award","wins contract","order win","acquisition completed","credit rating upgraded","rating upgraded","production resumed")
        negative = ("fire","explosion","accident","fatal","plant shutdown","production halted","fraud","default","insolvency","bankruptcy","penalty","show cause","investigation","resigned","resignation","order cancelled","contract terminated","credit rating downgraded","rating downgraded","regulatory action","raid")
        pos=any(x in text for x in positive); neg=any(x in text for x in negative)
        sudden=pos or neg
        if sudden:
            state="SUDDEN EVENT"
            if pos and neg: direction="mixed"; rationale="The filing contains both positive and negative event signals. Read the original disclosure before acting."
            elif neg: direction="bearish"; rationale="The filing describes a potentially negative company-specific event. This is an event-risk flag, not a price prediction."
            else: direction="bullish"; rationale="The filing describes a potentially positive company-specific event. This is an event flag, not a price prediction."
        else:
            direction="unassessed"; rationale="Routine or low-confidence announcement; excluded from push alerts."
            state="NEW EVENT"
    return category,state,direction,rationale

_COMPANY_MAP_CACHE = {}
_COMPANY_MAP_RETRY_AFTER = 0.0


def get_fno_company_map(allowed):
    global _COMPANY_MAP_CACHE, _COMPANY_MAP_RETRY_AFTER
    now = time.monotonic()
    if now < _COMPANY_MAP_RETRY_AFTER and _COMPANY_MAP_CACHE:
        return {symbol: name for symbol, name in _COMPANY_MAP_CACHE.items() if symbol in allowed}
    url=NSE_ARCHIVE + "/content/equities/EQUITY_L.csv"
    try:
        with _session() as session:
            response=session.get(url,timeout=settings.nse_timeout); response.raise_for_status()
        mapping={}
        for row in csv.DictReader(io.StringIO(response.text)):
            symbol=(row.get("SYMBOL") or "").strip().upper()
            name=(row.get("NAME OF COMPANY") or row.get("NAME_OF_COMPANY") or "").strip()
            if symbol in allowed and name: mapping[symbol]=name
        if mapping:
            _COMPANY_MAP_CACHE = dict(mapping)
            _COMPANY_MAP_RETRY_AFTER = 0.0
        return mapping
    except Exception as exc:
        _COMPANY_MAP_RETRY_AFTER = now + (1800 if getattr(getattr(exc, "response", None), "status_code", None) == 403 else 300)
        cached = {symbol: name for symbol, name in _COMPANY_MAP_CACHE.items() if symbol in allowed}
        if cached:
            log.warning("NSE equity master unavailable; using %d cached company names for 30 minutes: %s",
                        len(cached), exc)
            return cached
        log.warning("Could not load NSE equity company-name map: %s",exc)
        return {}

def _company_match_key(value):
    # NSE RSS sometimes abbreviates "India Limited" as "IND LTD".
    # Removing these legal suffix tokens allows equivalent names to match.
    ignored = {"INDIA", "IND", "LIMITED", "LTD", "PRIVATE", "PVT", "COMPANY", "CO"}
    words = re.findall(r"[A-Z0-9]+", str(value or "").upper())
    return "".join(word for word in words if word not in ignored)


KNOWN_COMPANY_ALIASES = {
    "COCHINSHIP": ("COCHIN SHIPYARD LIMITED", "COCHIN SHIPYARD LTD"),
    "KALYANKJIL": ("KALYAN JEWELLERS INDIA LIMITED", "KALYAN JEWELLERS IND LTD",
                   "KALYAN JEWELLERS"),
    "RVNL": ("RAIL VIKAS NIGAM LIMITED", "RAIL VIKAS NIGAM LTD"),
}
KNOWN_COMPANY_NAMES = {
    "COCHINSHIP": "Cochin Shipyard Limited",
    "KALYANKJIL": "Kalyan Jewellers India Limited",
    "RVNL": "Rail Vikas Nigam Limited",
}


def normalize(item,allowed,company_map=None):
    title=(item.get("title") or "").strip()
    details=(item.get("description") or "").strip()
    hay=(title+" "+details).upper()
    symbol=""
    # RSS entries sometimes carry the trading symbol in their text.
    for candidate in allowed:
        if re.search(r"(?<![A-Z0-9])"+re.escape(candidate)+r"(?![A-Z0-9])",hay):
            symbol=candidate; break
    company_map=company_map or {}
    if not symbol:
        normalized=_company_match_key(hay)
        matches=set()
        for candidate,name in company_map.items():
            alias=_company_match_key(name)
            if candidate in allowed and len(alias)>=8 and alias in normalized:
                matches.add(candidate)
        # Known NSE legal-name variants cover filings where the RSS title uses
        # abbreviations and the equity master uses a different company-name form.
        for candidate,aliases in KNOWN_COMPANY_ALIASES.items():
            if candidate in allowed and any(
                len(_company_match_key(alias))>=8 and _company_match_key(alias) in normalized
                for alias in aliases
            ):
                matches.add(candidate)
        if len(matches)==1: symbol=next(iter(matches))
    if not symbol: return None
    filed=_date(item.get("published"))
    event_date=_parse_event_date(title+" "+details)
    category,state,direction,rationale=_classify(title,details,item.get("feed_type",""),event_date)
    url=item.get("link") or "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    ident="|".join((symbol,filed or "",title,url))
    if state=="RESULT PUBLISHED":
        period_match=re.search(r"\bq[1-4]\s*(?:fy\s*)?(?:20\d{2}|\d{2})?\b",hay)
        period=period_match.group(0).strip() if period_match else (filed[:10] if filed else "unknown-period")
        dedupe_key=hashlib.sha256(f"{symbol}|RESULT|{period}".encode()).hexdigest()
    else:
        dedupe_key=_dedupe_key(symbol,category,title,event_date)
    company = company_map.get(symbol) or KNOWN_COMPANY_NAMES.get(symbol) or symbol
    return {"event_id":hashlib.sha256(ident.encode()).hexdigest(),"dedupe_key":dedupe_key,"symbol":symbol,"company":company,"event_date":event_date,
      "category":category,"impact":direction,"headline":title or "NSE filing","details":details,
      "filed_at":filed,"source_url":url,"event_status":state,"rationale":rationale,
      "source_name":item.get("source_name","NSE RSS"),"feed_type":item.get("feed_type","")}
