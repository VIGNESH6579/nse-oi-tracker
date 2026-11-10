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


def get_fno_symbols(session=None):
    # The official file has both UNDERLYING (display name) and SYMBOL (ticker).
    # Always use SYMBOL. The current file places UNDERLYING first, so reading
    # the first matching column silently loads company names instead of tickers.
    # FNO_SYMBOLS is additive, but malformed names are never accepted as symbols.
    s=session or _session()
    configured = {
        valid for item in settings.fno_symbols
        if (valid := _valid_stock_symbol(item))
    }
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
        source="NSE F&O market-lot CSV (SYMBOL column; stock underlyings only)"
        if configured:
            source+=" + valid FNO_SYMBOLS additions"
        return symbols,source
    except Exception:
        if configured:
            return configured,"FNO_SYMBOLS environment fallback (NSE CSV unavailable; valid tickers only)"
        raise

def _tag(el,name):
    for child in el:
        if child.tag.split("}")[-1].lower()==name.lower():
            return (child.text or "").strip()
    return ""

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
    with requests.Session() as session:
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": NSE_SITE + "/companies-listing/corporate-filings-board-meetings",
            "Origin": NSE_SITE,
        })
        home = session.get(NSE_SITE, timeout=settings.nse_timeout)
        home.raise_for_status()
        response = session.get(NSE_BOARD_MEETINGS_API, params=params, timeout=settings.nse_timeout)
        response.raise_for_status()
        items = _board_meeting_api_items(response.json(), allowed)
        return items




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
    with requests.Session() as session:
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
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
            try:
                is_bse = name.startswith("bse_")
                if is_bse:
                    session.headers.update({
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36",
                        "Referer": "https://www.bseindia.com/",
                        "Origin": "https://www.bseindia.com",
                    })
                else:
                    session.headers.pop("Referer", None)
                    session.headers.pop("Origin", None)
                    session.headers["User-Agent"] = "NSE-Company-News-Monitor/1.0 (RSS reader)"
                items=fetch_feed(session,url); health[name]={"ok":True,"items":len(items)}
                feed_kind = "board_meetings" if name == "bse_board_meetings" else ("financial_results" if name == "bse_financial_results" else "announcements")
                for x in items:
                    x["feed_type"]=feed_kind
                    x["source_name"]="BSE RSS" if is_bse else "NSE RSS"
                output.extend(items)
            except Exception as exc:
                health[name]={"ok":False,"error":str(exc)}
                log.warning("NSE RSS %s failed: %s",name,exc)
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
            try:
                fetched = fetcher(allowed)
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
                health[source_key] = {"ok": True, "items": len(unique_items)}
            except Exception as exc:
                health[source_key] = {"ok": False, "error": str(exc)}
                log.warning("NSE %s unavailable; retaining other sources: %s", source_key, exc)

    if not output: raise RuntimeError("All official NSE RSS feeds failed: "+str(health))
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
    result_agenda = any(x in text for x in result_terms) and (feed_type=="event_calendar" or any(x in text for x in ("consider","approve","approval","to take on record","meeting")))
    if is_board and result_agenda: category="Financial Results"
    elif is_board: category="Board Meeting"
    elif feed_type=="financial_results" or any(x in text for x in result_terms): category="Financial Results"
    elif any(x in text for x in ("dividend","record date","split","bonus issue")): category="Corporate Action"
    elif any(x in text for x in ("order","letter of award","work order","contract")): category="Order / Contract"
    elif any(x in text for x in ("resignation","appointed as","ceo","managing director","key managerial")): category="Management Change"
    elif any(x in text for x in ("sebi","nclt","court","penalty","investigation","show cause","default")): category="Regulatory / Legal"
    elif any(x in text for x in ("acquisition","merger","amalgamation","buyback","fund raising","qip","rights issue","credit rating")): category="Corporate Action"
    else: category="Corporate Announcement"
    is_result=category=="Financial Results" and any(x in text for x in ("result","financial","quarter","audited","unaudited"))
    direction="neutral"
    if feed_type=="financial_results" and is_result:
        rationale="Results filing detected. Reported figures and market expectations have not yet been compared, so result direction is not assessed."
        state="RESULT PUBLISHED"
    elif any(x in text for x in ("rescheduled","postponed","cancelled","canceled")):
        rationale="Schedule change detected. Verify the revised or cancelled date in the original exchange filing."
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
        rationale="Board meeting notice detected. Confirm the meeting date and agenda in the original exchange filing."
        state="BOARD MEETING NOTICE"
    else:
        direction="unclassified"; rationale="Insufficient structured evidence for a directional view."; state="NEW EVENT"
    return category,state,direction,rationale

def get_fno_company_map(allowed):
    url=NSE_ARCHIVE + "/content/equities/EQUITY_L.csv"
    try:
        with _session() as session:
            response=session.get(url,timeout=settings.nse_timeout); response.raise_for_status()
        mapping={}
        for row in csv.DictReader(io.StringIO(response.text)):
            symbol=(row.get("SYMBOL") or "").strip().upper()
            name=(row.get("NAME OF COMPANY") or row.get("NAME_OF_COMPANY") or "").strip()
            if symbol in allowed and name: mapping[symbol]=name
        return mapping
    except Exception as exc:
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
    dedupe_key=_dedupe_key(symbol,category,title,event_date)
    company = company_map.get(symbol) or KNOWN_COMPANY_NAMES.get(symbol) or symbol
    return {"event_id":hashlib.sha256(ident.encode()).hexdigest(),"dedupe_key":dedupe_key,"symbol":symbol,"company":company,"event_date":event_date,
      "category":category,"impact":direction,"headline":title or "NSE filing","details":details,
      "filed_at":filed,"source_url":url,"event_status":state,"rationale":rationale,
      "source_name":item.get("source_name","NSE RSS"),"feed_type":item.get("feed_type","")}
