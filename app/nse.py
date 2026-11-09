import csv, hashlib, io, logging, re, time, xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin
import requests
from .config import settings

log = logging.getLogger(__name__)
NSE_ARCHIVE = "https://nsearchives.nseindia.com"
FEEDS = {
    "announcements": NSE_ARCHIVE + "/content/RSS/Online_announcements.xml",
    "board_meetings": NSE_ARCHIVE + "/content/RSS/Board_Meetings.xml",
    "financial_results": NSE_ARCHIVE + "/content/RSS/Financial_Results.xml",
}

def _session():
    s=requests.Session()
    s.headers.update({"User-Agent":"NSE-Company-News-Monitor/1.0 (RSS reader; contact: operator)",
                      "Accept":"application/rss+xml, application/xml, text/xml, */*"})
    return s

def get_fno_symbols(session=None):
    # The exchange CSV is authoritative and changes when F&O constituents are
    # introduced/excluded. FNO_SYMBOLS is an additive override, not a permanent
    # replacement for the live list (which would silently omit new entrants).
    s=session or _session()
    try:
        r=s.get(settings.universe_csv_url,timeout=settings.nse_timeout)
        r.raise_for_status()
        reader=csv.DictReader(io.StringIO(r.text))
        symbols=set()
        for row in reader:
            for key,value in row.items():
                if key and key.strip().upper() in {"SYMBOL","UNDERLYING"} and value:
                    symbols.add(value.strip().upper()); break
        if not symbols:
            raise RuntimeError("F&O universe CSV parsed but contained no symbols")
        symbols.update(settings.fno_symbols)
        source="NSE F&O market-lot CSV"
        if settings.fno_symbols:
            source+=" + FNO_SYMBOLS additions"
        return symbols,source
    except Exception:
        if settings.fno_symbols:
            return set(settings.fno_symbols),"FNO_SYMBOLS environment fallback (NSE CSV unavailable)"
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

def fetch_all_feeds():
    session=_session(); output=[]; health={}
    try:
        for name,url in FEEDS.items():
            try:
                items=fetch_feed(session,url); health[name]={"ok":True,"items":len(items)}
                for x in items: x["feed_type"]=name; x["source_name"]="NSE RSS"
                output.extend(items)
            except Exception as exc:
                health[name]={"ok":False,"error":str(exc)}
                log.warning("NSE RSS %s failed: %s",name,exc)
    finally: session.close()
    if not output: raise RuntimeError("All official NSE RSS feeds failed: "+str(health))
    return output,health

def _date(value):
    if not value: return None
    raw=str(value).strip()
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


def _dedupe_key(symbol, category, headline):
    # Category may differ between NSE feeds for the same filing; key by symbol + title.
    normalized = re.sub(r"[^a-z0-9]+", " ", (headline or "").lower()).strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return hashlib.sha256(f"{symbol.upper()}|{normalized}".encode()).hexdigest()

def _classify(title,details,feed_type,event_date=None):
    text=(title+" "+details).lower()
    is_board = feed_type=="board_meetings" or "board meeting" in text or "meeting of the board" in text
    result_terms = ("financial results","quarterly results","audited results","unaudited results","standalone results","consolidated results")
    result_agenda = any(x in text for x in result_terms) and any(x in text for x in ("consider","approve","approval","to take on record","meeting"))
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
    elif any(x in text for x in ("rescheduled","postponed","cancelled","canceled")):
        direction="neutral"; rationale="Schedule change; verify the revised date in the source filing."; state="SCHEDULE CHANGE"
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
    dedupe_key=_dedupe_key(symbol,category,title)
    company = company_map.get(symbol) or KNOWN_COMPANY_NAMES.get(symbol) or symbol
    return {"event_id":hashlib.sha256(ident.encode()).hexdigest(),"dedupe_key":dedupe_key,"symbol":symbol,"company":company,"event_date":event_date,
      "category":category,"impact":direction,"headline":title or "NSE filing","details":details,
      "filed_at":filed,"source_url":url,"event_status":state,"rationale":rationale,
      "source_name":item.get("source_name","NSE RSS"),"feed_type":item.get("feed_type","")}
