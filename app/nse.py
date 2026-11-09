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
    if settings.fno_symbols:
        return set(settings.fno_symbols), "FNO_SYMBOLS environment"
    s=session or _session()
    r=s.get(settings.universe_csv_url,timeout=settings.nse_timeout); r.raise_for_status()
    reader=csv.DictReader(io.StringIO(r.text))
    symbols=set()
    for row in reader:
        for key,value in row.items():
            if key and key.strip().upper() in {"SYMBOL","UNDERLYING"} and value:
                symbols.add(value.strip().upper()); break
    if not symbols: raise RuntimeError("F&O universe CSV parsed but contained no symbols; set FNO_SYMBOLS")
    return symbols,"NSE F&O market-lot CSV"

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
    try: return parsedate_to_datetime(value).astimezone(timezone.utc).isoformat()
    except Exception:
        for fmt in ("%d-%b-%Y","%d-%m-%Y","%d/%m/%Y","%Y-%m-%d"):
            try: return datetime.strptime(value.strip(),fmt).replace(tzinfo=timezone.utc).isoformat()
            except ValueError: pass
    return None

def _classify(title,details,feed_type):
    text=(title+" "+details).lower()
    if feed_type=="board_meetings" or "board meeting" in text: category="Board Meeting"
    elif feed_type=="financial_results" or any(x in text for x in ("financial results","quarterly results","audited results","unaudited results")): category="Financial Results"
    elif any(x in text for x in ("dividend","record date","split","bonus issue")): category="Corporate Action"
    elif any(x in text for x in ("order","letter of award","work order","contract")): category="Order / Contract"
    elif any(x in text for x in ("resignation","appointed as","ceo","managing director","key managerial")): category="Management Change"
    elif any(x in text for x in ("sebi","nclt","court","penalty","investigation","show cause","default")): category="Regulatory / Legal"
    elif any(x in text for x in ("acquisition","merger","amalgamation","buyback","fund raising","qip","rights issue","credit rating")): category="Corporate Action"
    else: category="Corporate Announcement"
    is_result=category=="Financial Results" and any(x in text for x in ("result","financial","quarter","audited","unaudited"))
    if is_result:
        direction="neutral"; rationale="Results filing detected; detailed financial figures and expectations are not yet compared."
        state="RESULT PUBLISHED"
    elif feed_type=="board_meetings" or any(x in text for x in ("proposed meeting date","board meeting to be held","board meeting scheduled")):
        direction="neutral"; rationale="Scheduled corporate event. The event date must be checked against the filing; no price direction is implied."
        state="UPCOMING"
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
        normalized=re.sub(r"[^A-Z0-9]","",hay)
        matches=[]
        for candidate,name in company_map.items():
            alias=re.sub(r"[^A-Z0-9]","",name.upper())
            if len(alias)>=8 and alias in normalized: matches.append(candidate)
        if len(matches)==1: symbol=matches[0]
    if not symbol: return None
    filed=_date(item.get("published"))
    category,state,direction,rationale=_classify(title,details,item.get("feed_type",""))
    url=item.get("link") or "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    ident="|".join((symbol,filed or "",title,url))
    return {"event_id":hashlib.sha256(ident.encode()).hexdigest(),"symbol":symbol,"company":title,
      "category":category,"impact":direction,"headline":title or "NSE filing","details":details,
      "filed_at":filed,"source_url":url,"event_status":state,"rationale":rationale,
      "source_name":item.get("source_name","NSE RSS"),"feed_type":item.get("feed_type","")}
