import logging, threading, re, secrets
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI, Query, Header, HTTPException
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler
from .config import settings
from .store import Store
from .nse import fetch_all_feeds, get_fno_symbols, get_fno_company_map, normalize
from .notifier import send

logging.basicConfig(level=logging.INFO)
log=logging.getLogger(__name__)
log.info("ntfy configured=%s",bool(settings.ntfy_topic))
store=Store(settings.database_path)
scheduler=BackgroundScheduler(timezone="Asia/Kolkata")
poll_lock=threading.Lock()
status={"last_poll":None,"last_success":None,"last_error":None,"sources":{},"new_events":0,"universe_source":"not checked","universe_count":0,"raw_feed_items":0,"matched_feed_items":0,"matched_symbols":[],"feed_match_stats":{},"scanner_state":"starting"}
last_known_fno_symbols=set(store.load_fno_universe())

def _fallback_universe(configured, cached):
    reserved={"SYMBOL", "UNDERLYING", "CONTRACT"}
    def clean(values):
        return {
            symbol for value in (values or ())
            if (symbol := str(value).strip().upper())
            and symbol not in reserved
            and re.fullmatch(r"[A-Z0-9&-]{1,20}",symbol)
        }
    configured_symbols=clean(configured)
    cached_symbols=clean(cached)
    if configured_symbols and cached_symbols:
        return configured_symbols | cached_symbols, "FNO_SYMBOLS + last-known-good F&O universe fallback"
    if configured_symbols:
        return configured_symbols, "FNO_SYMBOLS environment fallback"
    if cached_symbols:
        return cached_symbols, "last-known-good F&O universe fallback"
    return set(), "unavailable"

def poll_once():
    global last_known_fno_symbols
    if not poll_lock.acquire(blocking=False): return {"skipped":True,"reason":"poll already running"}
    try:
        poll_errors=[]
        try:
            symbols,universe_source=get_fno_symbols()
            if symbols:
                last_known_fno_symbols=set(symbols)
                try:
                    store.save_fno_universe(symbols)
                except Exception as cache_exc:
                    log.warning("Could not persist last-known-good F&O universe: %s",cache_exc)
        except Exception as exc:
            symbols,universe_source=_fallback_universe(settings.fno_symbols,last_known_fno_symbols)
            poll_errors.append("F&O universe: "+str(exc))
            log.warning("F&O universe fetch failed; fallback=%s symbols=%d error=%s",
                        universe_source,len(symbols),exc)
        log.info("F&O universe source=%s symbols=%d",universe_source,len(symbols))
        company_map=get_fno_company_map(symbols) if symbols else {}
        unmapped_symbols=sorted(set(symbols)-set(company_map))
        if unmapped_symbols:
            log.warning("F&O symbols absent from NSE equity company master: %s",unmapped_symbols)
        if symbols and not company_map:
            log.warning("F&O company-name map is empty; symbol-only matching will miss name-only exchange filings")
        rows=[]
        source_health={}
        if symbols:
            try:
                rows,source_health=fetch_all_feeds(symbols,company_map)
            except Exception as exc:
                source_health={"all_feeds":{"ok":False,"error":str(exc)}}
                poll_errors.append("NSE feeds: "+str(exc))
                log.warning("NSE feeds unavailable; continuing market-data poll: %s",exc)
        else:
            source_health={"all_feeds":{"ok":False,"error":"No F&O symbols available; configure FNO_SYMBOLS"}}
        new_count=0
        matched_count=0
        matched_symbols=set()
        raw_by_feed=Counter()
        matched_by_feed=Counter()
        unmatched_sample=[]
        for row in rows:
            feed=str(row.get("source_name") or row.get("feed_type") or "unknown")
            raw_by_feed[feed]+=1
            event=normalize(row,symbols,company_map)
            if event:
                matched_count+=1
                matched_symbols.add(event["symbol"])
                matched_by_feed[feed]+=1
                if store.insert(event): new_count+=1
            elif len(unmatched_sample)<8:
                unmatched_sample.append((feed,str(row.get("title") or "")[:150]))
        feed_match_stats={
            name:{"raw":count,"matched":matched_by_feed.get(name,0)}
            for name,count in sorted(raw_by_feed.items())
        }
        log.info(
            "NSE filing matcher: universe=%d company_map=%d raw_feed_items=%d matched_items=%d unmatched_items=%d matched_symbols=%s feed_stats=%s",
            len(symbols),len(company_map),len(rows),matched_count,max(0,len(rows)-matched_count),
            ",".join(sorted(matched_symbols)) or "none",feed_match_stats
        )
        if unmatched_sample:
            log.info("NSE filing unmatched sample (feed, title): %s",unmatched_sample)
        for pending in store.pending_notifications(limit=50):
            if send(pending): store.mark_notified(pending["event_id"])
        now=datetime.now(timezone.utc).isoformat()
        source_ok=any(bool(feed.get("ok")) for feed in source_health.values())
        poll_ok=bool(symbols) and source_ok
        for name,feed in source_health.items():
            if not feed.get("ok"):
                poll_errors.append(f"{name}: {feed.get('error') or 'source unavailable'}")
        status.update(
            last_poll=now,
            last_success=now if poll_ok else status["last_success"],
            last_error="; ".join(poll_errors) or None,
            sources=source_health,
            new_events=new_count,
            universe_source=universe_source,
            universe_count=len(symbols),
            raw_feed_items=len(rows),
            matched_feed_items=matched_count,
            matched_symbols=sorted(matched_symbols),
            feed_match_stats=feed_match_stats,
            scanner_state="healthy" if poll_ok else "unavailable",
        )
        return {"received":len(rows),"matched":matched_count,"new_events":new_count,
                "sources":source_health,"scanner_state":status["scanner_state"],
                "errors":poll_errors}
    except Exception as exc:
        now=datetime.now(timezone.utc).isoformat()
        status.update(last_poll=now,last_error=str(exc))
        log.exception("Event poll failed")
        return {"error":str(exc),"sources":status["sources"]}
    finally: poll_lock.release()

@asynccontextmanager
async def lifespan(app:FastAPI):
    scheduler.add_job(poll_once,"interval",seconds=settings.poll_interval,id="event-poll",max_instances=1,coalesce=True)
    scheduler.start()
    threading.Thread(target=poll_once,daemon=True).start()
    yield
    scheduler.shutdown(wait=False)

app=FastAPI(title=settings.app_name,version="2.0.0",lifespan=lifespan)

def _esc(v):
    import html
    return html.escape(str(v or ""), quote=True)

def _now_ist():
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Asia/Kolkata"))

def _display_time(value):
    if not value: return "Date/time not supplied by exchange feed"
    raw=str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}",raw):
        return _display_event_date(raw)+" · time not supplied by exchange feed"
    try:
        from zoneinfo import ZoneInfo
        dt=datetime.fromisoformat(raw.replace("Z","+00:00"))
        if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ZoneInfo("Asia/Kolkata")).strftime("%d %b %Y · %I:%M %p IST")
    except Exception: return raw

def _display_event_date(value):
    try: return datetime.strptime(str(value)[:10],"%Y-%m-%d").strftime("%d %b %Y")
    except Exception: return "Date not identified in filing"

def _is_recent_or_scheduled(event,today):
    event_date=str(event.get("event_date") or "")
    if event_date and event_date>=today: return True
    stamp=event.get("filed_at") or event.get("created_at")
    if not stamp: return False
    try:
        dt=datetime.fromisoformat(str(stamp).replace("Z","+00:00"))
        if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
        return 0 <= (_now_ist()-dt.astimezone(_now_ist().tzinfo)).total_seconds() <= 72*3600
    except Exception: return False

def _impact_label(event):
    impact=str(event.get("impact") or "unclassified").lower()
    if event.get("event_status")=="RESULT PUBLISHED":
        if impact=="bullish": return "Result view: Potentially positive","positive"
        if impact=="bearish": return "Result view: Potentially negative","negative"
        if impact=="mixed": return "Result view: Mixed","mixed"
        return "Result direction: Not assessed","neutral"
    return ({"bullish":("Potentially positive","positive"),"bearish":("Potentially negative","negative"),
             "mixed":("Mixed indicators","mixed")}.get(impact,("Impact not assessed","neutral")))

def _card(event):
    symbol=str(event.get("symbol") or "")
    company=str(event.get("company") or "")
    if not company or company in (symbol,str(event.get("headline") or "")): company=symbol
    state=str(event.get("event_status") or "")
    labels={"RESULT SCHEDULED TODAY":"Results scheduled today","RESULT SCHEDULED":"Results scheduled",
            "RESULT DATE NOT CONFIRMED":"Result agenda · date unclear","RESULT PUBLISHED":"Results published",
            "NEW EVENT":"New company announcement","BOARD MEETING NOTICE":"Board meeting notice",
            "UPCOMING":"Board meeting notice","SCHEDULE CHANGE":"Schedule changed"}
    status_label=labels.get(state,state.replace("_"," ").title())
    filed=event.get("filed_at")
    if state=="RESULT PUBLISHED": date_line="Published on: "+_display_time(filed)
    elif event.get("event_date"):
        date_line="Scheduled date: "+_display_event_date(event.get("event_date"))
        if filed: date_line+=" · Notice filed: "+_display_time(filed)
    else: date_line="Exchange filing date/time: "+_display_time(filed)
    details=str(event.get("details") or "").strip()
    if len(details)>260: details=details[:257].rsplit(" ",1)[0]+"..."
    impact_label,impact_class=_impact_label(event)
    searchable=_esc(" ".join(str(event.get(k) or "") for k in ("symbol","company","category","headline","details","event_status")))
    link=_esc(event.get("source_url") or "https://www.nseindia.com/")
    return f"""<article class="event-card" data-search="{searchable}">
      <div class="topline"><span class="symbol">{_esc(symbol)}</span><strong>{_esc(company)}</strong><span class="state">{_esc(status_label)}</span></div>
      <h3>{_esc(event.get("headline") or "Company filing")}</h3>
      {f'<p class="details">{_esc(details)}</p>' if details else ''}
      <p class="date">{_esc(date_line)}</p>
      <div class="impact"><span class="impact-{impact_class}">{_esc(impact_label)}</span></div>
      <div class="foot"><span>{_esc(event.get("source_name") or "NSE")}</span><a href="{link}" target="_blank" rel="noopener noreferrer">Open original filing ↗</a></div>
    </article>"""

def _section(title,subtitle,events,empty,anchor):
    cards="".join(_card(e) for e in events)
    return f"""<section class="section" id="{anchor}">
      <header><div><h2>{title}</h2><p>{subtitle}</p></div><span class="count">{len(events)}</span></header>
      <div class="cards">{cards or f'<div class="empty">{empty}</div>'}</div>
    </section>"""

@app.get("/",response_class=HTMLResponse)
def dashboard():
    today=_now_ist().date().isoformat()
    recent=[e for e in store.list_events(500) if _is_recent_or_scheduled(e,today)]
    results_today=[e for e in recent if e.get("event_status")=="RESULT SCHEDULED TODAY" or
                   (e.get("event_status")=="RESULT SCHEDULED" and e.get("event_date")==today)]
    results_future=[e for e in recent if e.get("event_status")=="RESULT SCHEDULED" and e.get("event_date") and e.get("event_date")>today]
    results_future.sort(key=lambda e:(e.get("event_date") or "9999-12-31",e.get("symbol") or ""))
    results_published=[e for e in recent if e.get("event_status")=="RESULT PUBLISHED"]
    news=[e for e in recent if e.get("event_status")=="NEW EVENT"]
    board=[e for e in recent if e.get("event_status") in ("BOARD MEETING NOTICE","UPCOMING","RESULT DATE NOT CONFIRMED","SCHEDULE CHANGE")]
    event_symbols={str(e.get("symbol") or "").upper() for e in recent if e.get("symbol")}
    feeds=[]
    for name,feed in status.get("sources",{}).items():
        ok=bool(feed.get("ok"))
        detail=f'{int(feed.get("items",0))} items received' if ok else str(feed.get("error") or "Source unavailable")
        feeds.append(f'<div class="feed"><i class="dot {"green" if ok else "red"}"></i><div><b>{_esc(name.replace("_"," ").title())}</b><small>{_esc(detail)}</small></div><span class="badge {"good" if ok else "bad"}">{"OK" if ok else "Issue"}</span></div>')
    if not feeds: feeds.append('<div class="empty">Feed status will appear after the first poll.</div>')
    poll_time=_esc(_display_time(status.get("last_poll")))
    interval=max(1,round(settings.poll_interval/60))
    notification="ntfy alerts enabled" if settings.ntfy_topic else "ntfy alerts not configured"
    notify_class="good" if settings.ntfy_topic else "warn"
    error=_esc(status.get("last_error"))
    error_html=f'<div class="notice">{error}</div>' if error else ""
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Intraday Company News Watch</title><style>
:root{{--bg:#f4f6fa;--white:#fff;--ink:#172235;--muted:#68778b;--line:#e3e9f0;--blue:#2459a6;--green:#17794c;--red:#b42318;--amber:#936000}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}}a{{color:var(--blue);text-decoration:none}}a:hover{{text-decoration:underline}}.wrap{{max-width:1120px;margin:auto;padding:18px 16px 45px}}
.brand{{display:flex;align-items:center;gap:9px;font-weight:750}}.mark{{background:#1d4f91;color:white;border-radius:9px;width:34px;height:34px;display:grid;place-items:center}}.brand small{{display:block;font-size:10px;color:var(--muted);font-weight:500;text-transform:uppercase}}.bar{{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;gap:10px}}.muted{{color:var(--muted)}}.subtop{{font-size:11px;color:var(--muted)}}
.hero,.panel,.metric,.event-card{{background:var(--white);border:1px solid var(--line);border-radius:13px}}.hero{{padding:20px 22px}}.eyebrow{{font-size:10px;color:var(--blue);font-weight:800;letter-spacing:.1em;text-transform:uppercase}}h1{{font-size:clamp(24px,4vw,32px);margin:5px 0;letter-spacing:-.04em}}.hero p{{color:var(--muted);margin:0;max-width:760px}}.chips{{display:flex;flex-wrap:wrap;gap:7px;margin-top:13px}}.chip,.badge,.count,.state{{border-radius:30px;padding:4px 8px;font-size:10px;font-weight:700;background:#eff2f6;color:#536276}}
.good{{background:#eaf7f0;color:var(--green)}}.warn{{background:#fff6df;color:var(--amber)}}.bad{{background:#fff0ef;color:var(--red)}}.muted-badge{{background:#eff2f6;color:#536276}}
nav{{display:flex;gap:5px;overflow:auto;position:sticky;top:0;background:#f4f6faef;padding:10px 0;z-index:5}}nav a{{white-space:nowrap;padding:6px 9px;border-radius:7px;font-size:11px;color:#46566d}}nav a:hover{{background:#e7edf6;text-decoration:none}}
.metrics{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px;margin:12px 0}}.metric{{padding:11px 13px}}.metric label{{font-size:11px;color:var(--muted);font-weight:650}}.metric b{{display:block;font-size:24px;line-height:1.2;margin-top:3px}}.metric small{{font-size:10px;color:var(--muted)}}
.section{{margin-top:22px}}.section header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:9px;gap:10px}}h2{{font-size:17px;margin:0;letter-spacing:-.02em}}.section header p{{font-size:11px;color:var(--muted);margin:2px 0 0}}.cards{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}}
.event-card{{padding:13px 14px;min-width:0}}.topline{{display:flex;align-items:center;gap:7px;flex-wrap:wrap}}.symbol{{background:#edf4ff;color:#1d4f91;border-radius:5px;padding:3px 7px;font-size:10px;font-weight:800}}.topline strong{{font-size:12px}}.state{{margin-left:auto;font-size:9px}}.event-card h3{{font-size:13px;line-height:1.4;margin:9px 0 5px}}.details{{font-size:11px;color:#526176;margin:0}}.date{{font-size:11px;color:#40536b;font-weight:700;margin:8px 0}}.impact{{display:flex;flex-wrap:wrap;gap:7px;align-items:center;margin-top:8px}}.impact-positive{{background:#eaf7f0;color:var(--green)}}.impact-negative{{background:#fff0ef;color:var(--red)}}.impact-mixed{{background:#fff6df;color:var(--amber)}}.impact-neutral{{background:#eff2f6;color:#536276}}.impact span{{font-size:10px;font-weight:750;border-radius:6px;padding:4px 7px}}.market{{font-size:10px;color:#334b67;background:#f4f7fb;border:1px solid #e6edf5;border-radius:6px;padding:4px 7px}}.foot{{display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;border-top:1px solid var(--line);padding-top:7px;margin-top:9px;font-size:10px;color:var(--muted)}}.foot a{{font-weight:700}}
.panel{{padding:15px;margin-top:12px}}.panel-head{{display:flex;justify-content:space-between;gap:10px;align-items:center}}.panel-head p{{font-size:11px;color:var(--muted);margin:3px 0}}.explain{{font-size:11px;color:var(--muted);margin:8px 0}}.table-wrap{{overflow:auto;border:1px solid var(--line);border-radius:8px}}table{{width:100%;border-collapse:collapse;min-width:680px}}th,td{{font-size:11px;text-align:left;padding:8px 10px;border-bottom:1px solid var(--line);white-space:nowrap}}th{{background:#f7f9fc;color:#64748b;text-transform:uppercase;letter-spacing:.05em;font-size:9px}}tr:last-child td{{border-bottom:0}}td small{{display:block;font-size:9px;color:var(--muted)}}.up{{color:var(--green);font-weight:750}}.down{{color:var(--red);font-weight:750}}.flat{{color:var(--muted)}}.empty{{padding:18px;text-align:center;color:var(--muted);font-size:11px;grid-column:1/-1;border:1px dashed #ccd5e1;border-radius:9px}}.empty-row{{text-align:center;white-space:normal}}.note{{margin-top:9px;padding:9px 11px;border-left:3px solid #8caed8;background:#f7f9fc;color:#526176;font-size:10px}}.feeds{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:9px}}.feed{{border:1px solid var(--line);border-radius:8px;padding:8px;display:flex;align-items:flex-start;gap:7px;min-width:0}}.feed div{{flex:1;min-width:0}}.feed b{{display:block;font-size:10px}}.feed small{{display:block;font-size:9px;color:var(--muted);overflow-wrap:anywhere}}.dot{{width:7px;height:7px;border-radius:50%;margin-top:4px;flex-shrink:0}}.green{{background:#219566}}.red{{background:#d54d45}}.notice{{background:#fff6df;color:#75500b;padding:9px;border-radius:8px;font-size:10px;margin-top:9px}}.search{{margin:14px 0 0}}input{{width:100%;padding:9px 11px;border:1px solid #d4dce7;border-radius:8px;font:inherit;font-size:11px}}.search small{{font-size:10px;color:var(--muted)}}.hide{{display:none!important}}.footer{{font-size:10px;color:var(--muted);margin-top:12px;display:flex;gap:8px;justify-content:space-between;flex-wrap:wrap}}
@media(max-width:720px){{.metrics{{grid-template-columns:repeat(2,minmax(0,1fr))}}.cards{{grid-template-columns:1fr}}.feeds{{grid-template-columns:1fr}}.wrap{{padding:12px 10px 35px}}.hero{{padding:16px}}}}
</style></head><body><div class="wrap">
<div class="bar"><div class="brand"><span class="mark">N</span><div>Intraday News Watch<small>NSE F&O company monitor</small></div></div><span class="subtop">Poll: {interval} min · Last check: {poll_time}</span></div>
<section class="hero"><div class="eyebrow">Stock-specific exchange filings only</div><h1>Today’s Company Events</h1><p>Results expected today, results already published, and fresh company announcements — for NSE F&O companies.</p><div class="chips"><span class="chip {_esc(notify_class)}">{_esc(notification)}</span><span class="chip">All dates / times in IST</span></div></section>
<nav><a href="#today-results">Results today</a><a href="#published-results">Published results</a><a href="#company-news">Fresh news</a><a href="#upcoming-results">Upcoming results</a><a href="#data-status">Data status</a></nav>
<div class="metrics">
<div class="metric"><label>Results scheduled today</label><b>{len(results_today)}</b><small>From board meeting notices</small></div>
<div class="metric"><label>Results published</label><b>{len(results_published)}</b><small>Exchange result filings</small></div>
<div class="metric"><label>Fresh company news</label><b>{len(news)}</b><small>Recent F&O announcements</small></div>
<div class="metric"><label>News-linked stocks</label><b>{len(event_symbols)}</b><small>Unique F&O companies in recent filings</small></div>
</div>
<div class="search"><input id="quickSearch" type="search" placeholder="Search company, symbol or headline — e.g. RELIANCE, results, order"><small id="searchFeedback">Filters company events and filings.</small></div>
{_section("Results scheduled for today","Board notices indicate that results are on today’s agenda. The filing may not state the exact release time.",results_today,"No result-related board meeting is scheduled for today in the collected filings.","today-results")}
{_section("Results published","Publication time is taken from the exchange feed and displayed in IST. Result direction is not guessed without analysing the reported numbers.",results_published,"No results filings detected in the recent feed window.","published-results")}
{_section("Fresh company news","Every card shows the NSE feed filing date and time in IST when supplied by the exchange; missing times are never guessed.",news,"No recent company-specific announcements found.","company-news")}
{_section("Upcoming result dates","Future results-related board meetings identified from exchange notices.",results_future,"No future result dates were identified in recent notices.","upcoming-results")}
{_section("Other board meeting notices","Board meeting filings without a confirmed results agenda. Open the original filing to verify the date and agenda.",board,"No other recent board meeting notices found.","other-board")}
<section class="panel" id="data-status"><div class="panel-head"><div><h2>Data status</h2><p>Check this when a result, announcement or share price is missing.</p></div></div><div class="feeds">{"".join(feeds)}</div>{error_html}
<div class="note">A scheduled results notice is not a published result. A result’s direction is left unassessed until the financial figures can be compared.</div>
<div class="footer"><span>Last poll: {poll_time}</span><span>Exchange publication time may differ from the instant a filing became visible to every investor.</span><span><a href="/api/health">Technical status</a></span></div></section>
</div><script>(function(){{const s=document.getElementById("quickSearch"),f=document.getElementById("searchFeedback");if(!s)return;s.addEventListener("input",function(){{const q=s.value.trim().toLowerCase();let n=0;document.querySelectorAll("[data-search]").forEach(function(x){{const ok=!q||(x.getAttribute("data-search")||"").toLowerCase().includes(q);x.classList.toggle("hide",!ok);if(ok)n++;}});f.textContent=q?("Showing "+n+" matching company events."):"Filters company events and filings.";}});setTimeout(function(){{window.location.reload();}},{max(60,settings.poll_interval)*1000});}})();</script>
</body></html>"""

@app.get("/api/events")
def events(limit:int=Query(100,ge=1,le=500),symbol:str|None=None,status_filter:str|None=None):
    return {"events":store.list_events(limit,symbol,status_filter)}

# UptimeRobot sends HEAD by default. Register HEAD explicitly so the monitor receives
# the same successful status as GET; ASGI suppresses the response body for HEAD.
@app.api_route("/api/health", methods=["GET", "HEAD"])
def health():
    return {"ok":True,"app":settings.app_name,"database_events":store.count(),
      "poll_interval_seconds":settings.poll_interval,"ntfy_configured":bool(settings.ntfy_topic),
      "last_poll":status["last_poll"],"last_success":status["last_success"],
      "last_error":status["last_error"],"sources":status["sources"],"universe_source":status["universe_source"],
      "scanner_state":status["scanner_state"],"universe_count":status["universe_count"],
      "raw_feed_items":status["raw_feed_items"],"matched_feed_items":status["matched_feed_items"],
      "matched_symbols":status["matched_symbols"],"feed_match_stats":status["feed_match_stats"],
}

@app.get("/api/ready")
def readiness():
    from fastapi.responses import JSONResponse
    payload={"ready":status["scanner_state"]=="healthy",
             "scanner_state":status["scanner_state"],
             "universe_count":status["universe_count"],
             "last_success":status["last_success"],
             "sources":status["sources"]}
    return JSONResponse(payload,status_code=200 if payload["ready"] else 503)

@app.post("/api/sync")
def sync(x_sync_token: str | None = Header(default=None, alias="X-Sync-Token")):
    # Fail closed: when SYNC_TOKEN is unset, public manual triggering is disabled.
    if not settings.sync_token or not x_sync_token or not secrets.compare_digest(
        settings.sync_token, x_sync_token
    ):
        raise HTTPException(status_code=404, detail="Not found")
    return poll_once()
