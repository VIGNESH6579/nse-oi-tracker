import logging, threading, re, secrets, time
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI, Query, Header, HTTPException
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler
from .config import settings
from .store import Store
from .nse import FEEDS, fetch_all_feeds, get_fno_symbols, get_fno_company_map, normalize
from .notifier import send_many
from .finance import enrich_result_event
from .prices import format_quote, get_quotes

logging.basicConfig(level=logging.INFO)
log=logging.getLogger(__name__)
log.info("ntfy configured=%s",bool(settings.ntfy_topic))
store=Store(settings.database_path)
scheduler=BackgroundScheduler(timezone="Asia/Kolkata")
poll_lock=threading.Lock()
status={"last_poll":None,"last_success":None,"last_error":None,"sources":{},"new_events":0,"universe_source":"not checked","universe_count":0,"raw_feed_items":0,"matched_feed_items":0,"matched_symbols":[],"feed_match_stats":{},"scanner_state":"starting","rss_ok_count":0,"rss_total":len(FEEDS),"last_notification_success":None,"last_notification_count":0,"notification_error":None}
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
                if event.get("event_status")=="RESULT PUBLISHED":
                    existing=store.get_by_dedupe_key(event.get("dedupe_key"))
                    needs_doc=("xbrl" in str(event.get("source_url") or "").lower()) and (
                        not existing or not existing.get("analysis_json")
                    )
                    if needs_doc:
                        event=enrich_result_event(event)
                if store.insert(event):
                    new_count+=1
                elif event.get("event_status")=="RESULT PUBLISHED":
                    store.merge_result_fields(event)
            elif len(unmatched_sample)<8:
                unmatched_sample.append((feed,str(row.get("title") or "")[:150]))
        for pending_result in store.unpublished_result_analysis(limit=3):
            enriched=enrich_result_event(pending_result)
            store.merge_result_fields(enriched)
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
        # Publish one compact digest per polling cycle instead of one HTTP
        # request per filing. This materially reduces ntfy.sh rate-limit bursts.
        pending = store.pending_notifications(limit=3)
        if pending:
            if send_many(pending):
                for event in pending:
                    store.mark_notified(event["event_id"])
                status["last_notification_success"] = datetime.now(timezone.utc).isoformat()
                status["last_notification_count"] = len(pending)
                status["notification_error"] = None
            else:
                # Keep the queue intact on delivery failure; retry on a later poll.
                status["notification_error"] = "ntfy delivery failed or is rate-limited; priority events remain queued"
                log.warning("Keeping %d priority events queued after ntfy digest failure", len(pending))
        now=datetime.now(timezone.utc).isoformat()
        rss_ok_count=sum(
            1 for name in FEEDS
            if source_health.get(name, {}).get("ok")
            and not source_health.get(name, {}).get("stale")
        )
        poll_ok=bool(symbols) and rss_ok_count > 0
        scanner_state = (
            "healthy" if bool(symbols) and rss_ok_count == len(FEEDS)
            else ("degraded" if poll_ok else "unavailable")
        )
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
            scanner_state=scanner_state,
            rss_ok_count=rss_ok_count,
            rss_total=len(FEEDS),
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

@app.middleware("http")
async def prevent_stale_dashboard_cache(request, call_next):
    response = await call_next(request)
    if request.url.path == "/" or request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
    return response


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

def _analysis(event):
    raw=event.get("analysis_json")
    if not raw:
        return {}
    try:
        import json
        data=json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _age(value):
    if not value:
        return ""
    try:
        dt=datetime.fromisoformat(str(value).replace("Z","+00:00"))
        if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
        seconds=max(0,int((_now_ist()-dt.astimezone(_now_ist().tzinfo)).total_seconds()))
    except Exception:
        return ""
    if seconds < 90: return "just now"
    if seconds < 3600: return f"{seconds//60}m ago"
    if seconds < 86400: return f"{seconds//3600}h ago"
    return f"{seconds//86400}d ago"

def _card(event, quotes=None):
    quotes=quotes or {}
    symbol=str(event.get("symbol") or "")
    company=str(event.get("company") or "")
    if not company or company in (symbol,str(event.get("headline") or "")): company=symbol
    state=str(event.get("event_status") or "")
    labels={"RESULT SCHEDULED TODAY":"Results due today","RESULT SCHEDULED":"Results scheduled",
            "RESULT DATE NOT CONFIRMED":"Result agenda · date unclear","RESULT PUBLISHED":"Results out",
            "NEW EVENT":"Company update","SUDDEN EVENT":"SUDDEN",
            "BOARD MEETING NOTICE":"Board notice","ROUTINE":"Routine",
            "UPCOMING":"Board notice","SCHEDULE CHANGE":"Schedule change"}
    status_label=labels.get(state,state.replace("_"," ").title())
    filed=event.get("filed_at")
    analysis=_analysis(event)
    age=_age(filed or event.get("created_at"))
    if state=="RESULT PUBLISHED": date_line=("Out "+_display_time(filed) if filed else "Filing time not supplied")+(f" · {age}" if age and filed else "")
    elif event.get("event_date"):
        date_line="Due "+_display_event_date(event.get("event_date"))
        if filed: date_line+=" · Notice "+_display_time(filed)
    else: date_line=_display_time(filed)+(f" · {age}" if age else "")
    details=str(event.get("details") or "").strip()
    if len(details)>180: details=details[:177].rsplit(" ",1)[0]+"..."
    period=event.get("result_period") or analysis.get("result_period")
    statement=event.get("statement_type") or analysis.get("statement_type")
    extra=[]
    if period: extra.append(str(period))
    if statement and str(statement).upper() not in {"UNKNOWN",""}: extra.append(str(statement).title())
    if analysis.get("extraction_status")=="ok":
        if analysis.get("revenue_current_crore") is not None:
            yoy=analysis.get("revenue_yoy_pct")
            extra.append("Rev ₹"+f"{analysis['revenue_current_crore']:,.0f}"+" cr"+(f" YoY {yoy:+.1f}%" if yoy is not None else ""))
        if analysis.get("pat_current_crore") is not None:
            yoy=analysis.get("pat_yoy_pct")
            extra.append("PAT ₹"+f"{analysis['pat_current_crore']:,.0f}"+" cr"+(f" YoY {yoy:+.1f}%" if yoy is not None else ""))
        if analysis.get("eps") is not None:
            extra.append("EPS ₹"+f"{analysis['eps']:.2f}")
    impact_label,impact_class=_impact_label(event)
    searchable=_esc(" ".join(str(event.get(k) or "") for k in ("symbol","company","category","headline","details","event_status","result_period")))
    link=_esc(event.get("source_url") or "https://www.nseindia.com/")
    px=format_quote(quotes.get(symbol)).get("html")
    extra_html=(" · ".join(_esc(line) for line in extra))
    headline=event.get("headline") or "Company filing"
    if headline.upper().startswith(symbol.upper()):
        headline=headline
    return f"""<article class="event-card" data-search="{searchable}" data-symbol="{_esc(symbol)}">
      <div class="topline"><span class="symbol">{_esc(symbol)}</span><strong>{_esc(company)}</strong>{px}<span class="state state-{_esc(state.replace(' ','-').lower())}">{_esc(status_label)}</span></div>
      <h3>{_esc(headline)}</h3>
      {f'<p class="details">{_esc(details)}</p>' if details else ''}
      {f'<p class="metrics-line">{extra_html}</p>' if extra_html else ''}
      <p class="date">{_esc(date_line)}</p>
      <div class="impact"><span class="impact-{impact_class}">{_esc(impact_label)}</span></div>
      <div class="foot"><span>{_esc(event.get("source_name") or "NSE")}</span><a href="{link}" target="_blank" rel="noopener noreferrer">Filing ↗</a></div>
    </article>"""

def _section(title,subtitle,events,empty,anchor,quotes=None):
    cards="".join(_card(e, quotes) for e in events)
    return f"""<section class="section" id="{anchor}">
      <header><div><h2>{title}</h2><p>{subtitle}</p></div><span class="count">{len(events)}</span></header>
      <div class="cards">{cards or f'<div class="empty">{empty}</div>'}</div>
    </section>"""

def _dashboard_news_events(events):
    news=[event for event in events if event.get("event_status")=="SUDDEN EVENT"]
    news.sort(key=lambda e: str(e.get("filed_at") or e.get("created_at") or ""), reverse=True)
    return news

def _quote_tape(symbols, quotes):
    if not symbols:
        return '<div class="tape"><span class="tape-empty">No live names on the board yet.</span></div>'
    chips=[]
    for symbol in symbols:
        px=format_quote(quotes.get(symbol))
        chips.append(
            f'<span class="tape-item" data-symbol="{_esc(symbol)}"><b>{_esc(symbol)}</b>{px["html"]}</span>'
        )
    return '<div class="tape" id="quoteTape">'+"".join(chips)+"</div>"


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def dashboard():
    today=_now_ist().date().isoformat()
    recent=[e for e in store.list_events(500) if _is_recent_or_scheduled(e,today)]
    results_today=[e for e in recent if e.get("event_status")=="RESULT SCHEDULED TODAY" or
                   (e.get("event_status")=="RESULT SCHEDULED" and e.get("event_date")==today)]
    results_future=[e for e in recent if e.get("event_status")=="RESULT SCHEDULED" and e.get("event_date") and e.get("event_date")>today]
    results_future.sort(key=lambda e:(e.get("event_date") or "9999-12-31",e.get("symbol") or ""))
    results_published=[e for e in recent if e.get("event_status")=="RESULT PUBLISHED"]
    # Sudden material events are priority alerts and must also appear in
    # the dashboard, not only in the ntfy queue.
    news=_dashboard_news_events(recent)
    watch=results_today+results_published+news
    watch_symbols=[]
    seen_symbols=set()
    for event in watch:
        symbol=str(event.get("symbol") or "").upper()
        if symbol and symbol not in seen_symbols:
            seen_symbols.add(symbol)
            watch_symbols.append(symbol)
    quotes=get_quotes(watch_symbols)
    event_symbols=set(watch_symbols)
    feeds=[]
    for name,feed in status.get("sources",{}).items():
        ok=bool(feed.get("ok"))
        stale=bool(feed.get("stale"))
        count=int(feed.get("items",0))
        if stale:
            detail=f'{count} cached items · source temporarily unavailable'
            label,css="STALE","warn"
        elif ok:
            detail=f'{count} items received'
            label,css="OK","good"
        else:
            detail=str(feed.get("error") or "Source unavailable")
            label,css="Issue","bad"
        feeds.append(f'<div class="feed"><i class="dot {"green" if ok else "red"}"></i><div><b>{_esc(name.replace("_"," ").title())}</b><small>{_esc(detail)}</small></div><span class="badge {css}">{label}</span></div>')
    if not feeds: feeds.append('<div class="empty">Feed status will appear after the first poll.</div>')
    poll_time=_esc(_display_time(status.get("last_poll")))
    interval=max(1,round(settings.poll_interval/60))
    notification="ntfy alerts enabled" if settings.ntfy_topic else "ntfy alerts not configured"
    notify_class="good" if settings.ntfy_topic else "warn"
    error=_esc(status.get("last_error"))
    error_html=f'<div class="notice">{error}</div>' if error else ""
    tape=_quote_tape(watch_symbols, quotes)
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>F&O Scalp News</title><style>
:root{{--bg:#070b14;--card:#101827;--ink:#e8eef8;--muted:#8b9bb4;--line:#243049;--green:#22c55e;--red:#fb7185;--amber:#fbbf24;--blue:#7dd3fc}}
*{{box-sizing:border-box}}body{{margin:0;background:radial-gradient(1200px 500px at 10% -10%,#15233d,var(--bg));color:var(--ink);font:13px/1.45 "Segoe UI",system-ui,sans-serif}}a{{color:var(--blue);text-decoration:none}}.wrap{{max-width:1180px;margin:auto;padding:14px 14px 40px}}
.bar{{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:10px}}.brand{{display:flex;align-items:center;gap:8px;font-weight:800;letter-spacing:.02em}}.mark{{width:32px;height:32px;border-radius:8px;display:grid;place-items:center;background:#1d4ed8;color:#fff;font-weight:800}}.brand small{{display:block;color:var(--muted);font-size:10px;font-weight:600;letter-spacing:.12em;text-transform:uppercase}}.subtop{{color:var(--muted);font-size:11px}}
.hero{{background:linear-gradient(180deg,#15213a,#101827);border:1px solid var(--line);border-radius:14px;padding:16px 18px}}.eyebrow{{color:#93c5fd;font-size:10px;font-weight:800;letter-spacing:.12em;text-transform:uppercase}}h1{{margin:4px 0 6px;font-size:clamp(22px,4vw,30px);letter-spacing:-.04em}}.hero p{{margin:0;color:var(--muted);max-width:760px}}.chips{{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}}.chip,.badge,.count,.state{{border-radius:999px;padding:3px 8px;font-size:10px;font-weight:700;background:#1e293b;color:#cbd5e1}}
.good{{background:#052e1a;color:#4ade80}}.warn{{background:#3a2a05;color:#fbbf24}}.bad{{background:#3f0d16;color:#fb7185}}
.tape{{display:flex;gap:8px;overflow:auto;margin:10px 0;padding:8px;border:1px solid var(--line);border-radius:12px;background:#0b1220}}.tape-item{{flex:0 0 auto;display:flex;gap:8px;align-items:center;background:#162033;border:1px solid #243049;border-radius:10px;padding:7px 10px}}.tape-item b{{font-size:12px}}.tape-empty{{color:var(--muted);font-size:11px}}
.px{{font-variant-numeric:tabular-nums;display:inline-flex;gap:6px;align-items:baseline}}.px em{{font-style:normal;font-weight:800}}.px.up,.up{{color:var(--green)}}.px.down,.down{{color:var(--red)}}.px.flat,.flat,.px.na{{color:var(--muted)}}
nav{{display:flex;gap:6px;overflow:auto;position:sticky;top:0;background:#070b14ee;padding:8px 0;z-index:5}}nav a{{color:#9fb3cc;padding:6px 9px;border-radius:8px;font-size:11px;font-weight:700}}nav a:hover{{background:#1e293b;text-decoration:none}}
.metrics{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin:10px 0}}.metric,.event-card,.panel{{background:var(--card);border:1px solid var(--line);border-radius:12px}}.metric{{padding:10px 12px}}.metric label{{color:var(--muted);font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em}}.metric b{{display:block;font-size:26px;margin-top:2px}}.metric small{{color:var(--muted);font-size:10px}}
.section{{margin-top:18px}}.section header{{display:flex;justify-content:space-between;gap:8px;margin-bottom:8px}}h2{{margin:0;font-size:16px}}.section header p{{margin:2px 0 0;color:var(--muted);font-size:11px}}
.cards{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}}.event-card{{padding:12px}}.topline{{display:flex;align-items:center;gap:7px;flex-wrap:wrap}}.symbol{{background:#1d4ed8;color:#fff;border-radius:6px;padding:3px 7px;font-size:11px;font-weight:800}}.topline strong{{font-size:12px}}.topline .px{{margin-left:auto}}.state{{font-size:9px}}.state-sudden-event{{background:#3f0d16;color:#fb7185}}.state-result-published{{background:#052e1a;color:#4ade80}}.state-result-scheduled-today{{background:#1e3a5f;color:#93c5fd}}
.event-card h3{{font-size:13px;margin:8px 0 4px;line-height:1.35}}.details,.metrics-line{{margin:0;color:#9db0c9;font-size:11px}}.metrics-line{{color:#dbeafe;font-weight:700;margin-top:6px}}.date{{margin:7px 0;color:#c7d2fe;font-size:11px;font-weight:700}}
.impact span{{display:inline-block;border-radius:6px;padding:3px 7px;font-size:10px;font-weight:800}}.impact-positive{{background:#052e1a;color:#4ade80}}.impact-negative{{background:#3f0d16;color:#fb7185}}.impact-mixed{{background:#3a2a05;color:#fbbf24}}.impact-neutral{{background:#1e293b;color:#94a3b8}}
.foot{{display:flex;justify-content:space-between;gap:8px;border-top:1px solid var(--line);margin-top:8px;padding-top:7px;color:var(--muted);font-size:10px}}.foot a{{font-weight:800}}
.search input{{width:100%;background:#0b1220;border:1px solid var(--line);color:var(--ink);border-radius:10px;padding:9px 11px;font:inherit}}.search small{{color:var(--muted);font-size:10px}}
.panel{{padding:14px;margin-top:12px}}.feeds{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:8px}}.feed{{border:1px solid var(--line);border-radius:8px;padding:8px;display:flex;gap:7px}}.feed b{{display:block;font-size:10px}}.feed small{{display:block;color:var(--muted);font-size:9px;overflow-wrap:anywhere}}.dot{{width:7px;height:7px;border-radius:50%;margin-top:4px}}.green{{background:#22c55e}}.red{{background:#fb7185}}.notice,.note{{background:#1e293b;color:#cbd5e1;padding:8px 10px;border-radius:8px;font-size:10px;margin-top:8px}}.empty{{grid-column:1/-1;padding:16px;text-align:center;color:var(--muted);border:1px dashed #334155;border-radius:10px}}.hide{{display:none!important}}.footer{{display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;color:var(--muted);font-size:10px;margin-top:10px}}
@media(max-width:720px){{.metrics{{grid-template-columns:repeat(2,minmax(0,1fr))}}.cards,.feeds{{grid-template-columns:1fr}}.topline .px{{margin-left:0}}}}
</style></head><body><div class="wrap">
<div class="bar"><div class="brand"><span class="mark">N</span><div>F&O Scalp Tape<small>Results + sudden news only</small></div></div><span class="subtop">Scan {interval}m · {poll_time}</span></div>
<section class="hero"><div class="eyebrow">Trade the filing, not the noise</div><h1>Today’s actionable names</h1><p>Only published results and sudden material F&O events. Routine board notices, analyst meets and exchange boilerplate stay off this tape.</p><div class="chips"><span class="chip {_esc(notify_class)}">{_esc(notification)}</span><span class="chip {_esc("good" if status.get("scanner_state")=="healthy" else "warn" if status.get("scanner_state")=="degraded" else "bad")}">Scanner { _esc(str(status.get("scanner_state","starting")).upper()) } · {int(status.get("rss_ok_count",0))}/{int(status.get("rss_total",len(FEEDS)))} feeds</span><span class="chip">IST · delayed LTP</span></div></section>
{tape}
<nav><a href="#company-news">Sudden news</a><a href="#published-results">Results out</a><a href="#today-results">Results due</a><a href="#upcoming-results">Upcoming</a><a href="#data-status">Feeds</a></nav>
<div class="metrics">
<div class="metric"><label>Sudden news</label><b>{len(news)}</b><small>Order wins, penalties, shocks</small></div>
<div class="metric"><label>Results out</label><b>{len(results_published)}</b><small>With extracted figures when possible</small></div>
<div class="metric"><label>Results due today</label><b>{len(results_today)}</b><small>Agenda only — not yet published</small></div>
<div class="metric"><label>Names on tape</label><b>{len(event_symbols)}</b><small>Prices from delayed Yahoo quotes</small></div>
</div>
<div class="search"><input id="quickSearch" type="search" placeholder="Jump to a name — RELIANCE, COCHINSHIP, order"><small id="searchFeedback">Showing material F&O events only.</small></div>
{_section("Sudden material news","Order wins, plant shocks, penalties and other filings that can move an F&O name immediately. Routine announcements are hidden.",news,"No sudden material F&O event in the current window.","company-news",quotes)}
{_section("Results out","Extracted from official BSE iXBRL when the filing includes numbers. YoY is shown only if the document has a prior-year quarter.",results_published,"No published F&O results in the current window.","published-results",quotes)}
{_section("Results due today","Board said results would be considered today. This is not the print. Watch for the published filing.",results_today,"No F&O result agenda for today.","today-results",quotes)}
{_section("Upcoming result dates","Future result meetings only.",results_future,"No upcoming F&O result dates in recent notices.","upcoming-results",quotes)}
<section class="panel" id="data-status"><div><h2>Feed health</h2><p class="details">If NSE RSS is blocked, BSE still carries F&O result iXBRL. Quotes are delayed and not a broker feed.</p></div><div class="feeds">{"".join(feeds)}</div>{error_html}
<div class="note">Prices are delayed public quotes for context, not live order-book data. Never treat a headline as a guaranteed scalp.</div>
<div class="footer"><span>Last poll: {poll_time}</span><span><a href="/api/health">Technical status</a></span></div></section>
</div><script>(function(){{const s=document.getElementById("quickSearch"),f=document.getElementById("searchFeedback");if(s)s.addEventListener("input",function(){{const q=s.value.trim().toLowerCase();let n=0;document.querySelectorAll("[data-search]").forEach(function(x){{const ok=!q||(x.getAttribute("data-search")||"").toLowerCase().includes(q);x.classList.toggle("hide",!ok);if(ok)n++;}});f.textContent=q?("Showing "+n+" names."):"Showing material F&O events only.";}});
function paint(map){{document.querySelectorAll("[data-symbol]").forEach(function(node){{const q=map[node.getAttribute("data-symbol")];if(!q||q.ltp==null)return;const el=node.querySelector(".px");if(!el)return;const pct=q.change_pct||0,cls=pct>0?"up":pct<0?"down":"flat",chg=(pct>0?"+":"")+pct.toFixed(2)+"%";el.className="px "+cls;el.innerHTML="<em>₹"+Number(q.ltp).toLocaleString("en-IN",{{minimumFractionDigits:2,maximumFractionDigits:2}})+"</em> <b>"+chg+"</b>";}});}}
async function refreshQuotes(){{const symbols=[...new Set([...document.querySelectorAll("[data-symbol]")].map(x=>x.getAttribute("data-symbol")).filter(Boolean))];if(!symbols.length)return;try{{const r=await fetch("/api/quotes?symbols="+encodeURIComponent(symbols.join(",")));const j=await r.json();paint(j.quotes||{{}});}}catch(e){{}}}}
setInterval(refreshQuotes,30000);setTimeout(function(){{window.location.reload();}},{max(60,settings.poll_interval)*1000});}})();</script>
</body></html>"""

@app.get("/api/events")
def events(limit:int=Query(100,ge=1,le=500),symbol:str|None=None,status_filter:str|None=None):
    return {"events":store.list_events(limit,symbol,status_filter)}

@app.get("/api/quotes")
def quotes(symbols:str=""):
    wanted=[item.strip().upper() for item in symbols.split(",") if item.strip()][:40]
    return {"quotes":get_quotes(wanted),"delayed":True,"source":"Yahoo Finance"}

# UptimeRobot sends HEAD by default. Register HEAD explicitly so the monitor receives
# the same successful status as GET; ASGI suppresses the response body for HEAD.
@app.api_route("/api/health", methods=["GET", "HEAD"])
def health():
    try:
        database_events=store.count()
        database_error=None
    except Exception as exc:
        database_events=None
        database_error=str(exc)
    scanner=status["scanner_state"]
    overall="healthy" if scanner=="healthy" and not database_error else ("degraded" if scanner!="unavailable" else "unavailable")
    timestamp_ist=_now_ist().isoformat()
    try:
        pending=store.pending_count()
    except Exception:
        pending=None
    return {"ok":True,"status":overall,"service":"company-news-monitor","app":settings.app_name,
      "timestamp_ist":timestamp_ist,
      "database_events":database_events,
      "database_error":database_error,"poll_interval_seconds":settings.poll_interval,
      "ntfy_configured":bool(settings.ntfy_topic),
      "last_poll":status["last_poll"],"last_success":status["last_success"],
      "last_error":status["last_error"],"sources":status["sources"],"universe_source":status["universe_source"],
      "scanner_state":status["scanner_state"],"universe_count":status["universe_count"],
      "rss_ok_count":status["rss_ok_count"],"rss_total":status["rss_total"],
      "last_notification_success":status["last_notification_success"],
      "last_notification_count":status["last_notification_count"],
      "notification_error":status["notification_error"],
      "pending_notifications":pending,
      "database":{"status":"healthy" if database_error is None else "failing","persistent":False,
                  "note":"SQLite on Render Free is ephemeral and can reset after redeploy or spin-down."},
      "raw_feed_items":status["raw_feed_items"],"matched_feed_items":status["matched_feed_items"],
      "matched_symbols":status["matched_symbols"],"feed_match_stats":status["feed_match_stats"],
}

@app.get("/api/ready")
def readiness():
    from fastapi.responses import JSONResponse
    last_success=status["last_success"]
    try:
        last_success_age=max(0,int((datetime.now(timezone.utc)-datetime.fromisoformat(
            str(last_success).replace("Z","+00:00")
        )).total_seconds())) if last_success else None
    except (TypeError,ValueError):
        last_success_age=None
    fresh=last_success_age is not None and last_success_age <= max(900,settings.poll_interval*3)
    payload={"ready":status["scanner_state"]=="healthy" and fresh,
             "scanner_state":status["scanner_state"],
             "universe_count":status["universe_count"],
             "rss_ok_count":status["rss_ok_count"],"rss_total":status["rss_total"],
             "last_success":last_success,"last_success_age_seconds":last_success_age,
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
