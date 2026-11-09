import logging, threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler
from .config import settings
from .store import Store
from .nse import fetch_all_feeds, get_fno_symbols, get_fno_company_map, normalize
from .notifier import send
from .market_data import angel_market_data

logging.basicConfig(level=logging.INFO)
log=logging.getLogger(__name__)
store=Store(settings.database_path)
scheduler=BackgroundScheduler(timezone="Asia/Kolkata")
poll_lock=threading.Lock()
status={"last_poll":None,"last_success":None,"last_error":None,"sources":{},"new_events":0,"universe_source":"not checked","market_data":{"state":"not checked","quotes":0,"last_quote_at":None,"error":None}}

def poll_once():
    if not poll_lock.acquire(blocking=False): return {"skipped":True,"reason":"poll already running"}
    try:
        poll_errors=[]
        try:
            symbols,universe_source=get_fno_symbols()
        except Exception as exc:
            symbols=set(settings.fno_symbols)
            universe_source="FNO_SYMBOLS environment fallback" if symbols else "unavailable"
            poll_errors.append("F&O universe: "+str(exc))
            log.warning("F&O universe fetch failed: %s",exc)
        company_map=get_fno_company_map(symbols) if symbols else {}
        rows=[]
        source_health={}
        if symbols:
            try:
                rows,source_health=fetch_all_feeds()
            except Exception as exc:
                source_health={"all_feeds":{"ok":False,"error":str(exc)}}
                poll_errors.append("NSE feeds: "+str(exc))
                log.warning("NSE feeds unavailable; continuing market-data poll: %s",exc)
        else:
            source_health={"all_feeds":{"ok":False,"error":"No F&O symbols available; configure FNO_SYMBOLS"}}
        new_count=0
        for row in rows:
            event=normalize(row,symbols,company_map)
            if event and store.insert(event): new_count+=1
        # Read-only market snapshot. This adapter has no order/modify/cancel methods.
        # Only LTP, OHLC, percentage change and volume are persisted; OI is discarded.
        if not settings.angel_market_data_enabled:
            status["market_data"]={"state":"disabled","quotes":0,"last_quote_at":None,"error":None}
        elif not angel_market_data.configured:
            status["market_data"]={"state":"not_configured","quotes":0,"last_quote_at":None,
                "error":"Set ANGEL_ONE_API_KEY, ANGEL_ONE_CLIENT_CODE, ANGEL_ONE_PASSWORD and ANGEL_ONE_TOTP_SECRET in Render"}
        else:
            try:
                # Fetch only shares with recent company filings / scheduled result notices.
                # NIFTY 50 is added internally by the read-only adapter as a benchmark.
                quote_symbols=store.symbols_with_recent_news(days=3)
                quotes=angel_market_data.quotes(quote_symbols) if quote_symbols else {}
                saved=store.save_market_quotes(quotes)
                status["market_data"]={"state":"ok" if saved else "no_quotes",
                    "quotes":saved,"last_quote_at":max((q["fetched_at"] for q in quotes.values()),default=None),"error":None}
            except Exception as exc:
                status["market_data"]={"state":"error","quotes":0,"last_quote_at":None,"error":str(exc)[:240]}
                poll_errors.append("Angel One: "+str(exc))
                log.warning("Angel One read-only market-data poll failed: %s",exc)
        for pending in store.pending_notifications(limit=50):
            if send(pending): store.mark_notified(pending["event_id"])
        now=datetime.now(timezone.utc).isoformat()
        status.update(last_poll=now,last_success=now,last_error="; ".join(poll_errors) or None,
                      sources=source_health,new_events=new_count,universe_source=universe_source)
        return {"received":len(rows),"new_events":new_count,"sources":source_health,
                "market_data":status["market_data"],"errors":poll_errors}
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

def _display_time(value):
    if not value:
        return "Time not supplied"
    try:
        from zoneinfo import ZoneInfo
        dt=datetime.fromisoformat(str(value).replace("Z","+00:00"))
        if dt.tzinfo is None:
            dt=dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ZoneInfo("Asia/Kolkata")).strftime("%d %b %Y, %I:%M %p IST")
    except Exception:
        return str(value)

def _event_labels(event):
    state=str(event.get("event_status") or "").upper()
    labels={
        "UPCOMING":"Board meeting notice",
        "RESULT PUBLISHED":"Results filing detected",
        "NEW EVENT":"New company announcement",
        "SCHEDULE CHANGE":"Schedule change",
    }
    impact=str(event.get("impact") or "unclassified").lower()
    impact_labels={
        "bullish":"Potentially positive",
        "bearish":"Potentially negative",
        "neutral":"Impact not assessed",
        "mixed":"Mixed indicators",
        "unclassified":"Impact not assessed",
    }
    return labels.get(state,state.replace("_"," ").title()), impact_labels.get(impact,"Impact not assessed"), impact

def _card(event):
    event_type, impact_label, impact = _event_labels(event)
    symbol=_esc(event.get("symbol"))
    category=_esc(event.get("category"))
    headline=_esc(event.get("headline") or "Company filing")
    details=_esc(event.get("details"))
    rationale=_esc(event.get("rationale") or "No impact assessment is available.")
    filed=_esc(_display_time(event.get("filed_at")))
    source=_esc(event.get("source_name") or "Exchange filing")
    source_url=_esc(event.get("source_url") or "https://www.nseindia.com/")
    searchable=_esc(" ".join(str(event.get(k) or "") for k in ("symbol","category","headline","details","event_status")))
    if impact=="bullish":
        impact_class="impact-positive"
    elif impact=="bearish":
        impact_class="impact-negative"
    elif impact=="mixed":
        impact_class="impact-mixed"
    else:
        impact_class="impact-neutral"
    return f"""<article class="event-card" data-search="{searchable}">
      <div class="event-topline"><span class="symbol-chip">{symbol}</span><span class="category-label">{category}</span><span class="event-state">{_esc(event_type)}</span></div>
      <h3>{headline}</h3>
      {f'<p class="event-details">{details}</p>' if details else ''}
      <div class="impact-row"><span class="impact-pill {impact_class}">{_esc(impact_label)}</span><span class="impact-explain">{rationale}</span></div>
      <div class="event-footer"><span>{filed} · {source}</span><a href="{source_url}" target="_blank" rel="noopener noreferrer">View original filing ↗</a></div>
    </article>"""

def _section(title, subtitle, events, empty_text, anchor):
    cards="".join(_card(e) for e in events)
    return f"""<section class="content-section" id="{anchor}">
      <div class="section-heading"><div><h2>{title}</h2><p>{subtitle}</p></div><span class="count-pill">{len(events)} items</span></div>
      <div class="event-list">{cards or f'<div class="empty-state">{empty_text}</div>'}</div>
    </section>"""

@app.get("/",response_class=HTMLResponse)
def dashboard():
    board_notices=store.list_events(100,status="UPCOMING")
    results=store.list_events(100,status="RESULT PUBLISHED")
    sudden=store.list_events(100,status="NEW EVENT")
    all_events=store.list_events(100)
    quotes=store.latest_market_quotes()
    nifty=next((q for q in quotes if q["symbol"]=="NIFTY"),None)

    def quote_row(q):
        is_index=q["symbol"]=="NIFTY"
        relative=None
        if nifty and not is_index:
            relative=float(q.get("change_pct") or 0)-float(nifty.get("change_pct") or 0)
        change=float(q.get("change_pct") or 0)
        change_class="positive" if change>0 else ("negative" if change<0 else "flat")
        relative_text="—" if relative is None else f'{"+" if relative>=0 else ""}{relative:.2f}%'
        relative_class="" if relative is None else ("positive" if relative>0 else ("negative" if relative<0 else "flat"))
        display_symbol="NIFTY 50" if is_index else _esc(q["symbol"])
        fetched=_esc(_display_time(q.get("fetched_at")))
        searchable=_esc(q["symbol"]+" "+display_symbol)
        return f"""<tr data-search="{searchable}">
          <td><strong>{display_symbol}</strong>{" <span class='index-tag'>INDEX</span>" if is_index else ""}</td>
          <td class="price-cell">₹{float(q["ltp"]):,.2f}</td>
          <td><span class="number-change {change_class}">{change:+.2f}%</span></td>
          <td>{int(q.get("volume") or 0):,}</td>
          <td><span class="number-change {relative_class}">{relative_text}</span></td>
          <td class="time-cell">{fetched}</td>
        </tr>"""

    market_rows="".join(quote_row(q) for q in quotes) or '<tr><td colspan="6" class="table-empty">No prices received yet. Check the Angel One connection status below.</td></tr>'
    market_state=status.get("market_data") or {}
    md_state=str(market_state.get("state") or "not checked")
    md_labels={
        "ok":("Prices received","status-good"),
        "not_configured":("Credentials needed","status-warn"),
        "configured_no_quotes":("No quotes received","status-warn"),
        "no_quotes":("No quotes received","status-warn"),
        "error":("Connection error","status-bad"),
        "disabled":("Price tracking off","status-muted"),
        "not checked":("Waiting for first check","status-muted"),
    }
    md_label,md_class=md_labels.get(md_state,(md_state.replace("_"," ").title(),"status-muted"))
    md_error=_esc(market_state.get("error"))
    market_time=_esc(_display_time(market_state.get("last_quote_at")))
    if md_error and md_state in ("not_configured","error"):
        md_message=md_error
    elif md_state=="ok":
        md_message="Angel One market quotes received. The table shows the latest saved snapshot."
    else:
        md_message="Prices will appear here after a successful read-only Angel One quote request."
    health_items=[]
    for name,feed in status.get("sources",{}).items():
        ok=bool(feed.get("ok"))
        label=name.replace("_"," ").title()
        detail=(f'{int(feed.get("items",0))} items received' if ok else str(feed.get("error") or "Source unavailable"))
        health_items.append(f'<div class="source-item"><span class="source-dot {"dot-good" if ok else "dot-bad"}"></span><div><strong>{_esc(label)}</strong><small>{_esc(detail)}</small></div><span class="source-state {"status-good" if ok else "status-bad"}">{"Working" if ok else "Issue"}</span></div>')
    if not health_items:
        health_items.append('<div class="empty-state compact">Feed status will appear after the first polling cycle.</div>')
    feed_error=_esc(status.get("last_error"))
    feed_notice=(f'<div class="notice notice-warn"><strong>Some checks need attention</strong><p>{feed_error}</p></div>' if feed_error else "")
    last_check=_esc(_display_time(status.get("last_poll")))
    interval=max(1,round(settings.poll_interval/60))
    ntfy_text="Notifications configured" if settings.ntfy_topic else "Notifications not configured"
    ntfy_class="status-good" if settings.ntfy_topic else "status-warn"
    events_summary=len(board_notices)+len(results)+len(sudden)
    all_cards="".join(_card(e) for e in all_events[:30])
    all_section=f"""<section class="content-section" id="all-events"><div class="section-heading"><div><h2>All recent filings</h2><p>Combined list of the latest announcements and results notices</p></div><span class="count-pill">{min(30,len(all_events))} shown</span></div><div class="event-list">{all_cards or '<div class="empty-state">No company filings have been saved yet.</div>'}</div></section>"""

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#f4f6fa"><title>Company News & Results Monitor</title>
<style>
:root{{--bg:#f4f6fa;--surface:#fff;--ink:#172235;--muted:#66758a;--line:#e4e9f0;--blue:#2459a6;--blue-soft:#edf4ff;--green:#18794e;--green-bg:#eaf7f0;--red:#b42318;--red-bg:#fff0ef;--amber:#966000;--amber-bg:#fff6df;--shadow:0 2px 10px rgba(20,40,70,.045)}}
*{{box-sizing:border-box}}html{{scroll-behavior:smooth;scroll-padding-top:88px}}body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.55 Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}}
a{{color:var(--blue);text-decoration:none}}a:hover{{text-decoration:underline}}
.shell{{max-width:1240px;margin:0 auto;padding:24px 22px 60px}}
.topbar{{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-bottom:22px}}
.brand{{display:flex;align-items:center;gap:10px;font-weight:750;letter-spacing:-.02em}}.brand-mark{{width:36px;height:36px;border-radius:10px;background:#1d4f91;color:#fff;display:grid;place-items:center;font-weight:800}}
.brand small{{display:block;color:var(--muted);font-weight:500;font-size:11px;letter-spacing:.04em;text-transform:uppercase}}
.top-status{{font-size:12px;color:var(--muted);display:flex;align-items:center;gap:7px}}.live-dot{{width:8px;height:8px;border-radius:50%;background:#2d9b68;display:inline-block}}
.hero{{background:var(--surface);border:1px solid var(--line);border-radius:18px;padding:26px 28px;box-shadow:var(--shadow)}}
.eyebrow{{text-transform:uppercase;font-size:11px;letter-spacing:.12em;font-weight:750;color:var(--blue)}}h1{{font-size:clamp(26px,4vw,36px);letter-spacing:-.04em;line-height:1.15;margin:7px 0 8px}}.hero-copy{{color:var(--muted);max-width:760px;margin:0}}
.hero-meta{{display:flex;gap:9px;flex-wrap:wrap;margin-top:18px}}.meta-chip{{background:#f1f4f8;color:#46566d;border-radius:30px;padding:6px 11px;font-size:12px;font-weight:650}}
.nav{{position:sticky;top:0;z-index:10;display:flex;gap:7px;overflow-x:auto;padding:12px 0;background:rgba(244,246,250,.94);backdrop-filter:blur(10px)}}.nav a{{white-space:nowrap;color:#45546a;font-size:13px;font-weight:650;padding:8px 12px;border-radius:9px}}.nav a:hover{{background:#e8eef7;text-decoration:none}}
.overview{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:16px 0}}
.metric{{background:var(--surface);border:1px solid var(--line);border-radius:13px;padding:15px 17px;box-shadow:var(--shadow)}}.metric-label{{color:var(--muted);font-size:12px;font-weight:650}}.metric-value{{font-size:27px;line-height:1.2;font-weight:780;letter-spacing:-.04em;margin-top:5px}}.metric-note{{font-size:11px;color:var(--muted);margin-top:3px}}
.panel{{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:18px 20px;box-shadow:var(--shadow);margin-top:14px}}
.panel-heading{{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:10px}}.panel-heading h2,.section-heading h2{{font-size:18px;letter-spacing:-.025em;margin:0}}.panel-heading p,.section-heading p{{font-size:12px;color:var(--muted);margin:3px 0 0}}
.status-pill,.count-pill,.event-state,.impact-pill{{border-radius:30px;display:inline-flex;align-items:center;font-weight:700;font-size:11px;padding:5px 9px;white-space:nowrap}}
.status-good{{color:var(--green);background:var(--green-bg)}}.status-warn{{color:var(--amber);background:var(--amber-bg)}}.status-bad{{color:var(--red);background:var(--red-bg)}}.status-muted{{color:#58677c;background:#eef1f5}}
.notice{{padding:12px 14px;border-radius:11px;margin-top:12px;font-size:13px}}.notice p{{margin:3px 0 0}}.notice-warn{{background:var(--amber-bg);color:#75500b;border:1px solid #f1dfb4}}
.source-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:12px}}.source-item{{border:1px solid var(--line);border-radius:10px;padding:10px;display:flex;align-items:flex-start;gap:8px;min-width:0}}.source-item>div{{min-width:0;flex:1}}.source-item strong{{font-size:12px;display:block}}.source-item small{{font-size:11px;color:var(--muted);display:block;overflow-wrap:anywhere;margin-top:2px}}.source-state{{font-size:10px;padding:3px 6px;border-radius:20px}}.source-dot{{width:8px;height:8px;border-radius:50%;flex-shrink:0;margin-top:5px}}.dot-good{{background:#229764}}.dot-bad{{background:#d54d45}}
.content-section{{margin-top:30px}}.section-heading{{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px}}.count-pill{{background:#edf1f6;color:#4c5c72}}
.event-list{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:11px}}.event-card{{background:var(--surface);border:1px solid var(--line);border-radius:13px;padding:16px 17px;box-shadow:var(--shadow);min-width:0}}.event-topline{{display:flex;gap:7px;align-items:center;flex-wrap:wrap}}.symbol-chip{{background:var(--blue-soft);color:#1d4f91;border-radius:6px;padding:3px 7px;font-size:11px;font-weight:800}}.category-label{{font-size:11px;color:var(--muted);font-weight:650}}.event-state{{margin-left:auto;background:#f0f2f6;color:#56657a;font-size:10px}}.event-card h3{{font-size:15px;line-height:1.4;margin:12px 0 6px;letter-spacing:-.01em}}.event-details{{font-size:13px;color:#526176;margin:0 0 10px;overflow-wrap:anywhere}}
.impact-row{{display:flex;align-items:flex-start;gap:8px;flex-wrap:wrap;margin-top:12px}}.impact-pill{{font-size:10px}}.impact-positive{{background:var(--green-bg);color:var(--green)}}.impact-negative{{background:var(--red-bg);color:var(--red)}}.impact-mixed{{background:var(--amber-bg);color:var(--amber)}}.impact-neutral{{background:#eff2f6;color:#58677c}}.impact-explain{{font-size:11px;color:var(--muted);flex:1;min-width:150px}}
.event-footer{{display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;border-top:1px solid var(--line);padding-top:10px;margin-top:13px;font-size:11px;color:var(--muted)}}.event-footer a{{font-weight:700;white-space:nowrap}}
.empty-state{{background:var(--surface);border:1px dashed #ccd5e1;color:var(--muted);padding:23px;border-radius:12px;font-size:13px;text-align:center;grid-column:1/-1}}.empty-state.compact{{padding:12px;text-align:left}}
.market-summary{{display:flex;gap:9px;flex-wrap:wrap;align-items:center}}.market-explain{{color:var(--muted);font-size:12px;margin:10px 0 12px}}
.table-wrap{{overflow-x:auto;border:1px solid var(--line);border-radius:10px}}table{{width:100%;border-collapse:collapse;min-width:760px}}th,td{{padding:10px 12px;text-align:left;border-bottom:1px solid var(--line);font-size:12px;white-space:nowrap}}th{{color:#627188;background:#f7f9fc;font-size:10px;text-transform:uppercase;letter-spacing:.06em}}tr:last-child td{{border-bottom:0}}.price-cell{{font-weight:750;font-variant-numeric:tabular-nums}}.number-change{{font-weight:750;font-variant-numeric:tabular-nums}}.positive{{color:var(--green)}}.negative{{color:var(--red)}}.flat{{color:#66758a}}.index-tag{{background:#eef1f5;color:#64748a;border-radius:4px;padding:2px 5px;font-size:9px;font-weight:750}}.time-cell{{color:var(--muted);font-size:11px}}.table-empty{{text-align:center;color:var(--muted);padding:20px}}
.search-row{{display:flex;gap:10px;margin:14px 0 0}}.search-input{{width:100%;border:1px solid #d5dde8;background:#fff;padding:10px 13px;border-radius:9px;font:inherit;font-size:13px;outline:none}}.search-input:focus{{border-color:#6190c9;box-shadow:0 0 0 3px #dceaff}}.search-help{{font-size:11px;color:var(--muted);margin-top:6px}}.hidden-by-search{{display:none!important}}
.note-box{{background:#f7f9fc;border-left:3px solid #8caed8;padding:10px 12px;border-radius:0 8px 8px 0;color:#526176;font-size:12px;margin-top:12px}}
.footer{{margin-top:35px;border-top:1px solid var(--line);padding-top:15px;color:var(--muted);font-size:11px;display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap}}
@media(max-width:800px){{.overview{{grid-template-columns:repeat(2,minmax(0,1fr))}}.source-grid{{grid-template-columns:1fr}}.event-list{{grid-template-columns:1fr}}.shell{{padding:15px 13px 40px}}.hero{{padding:21px 18px}}.panel{{padding:15px}}}}
@media(max-width:440px){{.topbar{{align-items:flex-start}}.top-status{{font-size:10px}}.metric{{padding:12px}}.metric-value{{font-size:23px}}.event-state{{margin-left:0}}.event-footer{{display:block}}.event-footer a{{display:inline-block;margin-top:6px}}}}
</style></head>
<body><div class="shell">
  <div class="topbar"><div class="brand"><span class="brand-mark">N</span><div>Market Watch Desk<small>NSE F&O company monitor</small></div></div><div class="top-status"><span class="live-dot"></span> Scheduled checks every {interval} min</div></div>
  <header class="hero"><div class="eyebrow">Company announcements · results · market prices</div><h1>Company News & Results</h1>
    <p class="hero-copy">A single place to see which F&O companies have board-meeting notices, published results or fresh announcements — alongside their latest share-price snapshot.</p>
    <div class="hero-meta"><span class="meta-chip">Last checked: {last_check}</span><span class="meta-chip {_esc(ntfy_class)}">{_esc(ntfy_text)}</span><span class="meta-chip">Read-only market data</span></div>
  </header>
  <nav class="nav" aria-label="Dashboard sections"><a href="#overview">Overview</a><a href="#prices">Share prices</a><a href="#board-notices">Board meetings</a><a href="#results">Results published</a><a href="#latest-news">Latest news</a><a href="#all-events">All filings</a><a href="#data-health">Data status</a></nav>

  <section id="overview" class="overview">
    <div class="metric"><div class="metric-label">Board meeting notices</div><div class="metric-value">{len(board_notices)}</div><div class="metric-note">Check the filing for the meeting date</div></div>
    <div class="metric"><div class="metric-label">Results published</div><div class="metric-value">{len(results)}</div><div class="metric-note">Exchange results notices detected</div></div>
    <div class="metric"><div class="metric-label">New company announcements</div><div class="metric-value">{len(sudden)}</div><div class="metric-note">Recent filings classified as news</div></div>
    <div class="metric"><div class="metric-label">Companies with price quotes</div><div class="metric-value">{sum(1 for q in quotes if q["symbol"]!="NIFTY")}</div><div class="metric-note">Latest saved Angel One snapshots</div></div>
  </section>

  <section id="prices" class="panel">
    <div class="panel-heading"><div><h2>Share prices</h2><p>Latest Angel One quote snapshot for the tracked F&O shares</p></div><span class="status-pill {md_class}">{_esc(md_label)}</span></div>
    <div class="market-summary"><span class="meta-chip">Provider: Angel One SmartAPI</span><span class="meta-chip">Quotes saved: {int(market_state.get("quotes") or 0)}</span><span class="meta-chip">Fetched: {market_time}</span></div>
    <p class="market-explain">Change = today’s percentage move. Relative to NIFTY = the share’s move minus NIFTY 50’s move. Volume = shares traded as reported in the quote. These figures provide context; they do not prove that a particular news item caused the price move.</p>
    <div class="table-wrap"><table><thead><tr><th>Company / Index</th><th>Last traded price</th><th>Day change</th><th>Volume</th><th>Vs NIFTY 50</th><th>Quote fetched at</th></tr></thead><tbody>{market_rows}</tbody></table></div>
    <div class="note-box">{_esc(md_message)} Prices are snapshots, not a tick-by-tick chart. A blank table means no successful quote has been saved yet.</div>
  </section>

  <div class="search-row"><input class="search-input" id="quickSearch" type="search" placeholder="Search a symbol or company announcement (e.g. RELIANCE, results, order)..." aria-label="Search dashboard"></div><div class="search-help" id="searchFeedback">Search filters the visible price rows and company-filing cards.</div>

  {_section("Board meeting notices","The exchange feed has identified these board-meeting notices. Open the filing to confirm the exact meeting date.",board_notices,"No board-meeting notices are saved yet.","board-notices")}
  {_section("Results published","A results filing was detected. 'Impact not assessed' means the reported figures have not yet been compared with prior results and expectations.",results,"No results filings have been detected yet.","results")}
  {_section("Latest company news","Fresh announcements such as orders, management changes, regulatory items and corporate actions.",sudden,"No new company announcements are saved yet.","latest-news")}
  {all_section}

  <section id="data-health" class="panel"><div class="panel-heading"><div><h2>Data connection status</h2><p>Check this section when prices or announcements are missing</p></div></div>
    <div class="source-grid">{"".join(health_items)}</div>
    {feed_notice}
    <div class="note-box">Angel One is used only to read market quotes. This app has no order-placement, order-modification or order-cancellation feature. An event’s impact label is a preliminary assessment, not a guaranteed prediction.</div>
    <div class="footer"><span>Last check: {last_check}</span><span>Source timestamp and quote-fetch time may differ from publication time.</span><span><a href="/api/health">Technical health details</a> · <a href="/api/market">Market data API</a></span></div>
  </section>
</div>
<script>
(function(){{
  const search=document.getElementById("quickSearch");
  const feedback=document.getElementById("searchFeedback");
  if(!search)return;
  search.addEventListener("input",function(){{
    const q=search.value.trim().toLowerCase();
    let visible=0;
    document.querySelectorAll("[data-search]").forEach(function(node){{
      const match=!q||(node.getAttribute("data-search")||"").toLowerCase().includes(q);
      node.classList.toggle("hidden-by-search",!match);
      if(match)visible++;
    }});
    feedback.textContent=q?("Showing "+visible+" matching price rows / company filings."): "Search filters the visible price rows and company-filing cards.";
  }});
}})();
</script>
</body></html>"""

@app.get("/api/events")
def events(limit:int=Query(100,ge=1,le=500),symbol:str|None=None,status_filter:str|None=None):
    return {"events":store.list_events(limit,symbol,status_filter)}

@app.get("/api/health")
def health():
    return {"ok":True,"app":settings.app_name,"database_events":store.count(),
      "poll_interval_seconds":settings.poll_interval,"ntfy_configured":bool(settings.ntfy_topic),
      "last_poll":status["last_poll"],"last_success":status["last_success"],
      "last_error":status["last_error"],"sources":status["sources"],"universe_source":status["universe_source"],
      "market_data_provider":"Angel One SmartAPI read-only" if settings.angel_market_data_enabled else "disabled",
      "market_data":status["market_data"]}

@app.get("/api/market")
def market():
    return {"provider":"Angel One SmartAPI read-only","quotes":store.latest_market_quotes(),"status":status["market_data"]}

@app.post("/api/sync")
def sync(): return poll_once()
