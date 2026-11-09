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
                quote_symbols=sorted(symbols) if symbols else ["NIFTY"]
                quotes=angel_market_data.quotes(quote_symbols)
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
    return html.escape(str(v or ""))

def _card(e):
    direction=e.get("impact","unclassified").upper()
    return (f'<article><div class="meta">{_esc(e["symbol"])} · {_esc(e["category"])} · {_esc(e.get("event_status"))}</div>'
      f'<h3>{_esc(e["headline"])}</h3><p>{_esc(e.get("details"))}</p>'
      f'<b class="direction">{_esc(direction)}</b><p class="why">{_esc(e.get("rationale"))}</p>'
      f'<small>Filed: {_esc(e.get("filed_at") or "time not supplied")} · {_esc(e.get("source_name"))}</small>'
      f'<p><a href="{_esc(e["source_url"])}" target="_blank" rel="noopener">Open exchange filing ↗</a></p></article>')

@app.get("/",response_class=HTMLResponse)
def dashboard():
    upcoming=store.list_events(100,status="UPCOMING")
    results=store.list_events(100,status="RESULT PUBLISHED")
    sudden=store.list_events(100,status="NEW EVENT")
    all_events=store.list_events(100)
    quotes=store.latest_market_quotes()
    nifty=next((q for q in quotes if q["symbol"]=="NIFTY"),None)
    def quote_row(q):
        relative=(float(q.get("change_pct") or 0)-float(nifty.get("change_pct") or 0)) if nifty and q["symbol"]!="NIFTY" else None
        rel=(" · "+("+" if relative>=0 else "")+f"{relative:.2f}% vs NIFTY") if relative is not None else ""
        return f'<tr><td>{_esc(q["symbol"])}</td><td>{float(q["ltp"]):.2f}</td><td>{float(q.get("change_pct") or 0):+.2f}%</td><td>{int(q.get("volume") or 0):,}</td><td>{_esc(q.get("fetched_at"))}{_esc(rel)}</td></tr>'
    market_rows="".join(quote_row(q) for q in quotes[:100]) or "<tr><td colspan=5>No Angel One quotes yet. Check credentials and symbol-token mapping.</td></tr>"
    source_lines="".join(f'<li>{_esc(k)}: {"OK · "+str(v.get("items",0))+" items" if v.get("ok") else "ERROR · "+_esc(v.get("error"))}</li>' for k,v in status["sources"].items()) or "<li>Waiting for first feed poll</li>"
    banner="SOURCE ERROR: "+_esc(status["last_error"]) if status["last_error"] else "Feeds report status individually below."
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>{settings.app_name}</title><style>
body{{font:15px system-ui;background:#0b1220;color:#e8edf5;margin:0;padding:24px;max-width:1120px;margin:auto}}
h1{{margin-bottom:4px}}.sub,small,.why{{color:#9eacc2}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px}}
.panel,article{{background:#141f32;border:1px solid #263750;border-radius:12px;padding:16px;margin:12px 0}}
.meta{{color:#9ec5ff;font-size:12px;text-transform:uppercase;letter-spacing:.04em}}h3{{margin:8px 0}}
a{{color:#8fc5ff}}table{{width:100%;border-collapse:collapse}}td,th{{padding:7px;text-align:left;border-bottom:1px solid #263750;font-size:13px}}.direction{{color:#f0d28a}}.alert{{color:#ffb4a8}}.tabs{{display:flex;gap:18px;flex-wrap:wrap}}
</style></head><body><h1>Company Event & Results Monitor</h1><div class="sub">NSE F&O companies · scheduled events · sudden filings · results status · no automatic trade calls</div>
<div class="grid"><div class="panel">Last poll<br><b>{_esc(status["last_poll"] or "not yet")}</b></div>
<div class="panel">F&O universe<br><b>{_esc(status["universe_source"])}</b></div>
<div class="panel">Stored events<br><b>{store.count()}</b></div>
<div class="panel">New events last poll<br><b>{status["new_events"]}</b></div></div>
<div class="panel"><b>Feed health</b><ul>{source_lines}</ul><div class="alert">{banner}</div></div>
<h2>Market prices · Angel One read-only</h2>
<div class="panel"><b>Market data status:</b> {_esc(status["market_data"]["state"])} · <b>Quotes:</b> {status["market_data"]["quotes"]} · <b>Fetched:</b> {_esc(status["market_data"].get("last_quote_at") or "not yet")}
{("<p class='alert'>"+_esc(status["market_data"].get("error"))+"</p>") if status["market_data"].get("error") else ""}
<p class="why">Price change and traded volume are market context, not proof that a filing caused the move. Relative change is stock % change minus NIFTY 50 % change. Quote time is fetch time.</p>
<div style="overflow-x:auto"><table><thead><tr><th>Symbol</th><th>LTP</th><th>Change</th><th>Volume</th><th>Fetched / relative move</th></tr></thead><tbody>{market_rows}</tbody></table></div></div>
<h2>Upcoming events</h2>{''.join(_card(e) for e in upcoming) or '<p>No upcoming event records found in the feeds yet.</p>'}
<h2>Results published</h2>{''.join(_card(e) for e in results) or '<p>No new results filings found yet.</p>'}
<h2>Sudden / latest news events</h2>{''.join(_card(e) for e in sudden) or '<p>No new events found yet.</p>'}
<h2>All latest events</h2>{''.join(_card(e) for e in all_events[:30]) or '<p>No events collected.</p>'}
<p class="why">Direction labels are evidence-based preliminary assessments. Neutral/unclassified means there is not enough structured evidence. Market-price confirmation is shown separately when a market-data provider is configured.</p>
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
