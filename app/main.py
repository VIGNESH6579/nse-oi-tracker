import logging
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler
from .config import settings
from .store import Store
from .nse import fetch_announcements, get_fno_symbols, normalize
from .notifier import send

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
store = Store(settings.database_path)
scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
poll_lock = threading.Lock()
status = {"last_poll": None, "last_success": None, "last_error": None, "source": "not checked", "new_events": 0}

def poll_once():
    if not poll_lock.acquire(blocking=False):
        return {"skipped": True, "reason": "poll already running"}
    try:
        symbols, source = get_fno_symbols()
        rows = fetch_announcements()
        new_count = 0
        for row in rows:
            event = normalize(row, symbols)
            if not event:
                continue
            if store.insert(event):
                new_count += 1
        # Retry pending notifications on later polls; a temporary ntfy outage
        # must not permanently lose an alert after the event is stored.
        for pending in store.pending_notifications(limit=50):
            if send(pending):
                store.mark_notified(pending["event_id"])
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        status.update(last_poll=now, last_success=now, last_error=None, source=source, new_events=new_count)
        log.info("Poll complete: %s announcements, %s new F&O events", len(rows), new_count)
        return {"received":len(rows), "new_events":new_count, "universe_source":source}
    except Exception as exc:
        from datetime import datetime, timezone
        status.update(last_poll=datetime.now(timezone.utc).isoformat(), last_error=str(exc))
        log.exception("News poll failed")
        return {"error":str(exc)}
    finally:
        poll_lock.release()

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(poll_once, "interval", seconds=settings.poll_interval, id="nse-news-poll", max_instances=1, coalesce=True)
    scheduler.start()
    # First poll is asynchronous to avoid delaying startup.
    threading.Thread(target=poll_once, daemon=True).start()
    yield
    scheduler.shutdown(wait=False)

app = FastAPI(title=settings.app_name, version="1.0.0", lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
def dashboard():
    events = store.list_events(100)
    cards = "".join(
        f'<article><div class="meta">{e["symbol"]} · {e["category"]} · {e["impact"]}</div>'
        f'<h3>{_esc(e["headline"])}</h3><p>{_esc(e.get("details") or "")}</p>'
        f'<small>{_esc(e.get("filed_at") or "Time unavailable")}</small><p><a href="{_esc(e["source_url"])}" target="_blank" rel="noopener">Open NSE filing ↗</a></p></article>'
        for e in events
    ) or "<p>No events collected yet. Check source status below.</p>"
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
    <title>{settings.app_name}</title><style>
    body{{font:15px system-ui;background:#0b1220;color:#e8edf5;margin:0;padding:28px;max-width:1050px;margin:auto}}
    h1{{margin-bottom:4px}}.sub{{color:#9eacc2}}article{{background:#141f32;border:1px solid #263750;border-radius:12px;padding:16px;margin:12px 0}}
    .meta{{color:#9ec5ff;font-size:12px;text-transform:uppercase;letter-spacing:.05em}}h3{{margin:8px 0}}small,p{{color:#b6c2d3}}a{{color:#8fc5ff}}
    .top{{padding:18px;background:#141f32;border-radius:12px;margin:20px 0}}code{{color:#b6dcff}}
    </style></head><body><h1>Company News Monitor</h1><div class="sub">NSE F&O announcements · filing-first · no trading signals</div>
    <div class="top">Last poll: <code>{_esc(status.get("last_poll") or "not yet")}</code><br>
    Source: <code>{_esc(status.get("source") or "not checked")}</code><br>
    Status: <code>{_esc(status.get("last_error") or "healthy / waiting")}</code><br>
    Stored events: <code>{store.count()}</code> · New events in last poll: <code>{status.get("new_events",0)}</code></div>
    <h2>Latest announcements</h2>{cards}</body></html>"""

def _esc(value):
    import html
    return html.escape(str(value or ""))

@app.get("/api/events")
def events(limit: int = Query(100, ge=1, le=500), symbol: str | None = None):
    return {"events":store.list_events(limit, symbol)}

@app.get("/api/health")
def health():
    return {"ok":True,"app":settings.app_name,"database_events":store.count(),
            "poll_interval_seconds":settings.poll_interval,"ntfy_configured":bool(settings.ntfy_topic),
            "last_poll":status["last_poll"],"last_success":status["last_success"],
            "last_error":status["last_error"],"universe_source":status["source"]}

@app.post("/api/sync")
def sync():
    return poll_once()
