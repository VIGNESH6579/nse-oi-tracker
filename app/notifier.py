import logging
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests
from .config import settings

log = logging.getLogger(__name__)
_rate_limited_until = 0.0


def _filed_time(value):
    if not value:
        return "Time not supplied by exchange"
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ZoneInfo("Asia/Kolkata")).strftime("%d %b %Y, %I:%M:%S %p IST")
    except (TypeError, ValueError):
        return str(value)


def _alert_text(event):
    status = str(event.get("event_status") or "").upper()
    impact = str(event.get("impact") or "unassessed").upper()
    if impact not in {"BULLISH", "BEARISH", "MIXED"}:
        impact = "NOT ASSESSED"
    kind = "RESULT" if status == "RESULT PUBLISHED" else "SUDDEN EVENT"
    company = event.get("company") or event.get("symbol") or "Company"
    headline = event.get("headline") or "Exchange filing"
    details = str(event.get("details") or "").strip()
    rationale = str(event.get("rationale") or "").strip()
    lines = [
        f"Company: {company} ({event.get('symbol') or ''})",
        f"Event: {kind} — {headline}",
        f"Direction: {impact}",
    ]
    if rationale:
        lines.append(f"Why: {rationale}")
    if details:
        lines.append(f"Filing info: {details[:900]}")
    else:
        lines.append("Filing info: Feed did not include the detailed figures; open the original filing.")
    lines.extend([
        f"Filed: {_filed_time(event.get('filed_at'))}",
        f"Source: {event.get('source_name') or 'Exchange filing'}",
        f"Original: {event.get('source_url') or 'Exchange filing link unavailable'}",
        "Direction is a text-based event assessment, not a guaranteed price move.",
    ])
    return kind, impact, "\n".join(lines)


def send(event: dict) -> bool:
    global _rate_limited_until
    if time.monotonic() < _rate_limited_until:
        return False
    if not settings.ntfy_topic:
        return False
    status = str(event.get("event_status") or "").upper()
    if status not in {"RESULT PUBLISHED", "SUDDEN EVENT"}:
        log.info("ntfy skipped non-priority event: status=%s event_id=%s", status, event.get("event_id"))
        return True
    kind, impact, body = _alert_text(event)
    symbol = event.get("symbol") or "COMPANY"
    title = f"{symbol} | {kind} | {impact}"
    priority = "high" if kind == "SUDDEN EVENT" else "default"
    tag = "chart_with_downwards_trend" if impact == "BEARISH" else (
        "chart_with_upwards_trend" if impact == "BULLISH" else ("warning" if impact == "MIXED" else "newspaper")
    )
    url = f"{settings.ntfy_base_url}/{settings.ntfy_topic}"
    headers = {"Title": title[:250], "Priority": priority, "Tags": tag}
    if settings.ntfy_token:
        headers["Authorization"] = f"Bearer {settings.ntfy_token}"
    try:
        response = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=15)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "")
            try:
                cooldown = max(60, int(retry_after))
            except (TypeError, ValueError):
                cooldown = 900
            _rate_limited_until = time.monotonic() + cooldown
            log.warning("ntfy rate limited; pausing sends for %s seconds (event_id=%s)",
                        cooldown, event.get("event_id"))
            return False
        response.raise_for_status()
        log.info("ntfy delivery succeeded: symbol=%s status=%s impact=%s event_id=%s",
                 symbol, status, impact, event.get("event_id"))
        return True
    except requests.RequestException as exc:
        log.warning("ntfy delivery failed for event_id=%s error=%s", event.get("event_id"), exc)
        return False
