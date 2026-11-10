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


def send_many(events: list[dict]) -> bool:
    """Publish a small digest in one ntfy request to avoid per-event rate-limit bursts."""
    global _rate_limited_until
    if time.monotonic() < _rate_limited_until:
        return False
    if not settings.ntfy_topic:
        return False

    eligible = [
        event for event in events
        if str(event.get("event_status") or "").upper() in {"RESULT PUBLISHED", "SUDDEN EVENT"}
    ]
    if not eligible:
        return True

    rendered = []
    for event in eligible[:3]:
        _, _, body = _alert_text(event)
        rendered.append(body)
    body = "\n\n━━━━━━━━━━━━━━━━━━━━\n\n".join(rendered)
    if len(body) > 3500:
        body = body[:3480].rsplit("\n", 1)[0] + "\n… Open source links above for full filing details."

    symbols = ",".join(str(e.get("symbol") or "COMPANY") for e in eligible[:3])
    has_sudden = any(str(e.get("event_status") or "").upper() == "SUDDEN EVENT" for e in eligible)
    impacts = {str(e.get("impact") or "unassessed").upper() for e in eligible}
    tag = (
        "chart_with_downwards_trend" if impacts == {"BEARISH"} else
        "chart_with_upwards_trend" if impacts == {"BULLISH"} else
        "warning" if impacts & {"BEARISH", "BULLISH", "MIXED"} else "newspaper"
    )
    title = f"NSE NEWS | {len(eligible[:3])} ALERT(S)"
    headers = {
        "Title": title[:250],
        "Priority": "high" if has_sudden else "default",
        "Tags": tag,
    }
    if settings.ntfy_token:
        headers["Authorization"] = f"Bearer {settings.ntfy_token}"
    url = f"{settings.ntfy_base_url}/{settings.ntfy_topic}"
    try:
        response = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=15)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "")
            try:
                cooldown = max(60, int(retry_after))
            except (TypeError, ValueError):
                cooldown = 900
            _rate_limited_until = time.monotonic() + cooldown
            log.warning("ntfy rate limited; pausing digest for %s seconds (events=%s)",
                        cooldown, symbols)
            return False
        response.raise_for_status()
        log.info("ntfy digest delivery succeeded: events=%s count=%s", symbols, len(eligible[:3]))
        return True
    except requests.RequestException as exc:
        log.warning("ntfy digest delivery failed for events=%s error=%s", symbols, exc)
        return False


def send(event: dict) -> bool:
    """Backward-compatible single-event sender."""
    status = str(event.get("event_status") or "").upper()
    if status not in {"RESULT PUBLISHED", "SUDDEN EVENT"}:
        log.info("ntfy skipped non-priority event: status=%s event_id=%s",
                 status, event.get("event_id"))
        return True
    return send_many([event])
