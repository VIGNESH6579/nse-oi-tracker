import logging
import time
import requests
from .config import settings

log = logging.getLogger(__name__)
_rate_limited_until = 0.0

def send(event: dict) -> bool:
    global _rate_limited_until
    if time.monotonic() < _rate_limited_until:
        return False
    if not settings.ntfy_topic:
        return False
    url = f"{settings.ntfy_base_url}/{settings.ntfy_topic}"
    title = f'{event["symbol"]}: {event["category"]} [{event["impact"]}]'
    body = "\n".join([event["headline"], f'Filed: {event.get("filed_at") or "time not supplied"}',
                       f'Source: {event.get("source_url") or ""}',
                       "Impact is a text-based indication, not a price prediction."])
    headers = {"Title": title[:250], "Priority": "default", "Tags": "newspaper"}
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
        log.info("ntfy delivery succeeded: symbol=%s event_id=%s",event.get("symbol"),event.get("event_id"))
        return True
    except requests.RequestException as exc:
        log.warning("ntfy delivery failed for event_id=%s error=%s", event.get("event_id"), exc)
        return False
