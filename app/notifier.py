import logging
import requests
from .config import settings

log = logging.getLogger(__name__)

def send(event: dict) -> bool:
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
        response.raise_for_status()
        return True
    except requests.RequestException:
        log.exception("ntfy delivery failed for %s", event.get("event_id"))
        return False
