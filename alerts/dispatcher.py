"""Bounded opt-in webhook and ntfy alert delivery."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Protocol

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AlertDelivery:
    channel: str
    delivered: bool
    detail: str


class Sender(Protocol):
    def post(self, url: str, *, data: str, headers: dict[str, str], timeout: int) -> Any: ...


def candidate_alert_payload(signal: dict[str, Any]) -> dict[str, Any]:
    """Build an honest candidate alert, never an order instruction."""
    return {
        "title": f"NSE OI candidate: {signal.get('symbol', 'UNKNOWN')}",
        "event": "OI_PRICE_CANDIDATE",
        "symbol": signal.get("symbol"),
        "classification": signal.get("signal_label") or signal.get("signal"),
        "confidence": signal.get("confidence"),
        "observed_direction": signal.get("observed_direction") or signal.get("signal_direction"),
        "technical_alignment": signal.get("technical_alignment"),
        "trade_recommendation": "NO_TRADE",
        "message": "Price/OI candidate observed. Independent validation is still required; this is not a trade instruction.",
    }


def _post(sender: Sender, url: str, body: str, headers: dict[str, str]) -> AlertDelivery:
    try:
        response = sender.post(url, data=body, headers=headers, timeout=10)
        code = int(getattr(response, "status_code", 0))
        return AlertDelivery("", 200 <= code < 300, f"HTTP {code}")
    except Exception as exc:
        logger.warning("Alert delivery failed: %s", exc)
        return AlertDelivery("", False, str(exc))


def dispatch_candidate_alert(
    signal: dict[str, Any], *, webhook_url: str | None, ntfy_topic_url: str | None,
    telegram_bot_token: str | None = None, telegram_chat_id: str | None = None,
    sender: Sender | None = None,
) -> list[AlertDelivery]:
    """Send only to configured channels; return per-channel outcomes for audit."""
    if not webhook_url and not ntfy_topic_url and not (telegram_bot_token and telegram_chat_id):
        return []
    sender = sender or cffi_requests.Session(impersonate="chrome120")
    payload = candidate_alert_payload(signal)
    results: list[AlertDelivery] = []
    if webhook_url:
        result = _post(sender, webhook_url, json.dumps(payload), {"Content-Type": "application/json"})
        results.append(AlertDelivery("webhook", result.delivered, result.detail))
    if ntfy_topic_url:
        result = _post(sender, ntfy_topic_url, payload["message"], {
            "Title": payload["title"],
            "Tags": "chart_with_upwards_trend",
            "Priority": "default",
        })
        results.append(AlertDelivery("ntfy", result.delivered, result.detail))
    if telegram_bot_token and telegram_chat_id:
        # Telegram credentials are optional environment secrets.  The token is
        # used only as a destination URL and is never persisted in the audit DB.
        result = _post(
            sender,
            f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage",
            json.dumps({"chat_id": telegram_chat_id, "text": payload["message"]}),
            {"Content-Type": "application/json"},
        )
        results.append(AlertDelivery("telegram", result.delivered, result.detail))
    return results


def dispatch_lifecycle_alert(
    event: dict[str, Any], *, webhook_url: str | None, ntfy_topic_url: str | None,
    telegram_bot_token: str | None = None, telegram_chat_id: str | None = None,
    sender: Sender | None = None,
) -> list[AlertDelivery]:
    """Deliver paper scalp lifecycle events without implying a live order."""
    if not webhook_url and not ntfy_topic_url and not (telegram_bot_token and telegram_chat_id):
        return []
    sender = sender or cffi_requests.Session(impersonate="chrome120")
    event_type = str(event.get("event") or "SCALP_UPDATE").upper()
    symbol = str(event.get("symbol") or "UNKNOWN").upper()
    title = str(event.get("title") or f"NSE paper scalp {event_type}: {symbol}")
    message = str(event.get("message") or f"{event_type}: {symbol}. Paper-only; not an order instruction.")
    payload = {**event, "event": event_type, "symbol": symbol, "title": title,
               "message": message, "paper_only": True, "live_order": False}
    results: list[AlertDelivery] = []
    if webhook_url:
        result = _post(sender, webhook_url, json.dumps(payload, default=str),
                       {"Content-Type": "application/json"})
        results.append(AlertDelivery("webhook", result.delivered, result.detail))
    if ntfy_topic_url:
        result = _post(sender, ntfy_topic_url, message, {
            "Title": title, "Tags": "chart_with_upwards_trend" if event_type == "ENTRY" else "warning",
            "Priority": "high" if event_type == "EXIT" else "default",
        })
        results.append(AlertDelivery("ntfy", result.delivered, result.detail))
    if telegram_bot_token and telegram_chat_id:
        result = _post(sender, f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage",
                       json.dumps({"chat_id": telegram_chat_id, "text": message}),
                       {"Content-Type": "application/json"})
        results.append(AlertDelivery("telegram", result.delivered, result.detail))
    return results
