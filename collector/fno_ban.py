"""NSE F&O securities-in-ban list (daily CSV).

The official NSE file is authoritative. Render cloud egress currently receives
403s from both official archive hosts, so a GitHub Actions relay periodically
fetches the same official file and publishes it on the public data branch.
If neither direct NSE nor the verified relay is fresh, the gate remains fail-closed.
"""

from __future__ import annotations

import logging
import re
import threading
import time
import json
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
URLS = ["https://nsearchives.nseindia.com/content/fo/fo_secban.csv",
        "https://archives.nseindia.com/content/fo/fo_secban.csv"]
_lock = threading.Lock()
_state: dict = {"symbols": frozenset(), "fetched_at": None, "ok": False, "source": None}
_SYMBOL = re.compile(r"^[A-Z0-9&\-]{2,20}$")

RELAY_CSV_URLS = [
    "https://raw.githubusercontent.com/VIGNESH6579/nse-oi-tracker/data/data/fo_secban.csv",
    "https://github.com/VIGNESH6579/nse-oi-tracker/raw/refs/heads/data/data/fo_secban.csv",
]
RELAY_META_URLS = [
    "https://raw.githubusercontent.com/VIGNESH6579/nse-oi-tracker/data/data/fo_secban_meta.json",
    "https://github.com/VIGNESH6579/nse-oi-tracker/raw/refs/heads/data/data/fo_secban_meta.json",
]
# GitHub Actions normally refreshes this every 10 minutes. Allow a bounded
# delay during Actions scheduling so a perfectly valid official daily file
# does not fail closed merely because one scheduled run was delayed.
RELAY_MAX_AGE_S = 2 * 60 * 60


def _fetch_verified_relay() -> str:
    headers = {"User-Agent": "nse-oi-tracker-ban-relay/1.0", "Accept": "*/*"}
    meta = None
    last_error = None
    for url in RELAY_META_URLS:
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=10) as response:
                meta = json.loads(response.read().decode("utf-8"))
            break
        except Exception as exc:
            last_error = exc
    if not meta:
        raise RuntimeError(f"verified NSE ban relay metadata unavailable: {type(last_error).__name__ if last_error else 'unknown'}")
    stamp = str(meta.get("fetched_at_utc") or "")
    fetched = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    age = (datetime.now(timezone.utc) - fetched).total_seconds()
    if age < 0 or age > RELAY_MAX_AGE_S:
        raise RuntimeError(f"verified NSE ban relay is stale age_s={age:.0f}")

    last_error = None
    for url in RELAY_CSV_URLS:
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=10) as response:
                text = response.read().decode("utf-8")
            if text.strip():
                return text
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"verified NSE ban relay CSV unavailable: {type(last_error).__name__ if last_error else 'unknown'}")


def parse_ban_csv(text: str) -> frozenset[str]:
    found = set()
    for line in text.splitlines():
        parts = [p.strip().strip('"') for p in line.split(",")]
        if len(parts) >= 2 and parts[0].isdigit():
            symbol = parts[-1].upper()
            if _SYMBOL.match(symbol):
                found.add(symbol)
    return frozenset(found)


def _fetch(url: str) -> str:
    # Same session/TLS-impersonating client as the (working) bhavcopy download;
    # plain urllib is rejected by NSE's edge from cloud IPs.
    from app.nse_fetcher import fetch_nse_archive_text
    text = fetch_nse_archive_text(url)
    if not text:
        raise RuntimeError("empty or blocked response")
    return text


def refresh_ban_list(fetch=_fetch) -> bool:
    for url in URLS:
        try:
            symbols = parse_ban_csv(fetch(url))
        except Exception:
            logger.warning("F&O ban list fetch failed: %s", url, exc_info=True)
            continue
        with _lock:
            _state.update(symbols=symbols, fetched_at=time.time(), ok=True, source="official_nse")
        logger.info("F&O ban list loaded: %d symbols source=official_nse", len(symbols))
        return True
    try:
        symbols = parse_ban_csv(_fetch_verified_relay())
        with _lock:
            _state.update(symbols=symbols, fetched_at=time.time(), ok=True, source="official_nse_via_github_relay")
        logger.info("F&O ban list loaded: %d symbols source=official_nse_via_github_relay", len(symbols))
        return True
    except Exception:
        logger.warning("Verified NSE ban relay unavailable or stale", exc_info=True)
    with _lock:
        _state["ok"] = False
        _state["source"] = None
    logger.warning("FNO_BAN_LIST_UNAVAILABLE: keeping previous list (%d symbols)", len(_state["symbols"]))
    return False


def banned_symbols() -> frozenset[str]:
    return _state["symbols"]


def ban_info() -> dict:
    fetched = _state["fetched_at"]
    return {"ban_list_ok": _state["ok"], "ban_list_size": len(_state["symbols"]),
            "ban_list_age_s": round(time.time() - fetched) if fetched else None,
            "ban_list_source": _state.get("source")}
