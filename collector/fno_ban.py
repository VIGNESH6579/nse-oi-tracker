"""NSE F&O securities-in-ban list (daily CSV). OI signals on banned stocks are unreliable.

Fail-open with a loud warning: if the list cannot be fetched we keep the last good
list (same day) and report its age in /api/health rather than blocking everything.
"""

from __future__ import annotations

import logging
import re
import threading
import time
import urllib.request

logger = logging.getLogger(__name__)
URLS = ["https://nsearchives.nseindia.com/content/fo/fo_secban.csv",
        "https://archives.nseindia.com/content/fo/fo_secban.csv"]
_lock = threading.Lock()
_state: dict = {"symbols": frozenset(), "fetched_at": None, "ok": False}
_SYMBOL = re.compile(r"^[A-Z0-9&\-]{2,20}$")


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
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=20) as response:
        return response.read().decode("utf-8", "ignore")


def refresh_ban_list(fetch=_fetch) -> bool:
    for url in URLS:
        try:
            symbols = parse_ban_csv(fetch(url))
        except Exception:
            logger.warning("F&O ban list fetch failed: %s", url, exc_info=True)
            continue
        with _lock:
            _state.update(symbols=symbols, fetched_at=time.time(), ok=True)
        logger.info("F&O ban list loaded: %d symbols", len(symbols))
        return True
    with _lock:
        _state["ok"] = False
    logger.warning("FNO_BAN_LIST_UNAVAILABLE: keeping previous list (%d symbols)", len(_state["symbols"]))
    return False


def banned_symbols() -> frozenset[str]:
    return _state["symbols"]


def ban_info() -> dict:
    fetched = _state["fetched_at"]
    return {"ban_list_ok": _state["ok"], "ban_list_size": len(_state["symbols"]),
            "ban_list_age_s": round(time.time() - fetched) if fetched else None}
