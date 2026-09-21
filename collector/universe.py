"""Real NSE F&O underlying universe (free, public sources, fail-closed).

Priority: Angel One public instrument master -> NSE ``fo_mktlots.csv`` ->
committed ``data/fno_symbols.json``. If every source fails the universe is
empty; callers must then skip storage instead of falling back to all ~2,600
equities (that fallback previously bloated the DB and snapshots).
"""

from __future__ import annotations

import csv
import io
import json
import logging
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

ANGEL_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
NSE_LOTS_URL = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"
FALLBACK_PATH = Path(__file__).resolve().parents[1] / "data" / "fno_symbols.json"

INDEX_UNDERLYINGS = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYFPI"})
MIN_UNIVERSE = 120   # sanity bounds: NSE lists roughly 180-230 F&O stocks
MAX_UNIVERSE = 400
CACHE_TTL_S = 7 * 24 * 3600

_lock = threading.Lock()
_cache: tuple[float, frozenset[str]] | None = None
_extra: set[str] = set()      # symbols the live OI feed reports but Angel's master names differently (renames/demergers)
_source = "none"


def _is_test_symbol(symbol: str) -> bool:
    """NSE publishes dummy instruments (e.g. 011NSETEST) that Angel lists as stock futures."""
    return "NSETEST" in symbol or symbol.startswith("TEST")


def _valid(symbols: Iterable[str]) -> frozenset[str]:
    cleaned = frozenset(s.strip().upper() for s in symbols if s and s.strip())
    cleaned = frozenset(s for s in cleaned if not _is_test_symbol(s)) - INDEX_UNDERLYINGS
    return cleaned if MIN_UNIVERSE <= len(cleaned) <= MAX_UNIVERSE else frozenset()


def parse_angel_master(records: Iterable[dict]) -> frozenset[str]:
    """Underlying names of NFO stock futures from Angel's public scrip master."""
    names = (
        str(row.get("name") or "")
        for row in records
        if str(row.get("exch_seg") or "").upper() == "NFO"
        and str(row.get("instrumenttype") or "").upper() == "FUTSTK"
    )
    return _valid(names)


def parse_fo_mktlots(text: str) -> frozenset[str]:
    """Second CSV column of NSE's fo_mktlots.csv is the underlying symbol."""
    symbols: list[str] = []
    for row in csv.reader(io.StringIO(text)):
        if len(row) < 2:
            continue
        symbol = row[1].strip().upper()
        if not symbol or symbol == "SYMBOL" or " " in symbol:
            continue
        symbols.append(symbol)
    return _valid(symbols)


def _http_get(url: str, timeout: float = 30.0) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def _from_angel() -> frozenset[str]:
    """Stream the >100k-row master; hold only stock-future underlyings (memory-safe)."""
    from utils.jsonstream import iter_json_array
    text = _http_get(ANGEL_MASTER_URL, timeout=60).decode("utf-8", "ignore")
    try:
        return parse_angel_master(
            row for row in iter_json_array(text)
            if isinstance(row, dict)
            and str(row.get("exch_seg") or "").upper() == "NFO"
            and str(row.get("instrumenttype") or "").upper() == "FUTSTK"
        )
    finally:
        del text


def _from_nse() -> frozenset[str]:
    return parse_fo_mktlots(_http_get(NSE_LOTS_URL).decode("utf-8", "ignore"))


def _from_file() -> frozenset[str]:
    if not FALLBACK_PATH.exists():
        return frozenset()
    data = json.loads(FALLBACK_PATH.read_text())
    return _valid(data.get("symbols", []))


def _persist(symbols: frozenset[str], source: str) -> None:
    try:
        FALLBACK_PATH.parent.mkdir(parents=True, exist_ok=True)
        FALLBACK_PATH.write_text(json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source": source,
            "symbols": sorted(symbols),
        }, indent=1))
    except Exception:  # read-only or ephemeral disks must never break scans
        logger.debug("Could not persist F&O universe", exc_info=True)


def load_fno_universe(
    sources: list[tuple[str, Callable[[], frozenset[str]]]] | None = None,
    *,
    force: bool = False,
) -> frozenset[str]:
    """Return the F&O stock universe (cached for a week); empty if all sources fail."""
    global _cache, _source
    with _lock:
        if _cache and not force and time.time() - _cache[0] < CACHE_TTL_S:
            return _cache[1]
        chain = sources or [("angel_master", _from_angel), ("nse_fo_mktlots", _from_nse), ("committed_json", _from_file)]
        for name, loader in chain:
            try:
                symbols = loader()
            except Exception:
                logger.warning("F&O universe source failed: %s", name, exc_info=True)
                continue
            if symbols:
                _cache, _source = (time.time(), symbols), name
                if name != "committed_json" and sources is None:
                    _persist(symbols, name)
                logger.info("F&O universe loaded source=%s symbols=%d", name, len(symbols))
                return symbols
        logger.warning("F&O_UNIVERSE_UNAVAILABLE: all sources failed; equity storage is skipped (fail-closed)")
        return frozenset()


def universe_source() -> str:
    return _source


def add_extra_symbols(symbols: Iterable[str]) -> int:
    added = {s.strip().upper() for s in symbols if s and s.strip() and not _is_test_symbol(s.strip().upper())} - INDEX_UNDERLYINGS - _extra
    _extra.update(added)
    return len(added)


def fno_symbols() -> set[str]:
    return set(load_fno_universe()) | _extra


def cached_universe_size() -> int:
    """Size of the already-loaded universe; never touches the network."""
    return (len(_cache[1]) if _cache else 0) + len(_extra - set(_cache[1] if _cache else ()))


def cached_universe() -> set[str]:
    """Already-loaded universe (no network); empty until startup maintenance has run."""
    return (set(_cache[1]) if _cache else set()) | _extra
