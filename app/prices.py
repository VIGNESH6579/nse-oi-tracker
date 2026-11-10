import logging
import time
import requests

log = logging.getLogger(__name__)

_CACHE = {"at": 0.0, "quotes": {}}
_CACHE_TTL = 30
_YAHOO = "https://query1.finance.yahoo.com/v7/finance/quote"


def yahoo_symbol(nse_symbol: str) -> str:
    return f"{str(nse_symbol or '').strip().upper()}.NS"


def get_quotes(symbols: list[str] | set[str]) -> dict:
    wanted = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    if not wanted:
        return {}
    now = time.monotonic()
    cached = _CACHE.get("quotes") or {}
    if now - _CACHE.get("at", 0) <= _CACHE_TTL:
        hit = {symbol: cached[symbol] for symbol in wanted if symbol in cached}
        if len(hit) == len(wanted):
            return hit
    yahoo = ",".join(yahoo_symbol(symbol) for symbol in wanted)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }
    try:
        response = requests.get(
            _YAHOO,
            params={"symbols": yahoo},
            headers=headers,
            timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
        rows = ((payload.get("quoteResponse") or {}).get("result")) or []
        quotes = {}
        for row in rows:
            nse = str(row.get("symbol") or "").replace(".NS", "").replace(".BO", "").upper()
            ltp = row.get("regularMarketPrice")
            if ltp is None:
                continue
            quotes[nse] = {
                "ltp": float(ltp),
                "change": float(row.get("regularMarketChange") or 0),
                "change_pct": float(row.get("regularMarketChangePercent") or 0),
                "high": row.get("regularMarketDayHigh"),
                "low": row.get("regularMarketDayLow"),
                "volume": row.get("regularMarketVolume"),
                "source": "Yahoo Finance delayed quote",
            }
        _CACHE["at"] = now
        merged = dict(cached)
        merged.update(quotes)
        _CACHE["quotes"] = merged
        return {symbol: merged[symbol] for symbol in wanted if symbol in merged}
    except Exception as exc:
        log.warning("Quote fetch failed: %s", exc)
        return {symbol: cached[symbol] for symbol in wanted if symbol in cached}


def format_quote(quote: dict | None) -> dict:
    if not quote or quote.get("ltp") is None:
        return {"html": '<span class="px na">LTP n/a</span>', "cls": "flat", "text": "n/a"}
    change_pct = quote.get("change_pct")
    if change_pct is None:
        cls = "flat"
        chg = "0.00%"
    elif change_pct > 0:
        cls = "up"
        chg = f"+{change_pct:.2f}%"
    elif change_pct < 0:
        cls = "down"
        chg = f"{change_pct:.2f}%"
    else:
        cls = "flat"
        chg = "0.00%"
    ltp = f"₹{quote['ltp']:,.2f}"
    html = f'<span class="px {cls}"><em>{ltp}</em> <b>{chg}</b></span>'
    return {"html": html, "cls": cls, "text": f"{ltp} {chg}"}
