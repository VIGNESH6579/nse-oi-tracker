# oi_analyzer.py — Signal engine using live-analysis-oi-spurts-underlyings
#
# Data source: single NSE endpoint that returns ALL F&O stocks with:
#   - OI change %  (direction tells us: OI up = buildup, OI down = unwinding/covering)
#   - Price change % (direction tells us: price up = bulls, price down = bears)
#
# Classification matrix:
#   Price ↑ + OI ↑ → LONG_BUILDUP   (BUY  — strong)
#   Price ↓ + OI ↑ → SHORT_BUILDUP  (SELL — strong)
#   Price ↑ + OI ↓ → SHORT_COVERING (BUY  — weak, quick)
#   Price ↓ + OI ↓ → LONG_UNWINDING (SELL — weak, quick)

from __future__ import annotations
import logging
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
from app.config import (
    PRICE_CHANGE_THRESHOLD, OI_CHANGE_THRESHOLD,
    MIN_OI_ABSOLUTE, CONFIDENCE_HIGH, CONFIDENCE_MEDIUM,
    PUBLISH_MIN_CONFIDENCE, PUBLISH_MIN_OI_ABSOLUTE,
    QUALITY_REQUIRED_SCANS, QUALITY_MAX_SIGNALS,
)
from app.nse_fetcher import (
    fetch_all_fno_oi_change,
)
from app.angel_one import angel_one
from config.settings import get_settings

logger = logging.getLogger(__name__)

# ── Signal constants ──────────────────────────────────────────────────────────
SIGNAL_LONG_BUILDUP   = "LONG_BUILDUP"
SIGNAL_SHORT_BUILDUP  = "SHORT_BUILDUP"
SIGNAL_SHORT_COVERING = "SHORT_COVERING"
SIGNAL_LONG_UNWINDING = "LONG_UNWINDING"
SIGNAL_CAS_SHORT_COVERING = "CAS_SHORT_COVERING"
SIGNAL_NEUTRAL        = "NEUTRAL"

SIGNAL_META = {
    SIGNAL_LONG_BUILDUP:   {"label":"Long Buildup",   "emoji":"🟢","color":"green",  "bias":"Bullish",      "direction":"BUY"},
    SIGNAL_SHORT_BUILDUP:  {"label":"Short Buildup",  "emoji":"🔴","color":"red",    "bias":"Bearish",      "direction":"SELL"},
    SIGNAL_SHORT_COVERING: {"label":"Short Covering", "emoji":"🟡","color":"yellow", "bias":"Bullish Fade", "direction":"BUY"},
        SIGNAL_LONG_UNWINDING: {"label":"Long Unwinding", "emoji":"🟠","color":"orange", "bias":"Bearish Fade",     "direction":"SELL"},
    SIGNAL_CAS_SHORT_COVERING: {"label":"CAS Short Covering", "emoji":"⚡","color":"lime", "bias":"Strong Bullish Next Day", "direction":"BUY"},
    SIGNAL_NEUTRAL:        {"label":"Neutral",        "emoji":"⚪","color":"gray",   "bias":"Sideways",     "direction":"NONE"},
}

# Used by main.py /api/category route
CATEGORY_TO_SIGNAL = {
    "long_buildup":   SIGNAL_LONG_BUILDUP,
    "short_buildup":  SIGNAL_SHORT_BUILDUP,
    "short_covering": SIGNAL_SHORT_COVERING,
    "long_unwinding": SIGNAL_LONG_UNWINDING,
}


# ── Signal classifier ─────────────────────────────────────────────────────────

_IST = ZoneInfo("Asia/Kolkata")
_field_usage: dict[str, dict[str, str | None]] = {}
_last_cas_time_ist: str | None = None
_last_scan_data_status = "NOT_RUN"
# symbol -> (direction, consecutive scan count). A reversal resets the count,
# preventing rapid opposing signals for the same stock from being published.
_signal_stability: dict[str, tuple[str, int]] = {}


def last_scan_data_status() -> str:
    """Return whether the latest scan had upstream rows or was unavailable."""
    return _last_scan_data_status


def sample_field_usage(limit: int = 5) -> dict[str, dict[str, str | None]]:
    """
    A few real symbols' resolved field names, for /api/debug.
    Previously this hardcoded two specific ticker symbols (ATHER, POLICYBZR)
    which silently returned nothing once those symbols dropped out of the
    F&O list or were renamed. This just samples whatever was actually parsed
    on the most recent scan.
    """
    return dict(list(_field_usage.items())[:limit])


def detect_cas_jump(symbol: str, price_change_pct: float, time_ist, oi_change_pct: float) -> bool:
    """Return true for a strong 15:30–15:40 IST price jump with OI covering."""
    try:
        if isinstance(time_ist, datetime):
            current = time_ist.astimezone(_IST).time()
        elif hasattr(time_ist, "hour"):
            current = time_ist
        else:
            current = datetime.strptime(str(time_ist).strip(), "%H:%M:%S").time()
        return bool(dt_time(15, 30) <= current < dt_time(15, 40)
                    and price_change_pct > 1.5 and oi_change_pct < -3.0)
    except (TypeError, ValueError):
        return False


from analytics.oi_window import OIWindow
from utils.time import now_ist

oi_window = OIWindow()
_last_scan_stats: dict = {"rows": 0, "parsed": 0, "candidates": 0}


def classify_signal(price_change_pct: float, oi_change_pct: float) -> str:
    price_up = price_change_pct >= PRICE_CHANGE_THRESHOLD
    price_dn = price_change_pct <= -PRICE_CHANGE_THRESHOLD
    oi_up    = oi_change_pct    >= OI_CHANGE_THRESHOLD
    oi_dn    = oi_change_pct    <= -OI_CHANGE_THRESHOLD
    if price_up and oi_up:  return SIGNAL_LONG_BUILDUP
    if price_dn and oi_up:  return SIGNAL_SHORT_BUILDUP
    if price_up and oi_dn:  return SIGNAL_SHORT_COVERING
    if price_dn and oi_dn:  return SIGNAL_LONG_UNWINDING
    return SIGNAL_NEUTRAL


# ── High-confidence scoring ───────────────────────────────────────────────────

def confidence_score(price_chg_p: float, oi_chg_p: float, oi_abs: float) -> int:
    """
    Composite confidence 0–100.
    Components:
      Price strength  (0–35 pts): how far price moved from flat
      OI conviction   (0–45 pts): how strongly OI changed
      Liquidity       (0–20 pts): absolute OI size (illiquid stocks filtered)

    Tier thresholds are defined in config.py (CONFIDENCE_HIGH / CONFIDENCE_MEDIUM)
    — do not hardcode numbers in this docstring, they will drift out of sync
    with the actual thresholds again, which is exactly the bug this comment
    used to have (it said "HIGH ≥ 65" while config.py said 75).
    """
    score = 0
    p = abs(price_chg_p)
    if   p >= 3.0: score += 35
    elif p >= 2.0: score += 28
    elif p >= 1.0: score += 20
    elif p >= 0.5: score += 12
    elif p >= 0.4: score += 6

    o = abs(oi_chg_p)
    if   o >= 20: score += 45
    elif o >= 15: score += 38
    elif o >= 10: score += 30
    elif o >=  7: score += 22
    elif o >=  5: score += 15
    elif o >=  3: score += 8

    if   oi_abs >= 1_000_000: score += 20
    elif oi_abs >= 500_000:   score += 16
    elif oi_abs >= 100_000:   score += 12
    elif oi_abs >= 50_000:    score += 8
    elif oi_abs >= 10_000:    score += 4

    return min(score, 100)


def confidence_tier(score: int) -> str:
    if score >= CONFIDENCE_HIGH:   return "HIGH"
    if score >= CONFIDENCE_MEDIUM: return "MEDIUM"
    return "LOW"


def score_factors(price_chg_p: float, oi_chg_p: float, oi_abs: float) -> tuple[list[str], list[str]]:
    """Return transparent evidence behind the 0–100 confidence score."""
    confirmed: list[str] = []
    missing: list[str] = []
    p, o = abs(price_chg_p), abs(oi_chg_p)
    if p >= 0.4:
        confirmed.append(f"price move {price_chg_p:+.2f}%")
    else:
        missing.append("price confirmation below 0.40%")
    if o >= 3:
        confirmed.append(f"OI change {oi_chg_p:+.2f}%")
    else:
        missing.append("OI change below 3.00%")
    if oi_abs >= MIN_OI_ABSOLUTE:
        confirmed.append(f"liquidity {int(oi_abs):,} OI")
    else:
        missing.append(f"OI liquidity below {MIN_OI_ABSOLUTE:,}")
    return confirmed, missing


def signal_strength(price_chg_p: float, oi_chg_p: float) -> float:
    return round(min(abs(price_chg_p)/2.0, 50) + min(abs(oi_chg_p)/5.0, 50), 1)


# ── Safe field helpers ────────────────────────────────────────────────────────

def _f(val) -> float:
    """Safe float — returns 0.0 on None/empty/error."""
    if val is None or val == "" or val == "-":
        return 0.0
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _symbol(row: dict) -> str:
    """Extract symbol from NSE row — handles all known field name variants."""
    for key in ("underlying", "symbol", "UNDERLYING", "SYMBOL"):
        v = row.get(key)
        if v and isinstance(v, str):
            return v.strip().upper()
    return ""


def _first_numeric(row: dict, fields: tuple[str, ...]) -> tuple[float, str | None]:
    """Use the first non-empty numeric field and return both value and field name."""
    for field in fields:
        if field in row and row[field] not in (None, "", "-"):
            return _f(row[field]), field
    return 0.0, None


def _build_signal_row(sym, ltp, price_chg, price_chg_p, oi, oi_chg, oi_chg_p,
                      signal, is_cas_jump=False, low_liquidity=False, source=None) -> dict:
    meta = SIGNAL_META[signal]
    conf = confidence_score(price_chg_p, oi_chg_p, oi)
    if is_cas_jump and signal == SIGNAL_CAS_SHORT_COVERING:
        conf = min(conf + 15, 100)
    tier = "LOW" if low_liquidity else confidence_tier(conf)
    strg = signal_strength(price_chg_p, oi_chg_p)
    confirmed, missing = score_factors(price_chg_p, oi_chg_p, oi)
    actionable = bool(conf >= CONFIDENCE_HIGH and signal != SIGNAL_NEUTRAL and not low_liquidity)
    return {
        "symbol":           sym,
        "data_source":      source or "NSE live-analysis-oi-spurts-underlyings",
        "ltp":              round(ltp, 2),
        "price_change":     round(price_chg, 2),
        "price_change_pct": round(price_chg_p, 2),
        "oi":               int(oi),
        "oi_change":        int(oi_chg),
        "oi_change_pct":    round(oi_chg_p, 2),
        "signal":           signal,
        "signal_label":     meta["label"],
        "signal_emoji":     meta["emoji"],
        "signal_color":     meta["color"],
        "signal_bias":      meta["bias"],
        "signal_direction": meta["direction"],
        "strength":         strg,
        "confidence":       conf,
        "score":            conf,
        "confidence_tier":  tier,
        "confirmed_factors": confirmed,
        "missing_factors":   missing,
        "trade_recommendation": "NO_TRADE",
        "actionable":       actionable,
        "is_cas_jump": bool(is_cas_jump),
    }


def _track_window(row: dict, scan_time) -> None:
    sym = _symbol(row)
    if not sym:
        return
    ltp = _f(row.get("ltp") or row.get("lastPrice") or row.get("ltP") or row.get("LTP")
             or row.get("price") or row.get("underlyingValue") or 0)
    oi, _field = _first_numeric(row, ("oi", "openInterest", "OI", "openinterest", "latestOI", "totalOI"))
    oi_window.update(sym, scan_time, ltp, oi or 0)


def _parse_row(row: dict) -> dict | None:
    """
    Parse one row from live-analysis-oi-spurts-underlyings.
    Handles ALL known NSE field name variants (NSE changes names without notice).
    """
    sym = _symbol(row)
    if not sym:
        return None

    ltp = _f(
        row.get("ltp") or row.get("lastPrice") or row.get("ltP") or
        row.get("LTP") or row.get("price") or
        row.get("underlyingValue") or 0
    )

    price_chg_p = _f(
        row.get("pChange") or row.get("perChange") or
        row.get("changePer") or row.get("change_p") or
        row.get("perchange") or row.get("pchange") or
        row.get("percentChange") or 0
    )

    price_chg = _f(
        row.get("change") or row.get("priceChange") or
        row.get("netChange") or 0
    )

    oi, oi_field = _first_numeric(row, (
        "oi", "openInterest", "OI", "openinterest", "latestOI", "totalOI",
    ))
    oi_chg, oi_chg_field = _first_numeric(row, (
        "oiChange", "changeinOpenInterest", "changeInOI", "COI",
    ))
    oi_chg_p, oi_chg_p_field = _first_numeric(row, (
        "oiChangePct", "perOIchange", "oiChangePer", "changeOI_pct",
        "perOIChange", "oiChangePercent", "pOIchng", "oichngper",
        "oiChangePercentage", "changeInOIPercent", "pOIChange",
    ))
    _field_usage[sym] = {
        "oi_field": oi_field,
        "oi_change_field": oi_chg_field,
        "oi_change_pct_field": oi_chg_p_field,
    }

    # Derive OI% if NSE omits it or reports zero.
    if oi_chg_p == 0 and oi_chg != 0 and (oi - oi_chg) > 0:
        oi_chg_p = (oi_chg / (oi - oi_chg)) * 100

    if ltp == 0:
        return None

    low_liquidity = oi > 0 and oi < MIN_OI_ABSOLUTE
    signal = classify_signal(price_chg_p, oi_chg_p)
    global _last_cas_time_ist
    scan_time = datetime.now(_IST)
    cas_jump = detect_cas_jump(sym, price_chg_p, scan_time, oi_chg_p)
    if cas_jump:
        _last_cas_time_ist = scan_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        signal = SIGNAL_CAS_SHORT_COVERING
    if signal == SIGNAL_NEUTRAL:
        return None

    result = _build_signal_row(sym, ltp, price_chg, price_chg_p, oi, oi_chg, oi_chg_p,
                               signal, is_cas_jump=cas_jump, low_liquidity=low_liquidity,
                               source=row.get("_data_source"))

    # Keep LOW rows classified for diagnostics; scan_all_fno_realtime filters them.
    return result


# ── PRIMARY SCANNER ───────────────────────────────────────────────────────────

def scan_all_fno_realtime() -> list[dict]:
    """
    Scan ALL NSE F&O stocks for high-confidence signals.

    Uses: /api/live-analysis-oi-spurts-underlyings
    This single endpoint returns OI + price data for ALL ~200 F&O underlyings.
    We classify all 4 signal types from the price + OI direction combination.

    Returns only high-confidence, liquid signals, sorted by confidence. Medium
    and low-confidence rows are computed for diagnostics but deliberately
    dropped from the live dashboard to avoid noisy signal spam.

    Quality policy: a row must meet the publish confidence and absolute-OI
    gates. Directional classification alone is not enough to publish a signal.
    """
    global _last_scan_data_status
    rows = fetch_all_fno_oi_change()
    # Angel's full quote overlay creates large per-scan batches. Keep it off by
    # default on Render Free; enable only with a memory budget for paid tiers.
    if rows and angel_one.configured and get_settings().angel_overlay_enabled:
        symbols = [_symbol(row) for row in rows if _symbol(row)]
        angel_quotes = {}
        for start in range(0, len(symbols), 50):
            angel_quotes.update(angel_one.quotes(symbols[start:start + 50]))
        for row in rows:
            quote = angel_quotes.get(_symbol(row))
            if quote:
                row["ltp"] = quote["ltp"]
                row["latestOI"] = quote["oi"]
                row["changeInOI"] = quote["oi_change"]
                row["oiChangePct"] = quote["oi_change_pct"]
                row["_data_source"] = "Angel One FULL quote"

    if not rows:
        _last_scan_data_status = "UNAVAILABLE"
        logger.info("No data returned (market closed or NSE temporarily unavailable)")
        return []

    _last_scan_data_status = "RECEIVED"

    results:  list[dict] = []
    seen:     set[str]   = set()

    scan_time = now_ist()
    parsed_count = 0
    for row in rows:
        # Feed EVERY raw row (incl. currently neutral ones, which _parse_row drops) so
        # 15/30/60-minute windows already exist the moment a symbol becomes a candidate.
        _track_window(row, scan_time)
        result = _parse_row(row)
        if result is not None:
            parsed_count += 1
        if (
            result is None
            or result["confidence"] < PUBLISH_MIN_CONFIDENCE
            or result["oi"] < PUBLISH_MIN_OI_ABSOLUTE
        ):
            continue
        if result["symbol"] in seen:
            continue
        seen.add(result["symbol"])
        symbol = result["symbol"]
        direction = result.get("signal_direction") or result.get("direction") or ""
        previous_direction, previous_count = _signal_stability.get(symbol, ("", 0))
        count = previous_count + 1 if previous_direction == direction else 1
        _signal_stability[symbol] = (direction, count)
        if count < QUALITY_REQUIRED_SCANS:
            continue
        result["quality_gate"] = "CONFIRMED_TWO_SCAN_DIRECTION"
        result["stability_scans"] = count
        window_ctx = oi_window.context(symbol)
        window_ctx["streak"] = oi_window.note_agreement(symbol, window_ctx.get("window_signal") == result["signal"], scan_time)
        result["oi_window"] = window_ctx
        results.append(result)
    # Sort strongest first and expose only a small quality feed.
    results.sort(key=lambda r: (-r["confidence"], -r["strength"], r["symbol"]))
    results = results[:QUALITY_MAX_SIGNALS]
    _last_scan_stats.update(rows=len(rows), parsed=parsed_count, candidates=len(results),
                            oi_source="angel_futures" if any(r.get("_data_source") for r in rows[:5]) else "nse_oi_spurts")

    high   = sum(1 for r in results if r["confidence_tier"] == "HIGH")
    medium = sum(1 for r in results if r["confidence_tier"] == "MEDIUM")
    logger.info(
        f"Scan complete: {len(rows)} F&O stocks checked → "
        f"{len(results)} signals ({high} HIGH ⭐⭐⭐, {medium} MEDIUM ⭐⭐)"
    )
    return results


# ── Dummy parse for /api/category route (back-compat) ────────────────────────

def _parse_buildup_row(row: dict, signal: str) -> dict | None:
    """Parse a raw row and force a specific signal type (used by /api/category)."""
    sym = _symbol(row)
    if not sym:
        return None
    ltp         = _f(row.get("ltp") or row.get("lastPrice") or row.get("underlyingValue") or 0)
    price_chg_p = _f(row.get("pChange") or row.get("perChange") or row.get("percentChange") or 0)
    price_chg   = _f(row.get("change") or row.get("priceChange") or row.get("netChange") or 0)
    oi          = _f(row.get("oi") or row.get("openInterest") or row.get("latestOI") or 0)
    oi_chg      = _f(row.get("oiChange") or row.get("changeinOpenInterest") or row.get("changeInOI") or 0)
    oi_chg_p    = _f(row.get("oiChangePct") or row.get("perOIchange") or row.get("oiChangePercent") or 0)
    if ltp == 0:
        return None
    return _build_signal_row(sym, ltp, price_chg, price_chg_p, oi, oi_chg, oi_chg_p, signal)


# ── OPTION CHAIN ──────────────────────────────────────────────────────────────



