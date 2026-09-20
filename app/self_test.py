"""Automatic data-source self-test (pre-open and post-open).

Proves - with real calls, not assumptions - that every input the signal engine
needs is arriving and parsed correctly. Results are logged (SELF_TEST ...),
exposed in /api/health (readiness + data_quality.self_test) and never contain secrets.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Callable

from analytics.intraday_confirm import candle_minute

logger = logging.getLogger(__name__)
Probe = Callable[[], tuple[bool, str]]
_lock = threading.Lock()
_latest: dict[str, dict[str, Any]] = {}
SAMPLE = ["RELIANCE", "TCS", "INFY"]

# A failing critical check means no trustworthy signal can be produced -> BLOCKED.
CRITICAL = {
    "pre_open": {"angel_equity_quotes", "angel_session", "angel_futures_oi", "bars_coverage", "fno_universe"},
    "post_open": {"oi_rows", "angel_intraday_candles"},
}


def build_probes(stage: str, *, angel, repository, universe: Callable[[], set[str]], ban_info: Callable[[], dict],
                 scan_stats: Callable[[], dict], window_depth: Callable[[], dict], min_bars: int = 15) -> dict[str, Probe]:
    def equity_quotes():
        quotes = angel.full_quotes(SAMPLE)
        good = [s for s in SAMPLE if float((quotes.get(s) or {}).get("ltp") or 0) > 0]
        return len(good) == len(SAMPLE), f"{len(good)}/{len(SAMPLE)} symbols with ltp>0"

    def session():
        state = angel.health().get("state")
        return state == "authenticated", f"state={state}"

    def futures_oi():
        rows = angel.fno_quotes(SAMPLE)
        good = [s for s in SAMPLE if float((rows.get(s) or {}).get("oi") or 0) > 0 and float((rows.get(s) or {}).get("ltp") or 0) > 0]
        rolled = sum(1 for s in SAMPLE if (rows.get(s) or {}).get("oi_includes_next_month"))
        return len(good) == len(SAMPLE), f"{len(good)}/{len(SAMPLE)} rows with oi>0 and ltp>0; next_month_oi_added={rolled}"

    def daily_candles():
        candles = angel.daily_candles("NIFTY", days=5)
        return len(candles) >= 3, f"{len(candles)} NIFTY daily candles"

    def bars_coverage():
        symbols = universe()
        if not symbols:
            return False, "F&O universe empty"
        missing = repository.symbols_missing_bars(symbols, min_bars)
        pct = 100.0 * (1 - len(missing) / len(symbols))
        return pct >= 90.0, f"{pct:.1f}% of {len(symbols)} symbols have >={min_bars} bars; missing={missing[:25]}"

    def universe_size():
        n = len(universe())
        return 150 <= n <= 300, f"{n} symbols"

    def ban_list():
        info = ban_info()
        return bool(info.get("ban_list_ok")), f"size={info.get('ban_list_size')} age_s={info.get('ban_list_age_s')}"

    def index_bars():
        n = repository.index_bar_count("NIFTY")
        return n >= 20, f"{n} NIFTY index bars"

    def oi_rows():
        stats = scan_stats()
        rows = int(stats.get("rows") or 0)
        return rows >= 150, f"rows={rows} source={stats.get('oi_source')}"

    def window_ok():
        depth = window_depth()
        return float(depth.get("median_minutes") or 0) >= 10, f"symbols={depth.get('symbols')} median_minutes={depth.get('median_minutes')}"

    def intraday_candles():
        candles = angel.intraday_candles("NIFTY")
        first = candle_minute(candles[0]) if candles else None
        # 09:15 IST == minute 555: proves the session window is IST and starts at the open.
        return len(candles) >= 3 and first == 555, f"{len(candles)} candles, first_minute={first} (expected 555)"

    if stage == "pre_open":
        return {"angel_equity_quotes": equity_quotes, "angel_session": session, "angel_futures_oi": futures_oi,
                "angel_daily_candles": daily_candles, "bars_coverage": bars_coverage, "fno_universe": universe_size,
                "index_bars": index_bars, "ban_list": ban_list}
    if stage == "post_open":
        return {"oi_rows": oi_rows, "oi_window_depth": window_ok, "angel_intraday_candles": intraday_candles}
    raise ValueError(f"unknown stage {stage!r}")


def run_stage(stage: str, probes: dict[str, Probe], now: datetime) -> dict[str, Any]:
    checks: dict[str, dict[str, Any]] = {}
    for name, probe in probes.items():
        try:
            ok, detail = probe()
        except Exception as exc:  # a probe must never crash the app
            ok, detail = False, f"{type(exc).__name__}: {str(exc)[:140]}"
        checks[name] = {"ok": bool(ok), "detail": detail}
    failed = [name for name, check in checks.items() if not check["ok"]]
    critical = [name for name in failed if name in CRITICAL.get(stage, set())]
    verdict = "READY" if not failed else ("BLOCKED" if critical else "DEGRADED")
    result = {"stage": stage, "at": now.isoformat(timespec="seconds"), "verdict": verdict, "failed": failed, "checks": checks}
    with _lock:
        _latest[stage] = result
    (logger.info if verdict == "READY" else logger.warning)("SELF_TEST stage=%s verdict=%s failed=%s", stage, verdict, failed)
    for name in failed:
        logger.warning("SELF_TEST_FAIL stage=%s check=%s detail=%s", stage, name, checks[name]["detail"])
    return result


def latest() -> dict[str, dict[str, Any]]:
    with _lock:
        return dict(_latest)


def readiness() -> str:
    """Verdict of the most recent stage, or UNTESTED before the first run."""
    with _lock:
        if not _latest:
            return "UNTESTED"
        return max(_latest.values(), key=lambda r: r["at"])["verdict"]
