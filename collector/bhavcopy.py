"""NSE equity bhavcopy parsing and collection."""

from __future__ import annotations

import csv
from datetime import date, datetime
from io import StringIO

from app.nse_fetcher import fetch_equity_bhavcopy


def _number(value: object) -> float:
    try:
        return float(str(value or "").replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def parse_equity_bhavcopy(csv_text: str) -> list[dict[str, float | str]]:
    """Return normalized EQ-series daily bars from one public NSE archive."""
    bars: list[dict[str, float | str]] = []
    # NSE's full bhavcopy headers include a space after each comma.  Let the
    # CSV parser consume that formatting rather than silently treating every
    # row as a non-EQ series.
    for row in csv.DictReader(StringIO(csv_text), skipinitialspace=True):
        series = str(row.get("SERIES") or row.get("SctySrs") or "").strip().upper()
        if series != "EQ":
            continue
        symbol = str(row.get("SYMBOL") or row.get("TckrSymb") or "").strip().upper()
        raw_date = str(row.get("DATE1") or row.get("TradDt") or "").strip()
        try:
            trade_date = datetime.strptime(raw_date, "%d-%b-%Y").date().isoformat()
        except ValueError:
            try:
                trade_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").date().isoformat()
            except ValueError:
                continue
        if not symbol:
            continue
        bars.append(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "open": _number(row.get("OPEN_PRICE") or row.get("OpnPric")),
                "high": _number(row.get("HIGH_PRICE") or row.get("HghPric")),
                "low": _number(row.get("LOW_PRICE") or row.get("LwPric")),
                "close": _number(row.get("CLOSE_PRICE") or row.get("ClsPric")),
                "volume": _number(row.get("TTL_TRD_QNTY") or row.get("TtlTradgVol")),
            }
        )
    return bars


def collect_equity_bhavcopy(trade_date: date) -> list[dict[str, float | str]]:
    """Download and parse all EQ daily bars, returning an empty list on gaps."""
    content = fetch_equity_bhavcopy(trade_date)
    return parse_equity_bhavcopy(content) if content else []
