"""Official NSE Bhavcopy MCP client used when Render cannot reach NSE archives.

The NSE Bhavcopy MCP endpoint is read-only and requires no credentials.
It provides daily OHLCV history; today's live values remain on the existing
Angel/WebSocket pipeline.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import date
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

LOGGER = logging.getLogger(__name__)
NSE_BHAVCOPY_MCP_URL = "https://mcp.nseindia.in/bhavcopy/cm/mcp"


def _date_arg(properties: dict[str, Any], names: tuple[str, ...], value: date) -> tuple[str, str] | None:
    for name in names:
        if name in properties:
            return name, value.strftime("%Y-%m-%d")
    return None


def _rows_from_result(result: Any, symbol: str) -> list[dict]:
    texts = [item.text for item in getattr(result, "content", []) if getattr(item, "text", None)]
    rows: list[dict] = []
    for text in texts:
        try:
            payload = json.loads(text)
        except (TypeError, json.JSONDecodeError):
            continue
        candidates = payload.get("data") if isinstance(payload, dict) else payload
        if isinstance(payload, dict) and isinstance(candidates, dict):
            candidates = candidates.get("data") or candidates.get("rows") or candidates.get("history")
        if not isinstance(candidates, list):
            continue
        for row in candidates:
            if not isinstance(row, dict):
                continue
            def pick(*keys):
                for key in keys:
                    if row.get(key) not in (None, ""):
                        return row[key]
                return None
            raw_date = str(pick("date", "tradeDate", "TradDt", "DATE", "mTIMESTAMP") or "")
            parsed = None
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y"):
                try:
                    parsed = date.fromisoformat(raw_date[:10]) if fmt == "%Y-%m-%d" else __import__("datetime").datetime.strptime(raw_date[:10], fmt).date()
                    break
                except ValueError:
                    continue
            if parsed is None:
                continue
            try:
                values = [
                    float(pick("open", "Open", "OpnPric")),
                    float(pick("high", "High", "HghPric")),
                    float(pick("low", "Low", "LwPric")),
                    float(pick("close", "Close", "ClsPric")),
                    float(pick("volume", "Volume", "TtlTradgVol") or 0),
                ]
            except (TypeError, ValueError):
                continue
            if values[3] <= 0 or values[1] < values[2] or values[0] <= 0:
                continue
            rows.append({
                "symbol": str(pick("symbol", "SYMBOL", "TckrSymb") or symbol).upper(),
                "trade_date": parsed.isoformat(),
                "open": values[0], "high": values[1], "low": values[2],
                "close": values[3], "volume": values[4],
            })
    unique = {(row["symbol"], row["trade_date"]): row for row in rows}
    return sorted(unique.values(), key=lambda row: row["trade_date"])


async def _fetch_batch(symbols: list[str], end_date: date, delay_seconds: float) -> dict[str, list[dict]]:
    output: dict[str, list[dict]] = {}
    async with streamablehttp_client(NSE_BHAVCOPY_MCP_URL) as (read_stream, write_stream, _session_id):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools = await session.list_tools()
            stock_tool = next((tool for tool in tools.tools if tool.name == "get_stock_history"), None)
            if stock_tool is None:
                raise RuntimeError("NSE Bhavcopy MCP does not expose get_stock_history")
            schema = getattr(stock_tool, "inputSchema", {}) or {}
            properties = schema.get("properties", {})
            for index, symbol in enumerate(symbols):
                args: dict[str, Any] = {}
                if "symbol" in properties:
                    args["symbol"] = symbol
                else:
                    raise RuntimeError("NSE Bhavcopy MCP get_stock_history has no symbol parameter")
                start = _date_arg(properties, ("startDate", "start_date", "fromDate", "from_date"), end_date)
                finish = _date_arg(properties, ("endDate", "end_date", "toDate", "to_date"), end_date)
                if start:
                    args[start[0]] = (end_date.fromordinal(end_date.toordinal() - 100)).strftime("%Y-%m-%d")
                if finish:
                    args[finish[0]] = finish[1]
                result = await session.call_tool("get_stock_history", arguments=args)
                output[symbol] = _rows_from_result(result, symbol)
                if delay_seconds > 0 and index + 1 < len(symbols):
                    await asyncio.sleep(delay_seconds)
    return output


def fetch_symbol_history_batch(symbols: list[str], end_date: date, delay_seconds: float = 6.0) -> dict[str, list[dict]]:
    """Synchronously fetch a bounded symbol batch through the official NSE MCP server."""
    if not symbols:
        return {}
    started = time.monotonic()
    result = asyncio.run(_fetch_batch(symbols, end_date, delay_seconds))
    LOGGER.info(
        "NSE Bhavcopy MCP batch completed symbols=%d rows=%d duration=%.1fs",
        len(symbols), sum(len(rows) for rows in result.values()), time.monotonic() - started,
    )
    return result
