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


def _iter_dicts(value: Any):
    """Yield every nested mapping so MCP wrapper shapes cannot hide the history rows."""
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_dicts(child)


def _rows_from_result(result: Any, symbol: str) -> list[dict]:
    payloads: list[Any] = []
    for attr in ("structuredContent", "structured_content"):
        structured = getattr(result, attr, None)
        if structured is not None:
            payloads.append(structured)
    for item in getattr(result, "content", []) or []:
        text = getattr(item, "text", None)
        if not text:
            continue
        try:
            payloads.append(json.loads(text))
        except (TypeError, json.JSONDecodeError):
            continue

    def normalized(mapping: dict) -> dict[str, Any]:
        return {
            "".join(ch for ch in str(key).lower() if ch.isalnum()): value
            for key, value in mapping.items()
        }

    def pick(row: dict[str, Any], *names: str):
        data = normalized(row)
        for name in names:
            value = data.get("".join(ch for ch in name.lower() if ch.isalnum()))
            if value not in (None, ""):
                return value
        return None

    rows: list[dict] = []
    for payload in payloads:
        for row in _iter_dicts(payload):
            raw_date = str(pick(
                row, "date", "tradeDate", "trade_date", "businessDate",
                "tradingDate", "timestamp", "mTIMESTAMP", "TradDt",
            ) or "")
            parsed = None
            date_text = raw_date.strip()
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y", "%d/%m/%Y", "%Y/%m/%d"):
                try:
                    parsed = (
                        date.fromisoformat(date_text[:10])
                        if fmt in {"%Y-%m-%d", "%Y/%m/%d"}
                        else __import__("datetime").datetime.strptime(date_text[:10], fmt).date()
                    )
                    break
                except ValueError:
                    continue
            if parsed is None:
                continue
            try:
                raw_values = [
                    pick(row, "open", "openPrice", "open_price", "OpnPric"),
                    pick(row, "high", "highPrice", "high_price", "HghPric"),
                    pick(row, "low", "lowPrice", "low_price", "LwPric"),
                    pick(row, "close", "closePrice", "close_price", "ClsPric"),
                    pick(row, "volume", "tradedVolume", "totalVolume", "totalTradedQuantity", "TtlTradgVol") or 0,
                ]
                values = [float(str(value).replace(",", "")) for value in raw_values]
            except (TypeError, ValueError):
                continue
            if values[3] <= 0 or values[1] < values[2] or values[0] <= 0:
                continue
            rows.append({
                "symbol": str(pick(row, "symbol", "SYMBOL", "TckrSymb") or symbol).upper(),
                "trade_date": parsed.isoformat(),
                "open": values[0], "high": values[1], "low": values[2],
                "close": values[3], "volume": values[4],
            })
    unique = {(row["symbol"], row["trade_date"]): row for row in rows}
    return sorted(unique.values(), key=lambda row: row["trade_date"])


async def _fetch_worker(
    symbols: list[str],
    end_date: date,
    delay_seconds: float,
    worker_id: int,
) -> dict[str, list[dict]]:
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
    LOGGER.info("NSE Bhavcopy MCP worker=%d completed symbols=%d rows=%d",
                worker_id, len(symbols), sum(len(rows) for rows in output.values()))
    return output


async def _fetch_batch(symbols: list[str], end_date: date, delay_seconds: float) -> dict[str, list[dict]]:
    """Fetch with a small fixed worker pool so startup can finish within its bound."""
    workers = 1  # preserve BACKFILL_MAX_PER_MIN global pacing; concurrency is not rate-limit safe
    chunks = [symbols[index::workers] for index in range(workers)]
    results = await asyncio.gather(
        *(_fetch_worker(chunk, end_date, delay_seconds, index + 1)
          for index, chunk in enumerate(chunks)),
        return_exceptions=True,
    )
    output: dict[str, list[dict]] = {}
    errors = []
    for result in results:
        if isinstance(result, Exception):
            errors.append(result)
            continue
        output.update(result)
    if errors and not output:
        raise RuntimeError(f"NSE Bhavcopy MCP workers all failed: {errors[0]}")
    if errors:
        LOGGER.warning("NSE Bhavcopy MCP partial worker failure count=%d", len(errors))
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
