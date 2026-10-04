from types import SimpleNamespace
import json

from integrations.nse_bhavcopy_mcp import _rows_from_result


def test_rows_from_result_parses_nested_json_text():
    payload = {
        "data": json.dumps([
            {
                "symbol": "RELIANCE",
                "date": "2026-10-01",
                "open": 100.0,
                "high": 110.0,
                "low": 95.0,
                "close": 108.0,
                "volume": 12345,
            }
        ])
    }
    result = SimpleNamespace(
        structuredContent=None,
        structured_content=None,
        content=[SimpleNamespace(text=json.dumps(payload))],
    )
    rows = _rows_from_result(result, "RELIANCE")
    assert rows == [{
        "symbol": "RELIANCE",
        "trade_date": "2026-10-01",
        "open": 100.0,
        "high": 110.0,
        "low": 95.0,
        "close": 108.0,
        "volume": 12345.0,
    }]
