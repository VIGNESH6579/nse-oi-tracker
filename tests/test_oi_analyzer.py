def test_oi_change_fallback_is_used_when_percent_missing():
    from app import oi_analyzer
    row = {
        "symbol": "ATHER", "ltp": 100, "pChange": 2.0,
        "oi": 26000, "oiChange": -1182,
    }
    parsed = oi_analyzer._parse_row(row)
    assert parsed is not None
    assert parsed["oi_change_pct"] == -4.35
    assert oi_analyzer._field_usage["ATHER"]["oi_change_field"] == "oiChange"
