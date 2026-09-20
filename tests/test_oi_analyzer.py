from datetime import time

from app.oi_analyzer import (
    SIGNAL_CAS_SHORT_COVERING,
    detect_cas_jump,
)


def test_detect_cas_jump_requires_window_and_oi_covering():
    assert detect_cas_jump("ATHER", 2.1, time(15, 35), -4.0) is True
    assert detect_cas_jump("ATHER", 2.1, time(15, 45), -4.0) is False
    assert detect_cas_jump("ATHER", 2.1, time(15, 35), -2.9) is False




def test_cas_signal_constant_is_available():
    assert SIGNAL_CAS_SHORT_COVERING == "CAS_SHORT_COVERING"




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
