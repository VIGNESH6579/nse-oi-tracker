from pathlib import Path

from app.finance import (
    analysis_from_parsed,
    classify_impact,
    indian_fy_quarter,
    parse_ixbrl,
    parse_period_from_text,
    rupees_to_crore,
)
from app.nse import _classify, normalize
from datetime import date


FIXTURE = Path(__file__).parent / "fixtures" / "sample_ifindas.html"


def test_ixbrl_extracts_current_and_yoy_quarter():
    parsed = parse_ixbrl(FIXTURE.read_text(encoding="utf-8"))
    assert parsed["ok"] is True
    assert parsed["result_period"] == "Q2 FY27"
    assert parsed["statement_type"] == "CONSOLIDATED"
    assert rupees_to_crore(parsed["current"]["revenue"]) == 120.0
    assert rupees_to_crore(parsed["yoy"]["revenue"]) == 100.0
    assert rupees_to_crore(parsed["current"]["pat"]) == 12.0
    assert rupees_to_crore(parsed["yoy"]["pat"]) == 8.0
    assert parsed["current"]["eps"] == 5.5
    impact, reason = classify_impact(parsed)
    assert impact == "bullish"
    assert "YoY revenue" in reason


def test_ixbrl_without_comparatives_is_not_assessed():
    html = """
    <xbrli:context id="OneD"><xbrli:period>
    <xbrli:startDate>2026-07-01</xbrli:startDate><xbrli:endDate>2026-09-30</xbrli:endDate>
    </xbrli:period></xbrli:context>
    <ix:nonFraction name="in-bse-fin:RevenueFromOperations" contextRef="OneD" scale="7">50.00</ix:nonFraction>
    <ix:nonFraction name="in-bse-fin:ProfitLossForPeriod" contextRef="OneD" scale="7">5.00</ix:nonFraction>
    """
    parsed = parse_ixbrl(html)
    assert parsed["ok"] is True
    impact, reason = classify_impact(parsed)
    assert impact == "unassessed"
    assert "previous-year" in reason.lower()


def test_exceptional_income_makes_impact_mixed():
    parsed = {
        "ok": True,
        "current": {"revenue": 1.2e9, "pat": 1.2e8, "exceptional": 8e7, "ebitda": 2e8},
        "yoy": {"revenue": 1.0e9, "pat": 8e7, "ebitda": 1.5e8},
    }
    impact, reason = classify_impact(parsed)
    assert impact == "mixed"
    assert "exceptional" in reason.lower()


def test_missing_values_do_not_become_zero():
    parsed = {"ok": True, "current": {"revenue": None, "pat": None}, "yoy": {}}
    impact, _ = classify_impact(parsed)
    assert impact == "unassessed"
    analysis = analysis_from_parsed(parsed, "https://www.bseindia.com/xbrl.html")
    assert analysis["revenue_current_crore"] is None
    assert analysis["pat_yoy_pct"] is None


def test_bse_period_dates_map_to_q2_fy27():
    info = parse_period_from_text(
        "Cochin Shipyard Ltd (540678)",
        "Unaudited | Standalone|PERIOD START DATE : 01-07-2026 | PERIOD END DATE : 30-09-2026",
    )
    assert info["result_period"] == "Q2 FY27"
    assert info["statement_type"] == "STANDALONE"
    assert indian_fy_quarter(date(2026, 7, 1), date(2026, 9, 30)) == "Q2 FY27"


def test_invalid_ixbrl_is_failed_extraction():
    parsed = parse_ixbrl("<html>no facts</html>")
    assert parsed["ok"] is False


def test_earnings_transcript_is_not_a_published_result():
    category, state, _, _ = _classify(
        "Bajaj Finserv Ltd (532978)",
        "Transcript of earnings call held in respect of Financial Results for the Quarter.",
        "announcements",
    )
    assert state != "RESULT PUBLISHED"
    assert state in {"ROUTINE", "NEW EVENT"}


def test_audio_recording_of_results_call_is_not_published_result():
    _, state, _, _ = _classify(
        "Data Patterns (India) Ltd",
        "Audio Recording of the Earnings Conference Call held on Monday in connection with Unaudited Financial Results",
        "announcements",
    )
    assert state != "RESULT PUBLISHED"


def test_parent_company_results_reference_is_not_listed_company_result():
    _, state, _, _ = _classify(
        "Maruti Suzuki India Limited",
        "Reference to the Financial Results issued by Suzuki Motor Corporation",
        "announcements",
    )
    assert state != "RESULT PUBLISHED"


def test_false_positive_order_word_is_not_sudden():
    _, state, _, _ = _classify(
        "Board meeting intimation",
        "The company will consider ordinary business in order to complete statutory compliance.",
        "announcements",
    )
    assert state != "SUDDEN EVENT"


def test_material_order_win_is_sudden():
    _, state, direction, _ = _classify(
        "Company received a letter of award",
        "The company received an order worth Rs 500 crore.",
        "announcements",
    )
    assert state == "SUDDEN EVENT"
    assert direction == "bullish"


def test_trading_window_is_routine():
    _, state, _, _ = _classify(
        "Trading Window Closure",
        "Trading window shall remain closed for designated persons.",
        "announcements",
    )
    assert state == "ROUTINE"


def test_bse_xbrl_and_nse_result_share_period_dedupe_key():
    allowed = {"COCHINSHIP"}
    names = {"COCHINSHIP": "Cochin Shipyard Limited"}
    bse = normalize(
        {
            "title": "Cochin Shipyard Ltd (540678)",
            "description": "Unaudited | Standalone|PERIOD START DATE : 01-07-2026 | PERIOD END DATE : 30-09-2026",
            "published": "",
            "link": "https://www.bseindia.com/XBRLFILES/example.html",
            "feed_type": "financial_results",
        },
        allowed,
        names,
    )
    nse = normalize(
        {
            "title": "Cochin Shipyard Limited",
            "description": "Cochin Shipyard Limited has submitted to the Exchange, the financial results for the period ended September 30, 2026. |SUBJECT: Outcome of Board Meeting Q2 FY27",
            "published": "Tue, 10 Nov 2026 13:00:00 +0530",
            "link": "https://nsearchives.nseindia.com/corporate/example.pdf",
            "feed_type": "announcements",
        },
        allowed,
        names,
    )
    assert bse["event_status"] == "RESULT PUBLISHED"
    assert nse["event_status"] == "RESULT PUBLISHED"
    assert bse["result_period"] == "Q2 FY27"
    assert bse["dedupe_key"] == nse["dedupe_key"]


def test_official_url_allowlist():
    from app.documents import is_official_document_url
    assert is_official_document_url("https://www.bseindia.com/XBRLFILES/a.html")
    assert is_official_document_url("https://nsearchives.nseindia.com/corporate/a.pdf")
    assert not is_official_document_url("https://evil.example/steal")
    assert not is_official_document_url("http://127.0.0.1/secret")
    assert not is_official_document_url("file:///etc/passwd")


def test_homepage_and_health_payload_do_not_require_live_feeds(monkeypatch):
    from app.main import app, dashboard, health
    import app.main as main_module

    monkeypatch.setattr(main_module, "get_quotes", lambda symbols: {})
    homepage = next(route for route in app.routes if getattr(route, "path", None) == "/")
    assert homepage.methods == {"GET", "HEAD"}
    html = dashboard()
    assert "Today" in html
    assert "Sudden material news" in html
    payload = health()
    assert payload["ok"] is True
    assert payload["database"]["persistent"] is False
    assert "status" in payload


def test_informed_bse_does_not_map_to_bse_stock():
    item = normalize(
        {
            "title": "Ekennis Software Service Ltd (543475)",
            "description": "Ekennis Software Service Ltd has informed BSE that the meeting of the Board is scheduled.",
            "published": "Tue, 10 Nov 2026 13:00:00 +0530",
            "link": "https://www.bseindia.com/example.pdf",
            "feed_type": "announcements",
        },
        {"BSE", "RELIANCE"},
        {"BSE": "BSE Limited", "RELIANCE": "Reliance Industries Limited"},
    )
    assert item is None


def test_ongc_is_not_matched_as_oil_india():
    item = normalize(
        {
            "title": "Oil and Natural Gas Corporation Ltd (500312)",
            "description": "Schedule of Analyst/ Investor Meet",
            "published": "Tue, 10 Nov 2026 13:00:00 +0530",
            "link": "https://www.bseindia.com/example.pdf",
            "feed_type": "announcements",
        },
        {"OIL", "ONGC"},
        {"OIL": "Oil India Limited", "ONGC": "Oil and Natural Gas Corporation Limited"},
    )
    assert item is not None
    assert item["symbol"] == "ONGC"
    assert item["event_status"] == "ROUTINE"


def test_quote_formatter_shows_percent_move():
    from app.prices import format_quote
    html = format_quote({"ltp": 1234.5, "change_pct": 1.25})["html"]
    assert "1,234.50" in html or "1,234.5" in html
    assert "+1.25%" in html
    assert "up" in html
