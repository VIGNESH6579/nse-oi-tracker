from datetime import datetime, timedelta, timezone

from app.nse import _classify, _parse_event_date, normalize
from app.store import Store


def test_financial_results_feed_marks_published_but_does_not_guess_direction():
    category, state, direction, reason = _classify(
        "Financial results for quarter ended", "", "financial_results"
    )
    assert category == "Financial Results"
    assert state == "RESULT PUBLISHED"
    assert direction == "neutral"
    assert "not yet been compared" in reason


def test_board_result_notice_extracts_meeting_date_and_schedules_future_results():
    filing = (
        "The Board Meeting will be held on 15th December, 2099 to consider and "
        "approve the unaudited financial results for the quarter ended."
    )
    event_date = _parse_event_date(filing)
    assert event_date == "2099-12-15"
    category, state, direction, _ = _classify(
        filing, "", "board_meetings", event_date
    )
    assert category == "Financial Results"
    assert state == "RESULT SCHEDULED"
    assert direction == "neutral"


def test_board_meeting_without_result_agenda_is_not_a_result():
    category, state, _, _ = _classify(
        "Board meeting to be held", "", "board_meetings"
    )
    assert category == "Board Meeting"
    assert state == "BOARD MEETING NOTICE"


def test_normalize_filters_unknown_symbol():
    assert normalize({"title": "Some Company", "description": "Corporate filing"}, {"XYZ"}) is None


def test_normalize_keeps_company_name_and_schedule_date():
    item = normalize(
        {
            "title": "XYZ board meeting",
            "description": "The Board Meeting will be held on 15th December, 2099 to consider financial results.",
            "published": "Mon, 09 Nov 2026 10:00:00 +0530",
            "link": "https://example.com/a",
            "feed_type": "board_meetings",
        },
        {"XYZ"},
        {"XYZ": "XYZ Limited"},
    )
    assert item["symbol"] == "XYZ"
    assert item["company"] == "XYZ Limited"
    assert item["event_date"] == "2099-12-15"
    assert item["event_status"] == "RESULT SCHEDULED"
    assert len(item["event_id"]) == 64


def test_duplicate_rss_items_are_suppressed_within_72_hours(tmp_path):
    store = Store(str(tmp_path / "events.db"))
    now = datetime.now(timezone.utc)
    event = {
        "event_id": "first-id",
        "symbol": "XYZ",
        "company": "XYZ Limited",
        "category": "Order / Contract",
        "impact": "unclassified",
        "headline": "XYZ receives a major work order",
        "details": "Order announcement",
        "filed_at": now.isoformat(),
        "source_url": "https://example.com/filing/1",
        "event_status": "NEW EVENT",
    }
    assert store.insert(event) is True
    duplicate = dict(event)
    duplicate["event_id"] = "second-id"
    duplicate["source_url"] = "https://example.com/filing/2"
    duplicate["filed_at"] = (now + timedelta(minutes=2)).isoformat()
    assert store.insert(duplicate) is False
    assert len(store.list_events(20)) == 1
