from app.nse import _classify, normalize

def test_classifies_results():
    assert _classify("Financial results for quarter ended", "", "financial_results")[1] == "RESULT PUBLISHED"

def test_result_direction_is_conservative():
    category, state, direction, reason = _classify("Financial results", "", "financial_results")
    assert direction == "neutral"
    assert "not yet parsed" in reason

def test_board_meeting_is_upcoming():
    assert _classify("Board meeting to be held", "", "board_meetings")[1] == "UPCOMING"

def test_normalize_filters_unknown_symbol():
    assert normalize({"title":"Some Company","description":"Corporate filing"}, {"XYZ"}) is None

def test_normalize_creates_fno_event():
    item = normalize({"title":"XYZ board meeting","description":"Meeting notice","published":"Mon, 09 Nov 2026 10:00:00 +0530","link":"https://example.com/a"}, {"XYZ"})
    assert item["symbol"] == "XYZ"
    assert item["category"] == "Board Meeting"
    assert item["event_status"] == "UPCOMING"
    assert len(item["event_id"]) == 64
