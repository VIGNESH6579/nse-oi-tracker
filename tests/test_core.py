from datetime import datetime, timedelta, timezone

from app.nse import _classify, _parse_event_date, normalize
from app.store import Store


def test_nse_board_meeting_api_items_keep_symbol_and_broadcast_time():
    from app.nse import _board_meeting_api_items, _date

    rows = [{
        "bm_symbol": "KALYANKJIL",
        "bm_date": "10-Nov-2026",
        "bm_purpose": "KALYAN JEWELLERS IND LTD has informed the Exchange about Board Meeting to be held on 10-Nov-2026 to consider and approve the Financial Results.",
        "bm_desc": "Board Meeting Intimation",
        "bm_timestamp": "22-Oct-2026 11:43:03",
        "sm_name": "Kalyan Jewellers India Limited",
        "attachment": "https://nsearchives.nseindia.com/corporate/test.xml",
    }]
    items = _board_meeting_api_items(rows, {"KALYANKJIL"})
    assert len(items) == 1
    assert items[0]["feed_type"] == "board_meetings"
    assert items[0]["title"].startswith("KALYANKJIL")
    assert _date(items[0]["published"]) == "2026-10-22T06:13:03+00:00"


def test_nse_board_meeting_api_rejects_non_fno_symbols():
    from app.nse import _board_meeting_api_items

    rows = [{"bm_symbol": "RELIANCE", "bm_date": "10-Nov-2026",
             "bm_purpose": "Financial Results", "bm_desc": "Board Meeting Intimation"}]
    assert _board_meeting_api_items(rows, {"KALYANKJIL"}) == []


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


def test_fno_universe_uses_symbol_column_not_underlying_display_name(monkeypatch):
    from types import SimpleNamespace
    import app.nse as nse_module

    class Response:
        text = (
            "UNDERLYING,SYMBOL,NOV-26,DEC-26\n"
            "360 ONE WAM LIMITED,360ONE,500,500\n"
            "COCHIN SHIPYARD LIMITED,COCHINSHIP,125,125\n"
            "KALYAN JEWELLERS INDIA LIMITED,KALYANKJIL,125,125\n"
            "NIFTY 50,NIFTY,75,75\n"
            "NIFTY BANK,BANKNIFTY,35,35\n"
            "UNDERLYING,SYMBOL,NOV-26,DEC-26\n"
        )

        def raise_for_status(self):
            return None

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    monkeypatch.setattr(
        nse_module,
        "settings",
        SimpleNamespace(
            fno_symbols=frozenset({"RVNL", "RAIL VIKAS NIGAM LIMITED", "NIFTY BANK"}),
            universe_csv_url="https://example.com/fno.csv",
            nse_timeout=5,
        ),
    )
    symbols, source = nse_module.get_fno_symbols(Session())
    assert symbols == {"360ONE", "COCHINSHIP", "KALYANKJIL", "RVNL"}
    assert "SYMBOL column" in source


def test_fno_environment_symbols_are_additive_to_the_live_universe(monkeypatch):
    from types import SimpleNamespace
    import app.nse as nse_module

    class Response:
        text = "SYMBOL,CONTRACT\nRVNL,XX\nCOCHINSHIP,YY\n"

        def raise_for_status(self):
            return None

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    monkeypatch.setattr(
        nse_module,
        "settings",
        SimpleNamespace(
            fno_symbols=frozenset({"CUSTOMTEST"}),
            universe_csv_url="https://example.com/fno.csv",
            nse_timeout=5,
        ),
    )
    symbols, source = nse_module.get_fno_symbols(Session())
    assert {"RVNL", "COCHINSHIP", "CUSTOMTEST"}.issubset(symbols)
    assert "NSE F&O market-lot CSV" in source


def test_company_name_matching_handles_ind_ltd_abbreviation_for_fno_stocks():
    item = normalize(
        {
            "title": "KALYAN JEWELLERS IND LTD has informed the Exchange about Board Meeting",
            "description": "Board meeting to be held on 10-Nov-2026 to consider and approve the unaudited financial results.",
            "published": "Mon, 09 Nov 2026 10:00:00 +0530",
            "link": "https://example.com/kalyan",
            "feed_type": "board_meetings",
        },
        {"KALYANKJIL"},
        {"KALYANKJIL": "Kalyan Jewellers India Limited"},
    )
    assert item is not None
    assert item["symbol"] == "KALYANKJIL"
    assert item["event_date"] == "2026-11-10"
    assert item["event_status"] in {"RESULT SCHEDULED", "RESULT SCHEDULED TODAY"}


def test_normalize_matches_cochin_shipyard_name_without_symbol_in_rss():
    item = normalize(
        {
            "title": "COCHIN SHIPYARD LIMITED has informed the Exchange about Board Meeting",
            "description": "Board meeting to be held on 10-Nov-2026 to consider and approve the unaudited financial results.",
            "published": "Wed, 04 Nov 2026 14:11:29 +0530",
            "link": "https://example.com/cochin",
            "feed_type": "board_meetings",
        },
        {"COCHINSHIP"},
        {"COCHINSHIP": "Cochin Shipyard Limited"},
    )
    assert item is not None
    assert item["symbol"] == "COCHINSHIP"
    assert item["event_date"] == "2026-11-10"
    assert item["event_status"] in {"RESULT SCHEDULED", "RESULT SCHEDULED TODAY"}


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
    # Same filing from a different NSE feed/category must still be deduplicated.
    duplicate_category = dict(event)
    duplicate_category["event_id"] = "third-id"
    duplicate_category["category"] = "Corporate Announcement"
    duplicate_category["filed_at"] = (now + timedelta(minutes=4)).isoformat()
    assert store.insert(duplicate_category) is False
    assert len(store.list_events(20)) == 1


def test_date_only_filing_does_not_gain_an_invented_time():
    from app.nse import _date
    assert _date("09-Nov-2026") == "2026-11-09"



def test_event_calendar_fno_result_is_scheduled_without_inventing_filing_time():
    from app.nse import _event_calendar_items

    rows = [{
        "symbol": "KALYANKJIL",
        "company": "Kalyan Jewellers India Limited",
        "date": "10-Nov-2026",
        "purpose": "Financial Results",
    }]
    items = _event_calendar_items(rows, {"KALYANKJIL"})
    assert len(items) == 1
    event = normalize(items[0], {"KALYANKJIL"}, {"KALYANKJIL": "Kalyan Jewellers India Limited"})
    assert event["symbol"] == "KALYANKJIL"
    assert event["event_date"] == "2026-11-10"
    assert event["event_status"] in {"RESULT SCHEDULED", "RESULT SCHEDULED TODAY"}
    assert event["filed_at"] is None


def test_event_calendar_excludes_non_fno_company():
    from app.nse import _event_calendar_items
    rows = [{"symbol": "RELIANCE", "date": "10-Nov-2026", "purpose": "Financial Results"}]
    assert _event_calendar_items(rows, {"KALYANKJIL"}) == []



def test_bse_fallback_feeds_are_configured_for_independent_exchange_coverage():
    from app.nse import FEEDS
    assert "bse_announcements" in FEEDS
    assert "bse_board_meetings" in FEEDS
    assert "bse_financial_results" in FEEDS
    assert "bseindia.com" in FEEDS["bse_board_meetings"]


def test_health_endpoint_accepts_get_and_head_for_uptime_monitors():
    from app.main import app

    route = next(route for route in app.routes if getattr(route, "path", None) == "/api/health")
    assert {"GET", "HEAD"}.issubset(route.methods)


def test_universe_fallback_prefers_configured_then_last_known_good():
    from app.main import _fallback_universe

    symbols, source = _fallback_universe({"RVNL", "COCHINSHIP"}, {"RELIANCE"})
    assert symbols == {"RVNL", "COCHINSHIP", "RELIANCE"}
    assert source == "FNO_SYMBOLS + last-known-good F&O universe fallback"

    symbols, source = _fallback_universe(set(), {"RELIANCE", "TCS"})
    assert symbols == {"RELIANCE", "TCS"}
    assert source == "last-known-good F&O universe fallback"

    symbols, source = _fallback_universe({"SYMBOL", "RVNL"}, {"SYMBOL", "RELIANCE"})
    assert symbols == {"RVNL", "RELIANCE"}

    symbols, source = _fallback_universe(set(), set())
    assert symbols == set()
    assert source == "unavailable"


def test_store_persists_last_known_good_fno_universe_and_drops_legacy_quotes(tmp_path):
    store = Store(str(tmp_path / "events.db"))
    assert store.save_fno_universe({"RVNL", "COCHINSHIP", "SYMBOL"}) == 2
    assert store.load_fno_universe() == ["COCHINSHIP", "RVNL"]
    with store.connect() as db:
        tables = {row["name"] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "fno_universe" in tables
    assert "market_quotes" not in tables


def test_nse_session_uses_browser_compatible_headers(monkeypatch):
    from app import nse

    class FakeSession:
        def __init__(self):
            self.headers = {}

    fake = FakeSession()
    monkeypatch.setattr(nse.requests, "Session", lambda: fake)
    session = nse._session()
    assert session is fake
    assert "Mozilla/5.0" in session.headers["User-Agent"]
    assert "text/csv" in session.headers["Accept"]



def test_rescheduled_result_notice_is_classified_before_result_schedule():
    category, state, _, _ = _classify(
        "Board Meeting Rescheduled to 12-Nov-2026",
        "The meeting to consider and approve financial results has been postponed.",
        "board_meetings",
        "2026-11-12",
    )
    assert category == "Financial Results"
    assert state == "SCHEDULE CHANGE"


def test_same_headline_with_revised_meeting_date_is_not_deduplicated(tmp_path):
    store = Store(str(tmp_path / "events.db"))
    now = datetime.now(timezone.utc)
    base = {
        "symbol": "XYZ",
        "company": "XYZ Limited",
        "category": "Financial Results",
        "impact": "neutral",
        "headline": "XYZ Board Meeting Intimation",
        "details": "To consider financial results",
        "filed_at": now.isoformat(),
        "source_url": "https://example.com/filing",
        "event_status": "RESULT SCHEDULED",
    }
    first = dict(base, event_id="schedule-one", event_date="2026-11-12")
    revised = dict(base, event_id="schedule-two", event_date="2026-11-14",
                   event_status="SCHEDULE CHANGE",
                   filed_at=(now + timedelta(minutes=5)).isoformat())
    assert store.insert(first) is True
    assert store.insert(revised) is True



def test_notification_queue_ignores_stale_unnotified_backlog(tmp_path):
    store = Store(str(tmp_path / "events.db"))
    event = {
        "event_id": "stale-alert",
        "symbol": "XYZ",
        "company": "XYZ Limited",
        "category": "Corporate Announcement",
        "impact": "unclassified",
        "headline": "Old filing",
        "details": "",
        "filed_at": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
        "source_url": "https://example.com/old",
        "event_status": "NEW EVENT",
    }
    assert store.insert(event) is True
    old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    with store.connect() as db:
        db.execute("UPDATE events SET created_at=? WHERE event_id=?", (old, "stale-alert"))
        db.commit()
    assert store.pending_notifications(limit=10) == []



def test_board_meetings_api_does_not_require_blocked_nse_homepage(monkeypatch):
    from app import nse

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return []

    class Session:
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def get(self, url, **kwargs):
            assert url == nse.NSE_BOARD_MEETINGS_API
            return Response()

    monkeypatch.setattr(nse, "_session", lambda: Session())
    assert nse.fetch_board_meetings_api({"RELIANCE"}) == []


def test_nse_rss_uses_browser_user_agent_instead_of_bot_identifier(monkeypatch):
    from app import nse

    class FakeSession:
        def __init__(self):
            self.headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36"
            }

        def close(self):
            return None

    session = FakeSession()
    observed = []

    def fake_fetch_feed(s, url):
        if "nsearchives.nseindia.com" in url:
            observed.append((s.headers.get("User-Agent"), s.headers.get("Referer")))
            return [{"title": "test filing", "description": "", "link": url, "published": ""}]
        return []

    monkeypatch.setattr(nse, "_session", lambda: session)
    monkeypatch.setattr(nse, "fetch_feed", fake_fetch_feed)
    monkeypatch.setattr(nse, "fetch_board_meetings_api", lambda allowed: [])
    monkeypatch.setattr(nse, "fetch_event_calendar_api", lambda allowed: [])
    nse.fetch_all_feeds({"RELIANCE"}, {})
    assert observed
    assert all("Mozilla/5.0" in ua and "NSE-Company-News-Monitor" not in ua for ua, _ in observed)
    assert all(referer == "https://www.nseindia.com/" for _, referer in observed)
