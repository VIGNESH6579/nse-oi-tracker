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
    assert direction == "unassessed"
    assert "not assessed" in reason.lower()


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
    assert direction == "unassessed"


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



def test_result_outcome_from_board_meeting_is_published_not_scheduled():
    category, state, direction, rationale = _classify(
        "COCHINSHIP Outcome of Board Meeting for Q2 Results/Interim Dividend/Record Date",
        "The board approved the Q2 results for the quarter ended September 30, 2026.",
        "announcements",
    )
    assert category == "Financial Results"
    assert state == "RESULT PUBLISHED"
    assert direction == "unassessed"
    assert "not assessed" in rationale.lower()


def test_sudden_company_order_event_is_directionally_classified():
    category, state, direction, rationale = _classify(
        "Company secures major work order",
        "The company received a letter of award for a significant contract.",
        "announcements",
    )
    assert category == "Order / Contract"
    assert state == "SUDDEN EVENT"
    assert direction == "bullish"


def test_result_board_meeting_notice_is_not_misclassified_as_published():
    category, state, direction, _ = _classify(
        "Board Meeting Intimation for Unaudited Financial Results",
        "The board will consider and approve the financial results at its meeting.",
        "board_meetings",
        "2099-12-15",
    )
    assert category == "Financial Results"
    assert state == "RESULT SCHEDULED"
    assert direction == "unassessed"


def test_nse_financial_results_rss_is_tagged_as_financial_results(monkeypatch):
    from app import nse

    class FakeSession:
        headers = {}
        def close(self):
            return None

    monkeypatch.setattr(nse, "FEEDS", {"financial_results": "https://example.com/results.xml"})
    monkeypatch.setattr(nse, "_session", lambda: FakeSession())
    monkeypatch.setattr(nse, "fetch_feed", lambda session, url: [
        {"title": "XYZ financial results", "description": "", "link": url, "published": ""}
    ])
    monkeypatch.setattr(nse, "fetch_board_meetings_api", lambda allowed: [])
    monkeypatch.setattr(nse, "fetch_event_calendar_api", lambda allowed: [])
    rows, health = nse.fetch_all_feeds({"XYZ"}, {})
    assert rows[0]["feed_type"] == "financial_results"
    assert rows[0]["source_name"] == "NSE RSS"


def test_notification_queue_only_contains_results_and_sudden_events(tmp_path):
    store = Store(str(tmp_path / "events.db"))
    now = datetime.now(timezone.utc).isoformat()
    base = {
        "symbol": "XYZ", "company": "XYZ Limited", "category": "Financial Results",
        "impact": "unassessed", "headline": "Filing", "details": "",
        "filed_at": now, "source_url": "https://example.com", "rationale": "",
        "source_name": "BSE RSS", "feed_type": "announcements",
    }
    for event_id, state in [
        ("scheduled", "RESULT SCHEDULED"),
        ("routine", "NEW EVENT"),
        ("result", "RESULT PUBLISHED"),
        ("sudden", "SUDDEN EVENT"),
    ]:
        store.insert(dict(base, event_id=event_id, event_status=state, headline=event_id))
    pending = store.pending_notifications(limit=10)
    assert {x["event_id"] for x in pending} == {"result", "sudden"}



def test_same_quarter_result_from_different_feeds_has_same_dedupe_key():
    allowed = {"XYZ"}
    first = normalize(
        {"title": "XYZ Financial Results for Q2 FY27", "description": "Financial results",
         "published": "Tue, 10 Nov 2026 13:00:00 +0530", "link": "https://example.com/1",
         "feed_type": "financial_results"},
        allowed,
    )
    second = normalize(
        {"title": "XYZ Outcome of Board Meeting Q2 FY27 Results", "description": "Board approved results",
         "published": "Tue, 10 Nov 2026 13:01:00 +0530", "link": "https://example.com/2",
         "feed_type": "announcements"},
        allowed,
    )
    assert first["event_status"] == "RESULT PUBLISHED"
    assert second["event_status"] == "RESULT PUBLISHED"
    assert first["dedupe_key"] == second["dedupe_key"]


def test_ntfy_digest_batches_priority_events_in_one_request(monkeypatch):
    from types import SimpleNamespace
    from app import notifier

    monkeypatch.setattr(notifier, "settings", SimpleNamespace(
        ntfy_topic="Nse_News_Alert", ntfy_token="", ntfy_base_url="https://ntfy.sh"
    ))
    calls = []

    class Response:
        status_code = 200
        headers = {}
        def raise_for_status(self):
            return None

    def fake_post(url, data, headers, timeout):
        calls.append((url, data.decode("utf-8"), headers))
        return Response()

    monkeypatch.setattr(notifier.requests, "post", fake_post)
    events = [
        {"event_id": "r1", "symbol": "COCHINSHIP", "company": "Cochin Shipyard",
         "event_status": "RESULT PUBLISHED", "impact": "unassessed",
         "headline": "Q2 results filed", "details": "Results approved",
         "rationale": "Figures not supplied", "filed_at": None, "source_name": "BSE RSS",
         "source_url": "https://example.com/result"},
        {"event_id": "e1", "symbol": "RVNL", "company": "Rail Vikas Nigam",
         "event_status": "SUDDEN EVENT", "impact": "bullish",
         "headline": "Major order won", "details": "Order awarded",
         "rationale": "Positive contract event", "filed_at": "2026-11-10T09:00:00+00:00",
         "source_name": "NSE RSS", "source_url": "https://example.com/order"},
    ]
    assert notifier.send_many(events) is True
    assert len(calls) == 1
    assert "COCHINSHIP" in calls[0][1]
    assert "RVNL" in calls[0][1]
    assert calls[0][2]["Title"] == "NSE NEWS | 2 ALERT(S)"


def test_ntfy_date_only_filing_does_not_invent_midnight_time():
    from app.notifier import _filed_time
    assert _filed_time("2026-11-10") == "10 Nov 2026 · time not supplied by exchange"


def test_static_fno_fallback_is_used_when_live_csv_and_cache_are_unavailable(monkeypatch):
    from types import SimpleNamespace
    import requests
    import app.nse as nse_module

    class Session:
        def get(self, *args, **kwargs):
            response = requests.Response()
            response.status_code = 403
            raise requests.HTTPError("403 forbidden", response=response)

    monkeypatch.setattr(nse_module, "settings", SimpleNamespace(
        fno_symbols=frozenset(),
        fallback_fno_symbols=frozenset({"COCHINSHIP", "RVNL", "NIFTY BANK"}),
        universe_csv_url="https://example.com/fno.csv",
        nse_timeout=5,
    ))
    symbols, source = nse_module.get_fno_symbols(Session())
    assert symbols == {"COCHINSHIP", "RVNL"}
    assert "FNO_FALLBACK_SYMBOLS" in source


def test_source_backoff_reuses_recent_cache_without_repeating_blocked_request(monkeypatch):
    import requests
    import app.nse as nse_module

    nse_module._SOURCE_CACHE.clear()
    nse_module._SOURCE_RETRY_AFTER.clear()
    nse_module._SOURCE_FAILURES.clear()
    monkeypatch.setattr(nse_module.time, "monotonic", lambda: 100.0)
    nse_module._SOURCE_CACHE["blocked"] = {
        "at": 90.0, "items": [{"title": "Cached filing", "feed_type": "announcements"}]
    }
    nse_module._SOURCE_RETRY_AFTER["blocked"] = 200.0
    calls = []
    items, health = nse_module._fetch_source(
        "blocked", lambda: calls.append("called") or []
    )
    assert calls == []
    assert len(items) == 1
    assert items[0]["title"] == "Cached filing"
    assert health["stale"] is True
    assert health["ok"] is False


def test_source_403_uses_thirty_minute_retry_backoff(monkeypatch):
    import requests
    import app.nse as nse_module

    nse_module._SOURCE_CACHE.clear()
    nse_module._SOURCE_RETRY_AFTER.clear()
    nse_module._SOURCE_FAILURES.clear()
    monkeypatch.setattr(nse_module.time, "monotonic", lambda: 500.0)

    def fail():
        response = requests.Response()
        response.status_code = 403
        raise requests.HTTPError("403 forbidden", response=response)

    items, health = nse_module._fetch_source("blocked-new", fail)
    assert items == []
    assert health["ok"] is False
    assert health["retry_in_seconds"] == 1800
    assert nse_module._SOURCE_RETRY_AFTER["blocked-new"] == 2300.0


def test_store_enables_wal_for_concurrent_dashboard_reads(tmp_path):
    store = Store(str(tmp_path / "wal.db"))
    with store.connect() as db:
        mode = db.execute("PRAGMA journal_mode").fetchone()[0]
        timeout = db.execute("PRAGMA busy_timeout").fetchone()[0]
    assert mode.lower() == "wal"
    assert timeout == 15000


def test_successful_empty_feeds_are_not_reported_as_total_outage(monkeypatch):
    import app.nse as nse_module

    monkeypatch.setattr(nse_module, "FEEDS", {"announcements": "https://example.com/rss"})
    monkeypatch.setattr(nse_module, "fetch_feed", lambda session, url: [])
    monkeypatch.setattr(nse_module, "fetch_board_meetings_api", lambda allowed: [])
    monkeypatch.setattr(nse_module, "fetch_event_calendar_api", lambda allowed: [])
    nse_module._SOURCE_CACHE.pop("announcements", None)
    nse_module._SOURCE_RETRY_AFTER.pop("announcements", None)
    nse_module._SOURCE_FAILURES.pop("announcements", None)
    rows, health = nse_module.fetch_all_feeds(set(), {})
    assert rows == []
    assert health["announcements"]["ok"] is True


def test_company_master_backoff_does_not_retry_when_cache_is_empty(monkeypatch):
    import app.nse as nse_module

    calls = []
    monkeypatch.setattr(nse_module.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(nse_module, "_session", lambda: calls.append("session"))
    nse_module._COMPANY_MAP_CACHE.clear()
    nse_module._COMPANY_MAP_RETRY_AFTER = 200.0
    try:
        assert nse_module.get_fno_company_map({"COCHINSHIP"}) == {}
        assert calls == []
    finally:
        nse_module._COMPANY_MAP_RETRY_AFTER = 0.0


def test_sudden_events_are_in_dashboard_news_section():
    from app.main import _dashboard_news_events

    events = [
        {"event_status": "NEW EVENT", "symbol": "XYZ"},
        {"event_status": "SUDDEN EVENT", "symbol": "ABC"},
        {"event_status": "RESULT PUBLISHED", "symbol": "DEF"},
        {"event_status": "RESULT SCHEDULED", "symbol": "GHI"},
    ]
    assert {event["symbol"] for event in _dashboard_news_events(events)} == {"XYZ", "ABC"}
