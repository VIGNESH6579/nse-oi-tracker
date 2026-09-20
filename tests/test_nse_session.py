import json

import app.nse_fetcher as nse_fetcher


class FakeResponse:
    def __init__(self, status_code=200, payload=None, content_type="application/json"):
        self.status_code = status_code
        self.headers = {"content-type": content_type}
        self.content = json.dumps(payload).encode() if payload is not None else b""
        self.text = self.content.decode(errors="replace")

    def json(self):
        return json.loads(self.content)


def test_safe_json_rejects_html_even_when_session_is_stale():
    session = nse_fetcher.NSESession()
    response = FakeResponse(content_type="text/html", payload={"blocked": True})
    assert session._safe_json(response, "test-endpoint") is None


def test_get_rebuilds_after_block_response(monkeypatch):
    session = nse_fetcher.NSESession()
    session._sess = type("FakeSession", (), {"headers": {}})()
    monkeypatch.setattr(session, "_ensure", lambda: None)
    rebuilds = []
    monkeypatch.setattr(session, "_build", lambda: rebuilds.append(True) or session._sess)
    monkeypatch.setattr(nse_fetcher.time, "sleep", lambda *_: None)
    responses = iter([FakeResponse(403, {"error": "blocked"}), FakeResponse(200, {"ok": True})])
    monkeypatch.setattr(session, "_api_get", lambda *_: next(responses))

    assert session.get("https://nse.test/api", "https://nse.test/") == {"ok": True}
    assert len(rebuilds) == 1


def test_get_seeded_visits_page_before_api(monkeypatch):
    session = nse_fetcher.NSESession()
    session._sess = type("FakeSession", (), {"headers": {}})()
    monkeypatch.setattr(session, "_ensure", lambda: None)
    monkeypatch.setattr(nse_fetcher.time, "sleep", lambda *_: None)
    calls = []
    monkeypatch.setattr(session, "_nav_get", lambda url, referer: calls.append(("seed", url, referer)) or FakeResponse(200, {}))
    monkeypatch.setattr(session, "_api_get", lambda url, referer: calls.append(("api", url, referer)) or FakeResponse(200, {"data": []}))

    result = session.get_seeded("seed-url", "seed-ref", "api-url", "api-ref")

    assert result == {"data": []}
    assert [call[0] for call in calls] == ["seed", "api"]




def test_proxy_is_applied_to_archive_requests(monkeypatch):
    monkeypatch.setenv("NSE_OI_PROXY_URL", "socks5h://proxy.example:1080")
    seen = {}

    def get(url, **kwargs):
        seen.update(kwargs)
        return FakeResponse(200, "csv", content_type="text/csv")

    monkeypatch.setattr(nse_fetcher.cffi_requests, "get", get)
    assert nse_fetcher.NSESession().get_archive_text("https://nse.test/file.csv", "https://nse.test/") == '"csv"'
    assert seen["proxies"] == {
        "http": "socks5h://proxy.example:1080",
        "https": "socks5h://proxy.example:1080",
    }


def test_invalid_proxy_url_is_rejected(monkeypatch):
    monkeypatch.setenv("NSE_OI_PROXY_URL", "file:///tmp/proxy")
    try:
        nse_fetcher.NSESession()._new_session()
    except ValueError as exc:
        assert "NSE_OI_PROXY_URL" in str(exc)
    else:
        raise AssertionError("invalid proxy URL should be rejected")


def test_placeholder_proxy_url_is_ignored(monkeypatch):
    monkeypatch.setenv(
        "NSE_OI_PROXY_URL",
        "http://user:password@india-residential-proxy:port",
    )
    assert nse_fetcher._proxy_kwargs() == {}
