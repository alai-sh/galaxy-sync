import email.utils
import time

import pytest

from galaxy_sync import galaxy as galaxy_mod
from galaxy_sync.galaxy import USER_AGENT, GalaxyClient, GalaxyError, NotFound


def test_user_agent_has_the_package_version():
    assert USER_AGENT.startswith("alai-galaxy-sync/0.")


def test_url_rewrites_pagination_links_to_the_base_scheme():
    client = GalaxyClient("https://galaxy.example/")
    assert client._url("/api/") == "https://galaxy.example/api/"
    assert client._url("http://galaxy.example/next?page=2") == "https://galaxy.example/next?page=2"
    assert client._url("http://elsewhere.example/x") == "http://elsewhere.example/x"


def test_retries_unavailable_then_succeeds(galaxy, client):
    galaxy.routes["/api/"] = [(502, b"bad gateway", {}), (429, b"", {"Retry-After": "0"}), (200, {"ok": 1}, {})]
    assert client.get_json("/api/") == {"ok": 1}
    assert client.requests == 3


def test_gives_up_after_retries(galaxy, client, monkeypatch):
    sleeps = []
    monkeypatch.setattr(galaxy_mod.time, "sleep", sleeps.append)
    client.backoff = 2
    galaxy.routes["/api/"] = (503, b"", {})
    with pytest.raises(GalaxyError, match="gave up after 3 attempts") as e:
        client.get_json("/api/")
    assert e.value.status is None
    assert sleeps == [2, 4]  # no sleep after the last attempt


def test_retry_after_is_honoured_and_capped(galaxy, client, monkeypatch):
    sleeps = []
    monkeypatch.setattr(galaxy_mod.time, "sleep", sleeps.append)
    client.max_wait = 60
    soon = email.utils.formatdate(time.time() + 30, usegmt=True)
    galaxy.routes["/api/"] = [
        (429, b"", {"Retry-After": "1000"}),
        (429, b"", {"Retry-After": soon}),
        (200, {}, {}),
    ]
    _ = client.get_json("/api/")
    assert sleeps[0] == 60
    assert 0 < sleeps[1] <= 30


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, None), ("", None), ("5", 5), ("-1", 0), ("soon", None), ("Mon, 01 Jan 2001 00:00:00 GMT", 0)],
)
def test_retry_after_parsing(value, expected):
    assert galaxy_mod._retry_after(value) == expected


def test_other_errors_are_not_retried(galaxy, client):
    galaxy.routes["/api/"] = (403, b"no", {})
    with pytest.raises(GalaxyError) as e:
        client.get_json("/api/")
    assert e.value.status == 403
    assert not isinstance(e.value, NotFound)
    assert client.requests == 1

    with pytest.raises(NotFound):
        client.get_json("/missing/")


def test_connection_errors_are_retried(client, monkeypatch):
    monkeypatch.setattr(client, "base_url", "http://127.0.0.1:1")
    with pytest.raises(GalaxyError, match="gave up"):
        client.get_json("/api/")
    assert client.requests == 3


def test_versions_follows_pages(galaxy, client):
    galaxy.json(
        "/api/v3/collections/ns/coll/versions/?limit=100",
        {"data": [{"version": "1.0.0"}], "links": {"next": f"{galaxy.url}/page2"}},
    )
    galaxy.json("/page2", {"results": [{"version": "2.0.0"}], "links": {"next": None}})
    assert [v["version"] for v in client.versions("ns", "coll")] == ["1.0.0", "2.0.0"]
    assert client.versions("ns", "missing") == []


def test_version_detail_quotes_the_version(galaxy, client):
    galaxy.json("/api/v3/collections/ns/coll/versions/1.0.0%2Bbuild/", {"version": "1.0.0+build"})
    assert client.version_detail("ns", "coll", "1.0.0+build") == {"version": "1.0.0+build"}


def test_error_kind():
    assert GalaxyError("u", None, "timeout").kind == "galaxy_unavailable"
    assert GalaxyError("u", 429, "slow down").kind == "galaxy_unavailable"
    assert GalaxyError("u", 403, "forbidden").kind == "galaxy_http_403"
