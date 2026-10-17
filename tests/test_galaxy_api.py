import functools
import hashlib
import http.server
import json
import threading
import urllib.error
import urllib.parse
import urllib.request

import pytest

from galaxy_sync import galaxy_api
from galaxy_sync.reconcile import reconcile
from galaxy_sync.storage import Buckets
from galaxy_sync.store import Store


def page(store, path):
    return json.loads(store.galaxy.read(galaxy_api.page_key(path)))


def line(vers, **extra):
    return {
        "v": 1,
        "name": "ns.coll",
        "vers": vers,
        "deps": [],
        "cksum": "ab" * 32,
        "size": 1,
        "yanked": False,
        "origin": "galaxy",
        "published": "2026-01-01T00:00:00Z",
        **extra,
    }


def test_download_url():
    template = "https://dl.example/{sha256-prefix}/{sha256-checksum}/{namespace}-{name}-{version}.tar.gz"
    assert galaxy_api.download_url(template, line("1.0.0+build.1")) == (
        f"https://dl.example/ab/{'ab' * 32}/ns-coll-1.0.0%2Bbuild.1.tar.gz"
    )


def test_highest_version_prefers_stable():
    lines = [line("1.0.0"), line("2.0.0-rc.1"), line("1.10.0"), line("3.0.0", yanked=True)]
    assert galaxy_api.collection("ns", "coll", lines, "t")["highest_version"]["version"] == "1.10.0"
    only_pre = galaxy_api.collection("ns", "coll", [line("1.0.0-a"), line("1.0.0-b")], "t")
    assert only_pre["highest_version"]["version"] == "1.0.0-b"
    assert galaxy_api.collection("ns", "coll", [line("1.0.0", yanked=True)], "t")["highest_version"] is None


def test_reconcile_writes_the_galaxy_view(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.10.0", "1.2.0+build"])
    reconcile(client, store, "ns", "coll")
    store.write_galaxy_root()

    assert page(store, "/api/") == {"available_versions": {"v3": "v3/"}}
    assert page(store, "/") == {"available_versions": {"v3": "api/v3/"}}

    coll = page(store, "/api/v3/collections/ns/coll/")
    assert coll["updated_at"] == store.state["collections"]["ns.coll"]["updated"]
    assert coll["highest_version"] == {"href": "/api/v3/collections/ns/coll/versions/1.10.0/", "version": "1.10.0"}
    assert coll["created_at"] == min(line["published"] for line in store.read_index("ns", "coll"))
    assert coll["deprecated"] is False

    versions = page(store, "/api/v3/collections/ns/coll/versions/")
    assert [v["version"] for v in versions["data"]] == ["1.10.0", "1.2.0+build", "1.0.0"]
    assert versions["links"]["next"] is None
    assert versions["meta"] == {"count": 3}
    assert versions["data"][0]["requires_ansible"] == ">=2.16"

    # The client puts the version in the path unquoted.
    built = page(store, "/api/v3/collections/ns/coll/versions/1.2.0+build/")
    assert built["artifact"]["filename"] == "ns-coll-1.2.0+build.tar.gz"
    v = page(store, "/api/v3/collections/ns/coll/versions/1.10.0/")
    (index_line,) = (x for x in store.read_index("ns", "coll") if x["vers"] == "1.10.0")
    assert v["namespace"] == {"name": "ns"}
    assert v["collection"]["name"] == "coll"
    assert v["version"] == "1.10.0"
    assert v["href"] == "/api/v3/collections/ns/coll/versions/1.10.0/"
    assert v["artifact"]["sha256"] == index_line["cksum"]
    assert v["artifact"]["size"] == index_line["size"]
    assert v["download_url"] == f"https://dl.example/{index_line['cksum']}"
    assert v["metadata"] == {"dependencies": {"other.coll": ">=1.0.0"}}
    assert v["signatures"] == []


def test_galaxy_view_follows_yanks_and_deprecation(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    reconcile(client, store, "ns", "coll")
    galaxy.add_collection("ns", "coll", ["1.0.0"], deprecated=True)
    reconcile(client, store, "ns", "coll")

    # Galaxy has no yank: the version leaves the listing like a Galaxy delete.
    versions = page(store, "/api/v3/collections/ns/coll/versions/")
    assert [v["version"] for v in versions["data"]] == ["1.0.0"]
    assert page(store, "/api/v3/collections/ns/coll/")["deprecated"] is True


def test_rebuild_galaxy(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    reconcile(client, store, "ns", "coll")

    moved = Store(store.buckets, "https://moved.example/{sha256-checksum}.tar.gz")
    assert moved.rebuild_collection("ns", "coll").pages == 1  # the version page, which has the download URL
    assert moved.rebuild_collection("ns", "coll").pages == 0
    moved.write_galaxy_root()
    v = page(moved, "/api/v3/collections/ns/coll/versions/1.0.0/")
    assert v["download_url"].startswith("https://moved.example/")
    assert page(moved, "/api/")["available_versions"] == {"v3": "v3/"}


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


def _serve(directory):
    """`python -m http.server --directory DIR`, as the README uses for local testing."""
    handler = functools.partial(_QuietHandler, directory=str(directory))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


@pytest.fixture
def served(tmp_path):
    """Stock http.server on the Galaxy view and on the artifacts, like the README's local test."""
    root = tmp_path / "store"
    (root / "galaxy-root").mkdir(parents=True)
    (root / "dl-root").mkdir(parents=True)
    galaxy_server, galaxy_url = _serve(root / "galaxy-root")
    dl_server, dl_url = _serve(root / "dl-root")
    yield root, galaxy_url, dl_url
    for server in (galaxy_server, dl_server):
        server.shutdown()
        server.server_close()


def _get(url, method="GET"):
    with urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=5) as resp:  # noqa: S310
        return resp.read()


def test_ansible_galaxy_requests(galaxy, client, served):
    """Walk the requests `ansible-galaxy collection install` makes, reading only the fields it reads."""
    root, served_url, dl_url = served
    store = Store(Buckets.local(root), dl_url + "/sha256/{sha256-prefix}/{sha256-checksum}.tar.gz")
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0+build"])
    reconcile(client, store, "ns", "coll")
    store.write_galaxy_root()

    # The client tries the configured URL as the API root first, so both the
    # server root and /api/ work. Its fallback, /api with no slash, redirects.
    body = _get(served_url + "/")
    assert urllib.parse.urljoin(served_url + "/", json.loads(body)["available_versions"]["v3"]) == (
        served_url + "/api/v3/"
    )
    v3 = json.loads(_get(served_url + "/api"))["available_versions"]["v3"]
    api = urllib.parse.urljoin(served_url + "/api/", v3)

    coll = json.loads(_get(api + "collections/ns/coll/"))
    assert coll["updated_at"]
    # The query string is ignored, so every limit/offset gets the one page.
    listing = json.loads(_get(api + "collections/ns/coll/versions/?limit=100&offset=0"))
    assert [(v["version"], v["requires_ansible"]) for v in listing["data"]] == [
        ("1.1.0+build", ">=2.16"),
        ("1.0.0", ">=2.16"),
    ]
    assert listing["links"]["next"] is None

    # The client puts `+` in the path unencoded.
    assert json.loads(_get(api + "collections/ns/coll/versions/1.1.0+build/"))["version"] == "1.1.0+build"
    data = json.loads(_get(api + "collections/ns/coll/versions/1.0.0/"))
    assert (data["namespace"]["name"], data["collection"]["name"], data["version"]) == ("ns", "coll", "1.0.0")
    assert data["metadata"]["dependencies"] == {"other.coll": ">=1.0.0"}
    assert data["href"]
    assert data["signatures"] == []
    blob = _get(data["download_url"])
    assert hashlib.sha256(blob).hexdigest() == data["artifact"]["sha256"]

    with pytest.raises(urllib.error.HTTPError) as e:
        _get(api + "collections/ns/missing/versions/?limit=100")
    assert e.value.code == 404
