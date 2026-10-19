"""Purging changed index files from the CDN, against a local server answering like Cloudflare's API."""

import http.server
import json
import threading

import pytest

from galaxy_sync.cdn import BATCH, PurgeError, Purger
from galaxy_sync.storage import StorageError

PURGE_ENVIRON = {
    "CLOUDFLARE_PURGE_TOKEN": "t0ken",
    "CLOUDFLARE_ZONE_ID": "zone",
    "ALAI_INDEX_URL": "https://index.example/",
}


class MockCloudflare:
    """Answers `POST /zones/<zone>/purge_cache` with `response` (status, body), recording each request."""

    def __init__(self):
        self.requests = []
        self.response = (200, {"success": True, "errors": [], "result": {"id": "zone"}})
        mock = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                mock.requests.append((self.path, self.headers["Authorization"], body))
                status, response = mock.response
                data = response if isinstance(response, bytes) else json.dumps(response).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"


@pytest.fixture
def cloudflare():
    mock = MockCloudflare()
    thread = threading.Thread(target=mock.server.serve_forever, daemon=True)
    thread.start()
    yield mock
    mock.server.shutdown()


def test_from_env():
    assert Purger.from_env({}) is None
    purger = Purger.from_env(PURGE_ENVIRON)
    assert purger is not None
    assert purger.url("index/ns/coll.jsonl") == "https://index.example/index/ns/coll.jsonl"
    assert "t0ken" not in repr(purger)


def test_from_env_lists_missing_variables():
    environ = {**PURGE_ENVIRON, "CLOUDFLARE_PURGE_TOKEN": ""}
    del environ["ALAI_INDEX_URL"]
    with pytest.raises(StorageError, match=r"CLOUDFLARE_PURGE_TOKEN, ALAI_INDEX_URL$"):
        Purger.from_env(environ)


def test_purge_in_batches(cloudflare):
    purger = Purger("t0ken", "zone", "https://index.example", cloudflare.url)
    keys = [f"index/ns/c{i}.jsonl" for i in range(BATCH + 1)]
    purger.purge(keys)
    assert [(path, auth) for path, auth, _ in cloudflare.requests] == [("/zones/zone/purge_cache", "Bearer t0ken")] * 2
    assert [body["files"] for _, _, body in cloudflare.requests] == [
        [f"https://index.example/{key}" for key in keys[:BATCH]],
        [f"https://index.example/{keys[BATCH]}"],
    ]


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (
            (403, {"success": False, "errors": [{"code": 10000, "message": "Authentication error"}]}),
            "HTTP 403: Authentication error",
        ),
        ((200, {"success": False, "errors": ["odd"]}), "not purged: odd"),
        ((502, b"<html>bad gateway</html>"), "HTTP 502: <html>bad gateway</html>"),
    ],
)
def test_purge_failures(cloudflare, response, message):
    cloudflare.response = response
    purger = Purger("t0ken", "zone", "https://index.example", cloudflare.url)
    with pytest.raises(PurgeError, match=f"^{message}$"):
        purger.purge(["config.json"])


def test_purge_unreachable():
    purger = Purger("t0ken", "zone", "https://index.example", "http://127.0.0.1:1")
    with pytest.raises(PurgeError):
        purger.purge(["config.json"])
