import hashlib
import http.server
import json
import threading
import urllib.parse

import pytest
from helpers import COUNT, FEED, LISTING, REPO_METADATA, artifact, feed_row

from galaxy_sync.galaxy import GalaxyClient
from galaxy_sync.storage import Buckets
from galaxy_sync.store import Store


class MockGalaxy:
    """A local HTTP server answering like Galaxy.

    `routes` maps a path, including any query string, to a response
    `(status, body, headers)` or a list of them that are served in turn, the
    last one repeating. A body that isn't bytes is sent as JSON. Unknown paths
    are 404s. `requests` records every path asked for.
    """

    def __init__(self):
        self.routes = {}
        self.requests = []
        mock = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                mock.requests.append(self.path)
                route = mock.routes.get(self.path, (404, {"detail": "Not found."}, {}))
                if isinstance(route, list):
                    route = route.pop(0) if len(route) > 1 else route[0]
                status, body, headers = route
                data = body if isinstance(body, bytes) else json.dumps(body).encode()
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.blobs = {}
        self.listed = {}  # (ns, name) -> item in the collection listing
        self.uploaded = {}  # (ns, name, version) -> pulp_created, for the upload feed
        self.uploads = 0
        self.collections = {}  # (ns, name) -> add_collection arguments, for replace()

    def json(self, path, body, status=200):
        self.routes[path] = (status, body, {})

    def _update_listing(self):
        self.json(LISTING, {"data": [self.listed[k] for k in sorted(self.listed)], "links": {"next": None}})
        rows = [feed_row(*key, created) for key, created in sorted(self.uploaded.items(), key=lambda i: i[1])]
        rows.reverse()
        self.json(FEED, {"meta": {"count": len(rows)}, "data": rows, "links": {"next": None}})
        self.json(COUNT, {"meta": {"count": len(rows)}, "data": rows[:1], "links": {"next": None}})
        self.touch()

    def touch(self):
        """Create a repo version, as every upload, delete or deprecation does."""
        self.repo_versions = getattr(self, "repo_versions", 0) + 1
        n = self.repo_versions
        self.json(REPO_METADATA, {"published": f"2026-04-01T00:{n // 60:02}:{n % 60:02}.000000Z"})

    def upload_time(self):
        """A pulp_created later than every upload so far, further apart than the feed overlap."""
        self.uploads += 1
        return f"2026-03-{1 + self.uploads // 12:02}T{self.uploads % 12 * 2:02}:00:00.000000Z"

    def add_collection(self, ns, name, versions, deprecated=False, blobs=None, updated="2026-01-01T00:00:00Z"):
        """Serve the contract endpoints and listing entry for a collection and its versions.

        A version's created_at is its upload time, as in the feed. Blobs not
        given keep what was served before, or a generated artifact.
        """
        self.collections[(ns, name)] = {"versions": list(versions), "deprecated": deprecated, "updated": updated}
        base = f"/api/v3/collections/{ns}/{name}"
        coll = {"namespace": ns, "name": name, "updated_at": updated, "deprecated": deprecated}
        self.json(f"{base}/", coll)
        self.listed[(ns, name)] = coll
        for key in [k for k in self.uploaded if k[:2] == (ns, name) and k[2] not in versions]:
            del self.uploaded[key]
        for v in versions:
            if (ns, name, v) not in self.uploaded:
                self.uploaded[(ns, name, v)] = self.upload_time()
        self._update_listing()
        self.json(
            f"{base}/versions/?limit=100",
            {
                "data": [
                    {"version": v, "created_at": self.uploaded[(ns, name, v)], "requires_ansible": ">=2.16"}
                    for v in versions
                ],
                "links": {"next": None},
            },
        )
        for v in versions:
            blob = (blobs or {}).get(v) or self.blobs.get((ns, name, v)) or artifact(ns, name, v)
            self.blobs[(ns, name, v)] = blob
            filename = f"{ns}-{name}-{v}.tar.gz"
            self.json(
                f"{base}/versions/{urllib.parse.quote(v, safe='')}/",
                {
                    "artifact": {"sha256": hashlib.sha256(blob).hexdigest()},
                    "download_url": f"{self.url}/api/v3/plugin/ansible/content/published/collections/artifacts/{filename}",
                    "metadata": {"dependencies": {"other.coll": ">=1.0.0"}},
                    "created_at": None,
                    "signatures": [],
                },
            )
            # Like Galaxy: the download URL redirects to a signed Pulp URL.
            self.routes[f"/api/v3/plugin/ansible/content/published/collections/artifacts/{filename}"] = (
                302,
                b"",
                {"Location": f"/pulp/content/{filename}?sig=x"},
            )
            self.routes[f"/pulp/content/{filename}?sig=x"] = (200, blob, {})

    def replace(self, ns, name, version, blob, uploaded=None):
        """Serve new bytes for a version.

        It gets a new upload time, or `uploaded` (re-uploading an earlier
        artifact brings back that upload's time).
        """
        self.uploaded[(ns, name, version)] = uploaded or self.upload_time()
        args = self.collections[(ns, name)]
        self.add_collection(ns, name, args["versions"], args["deprecated"], {version: blob}, args["updated"])

    def remove_collection(self, ns, name):
        prefix = f"/api/v3/collections/{ns}/{name}/"
        for path in [p for p in self.routes if p.startswith(prefix)]:
            del self.routes[path]
        self.listed.pop((ns, name), None)
        for key in [k for k in self.uploaded if k[:2] == (ns, name)]:
            del self.uploaded[key]
        self._update_listing()


@pytest.fixture
def galaxy():
    mock = MockGalaxy()
    thread = threading.Thread(target=mock.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    yield mock
    mock.server.shutdown()
    mock.server.server_close()


@pytest.fixture
def client(galaxy):
    return GalaxyClient(galaxy.url, backoff=0, retries=3, timeout=5)


@pytest.fixture
def store(tmp_path):
    return Store(Buckets.local(tmp_path), "https://dl.example/{sha256-checksum}")
