"""Galaxy API client.

Contract endpoints are the ones ansible-galaxy itself uses (see
docs/galaxy-api.md), and the only ones whose answers can yank or mirror
anything. Everything else is best effort and its callers must cope with it
failing.
"""

import email.utils
import importlib.metadata
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from typing import cast

from .models import (
    GalaxyCollection,
    GalaxyListedCollection,
    GalaxyPage,
    GalaxyRepoMetadata,
    GalaxySearchPage,
    GalaxySearchRow,
    GalaxyVersion,
    GalaxyVersionDetail,
)

log = logging.getLogger(__name__)

BASE_URL = "https://galaxy.ansible.com"
USER_AGENT = f"alai-galaxy-sync/{importlib.metadata.version('alai-galaxy-sync')} (+https://github.com/alai-sh)"

_RETRY_STATUS = {429, 500, 502, 503, 504}
_PROGRESS_PAGES = 10  # log paging progress this often


def _retry_after(value: str | None) -> float | None:
    """Seconds from a Retry-After header, given as seconds or an HTTP date."""
    if not value:
        return None
    try:
        return max(float(value), 0)
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
    except TypeError, ValueError:
        return None
    return max(when.timestamp() - time.time(), 0)


class GalaxyError(Exception):
    def __init__(self, url: str, status: int | None, message: str) -> None:
        super().__init__(f"{url}: {status or ''} {message}".strip())
        self.url = url
        self.status = status

    @property
    def kind(self) -> str:
        """The failure-ledger kind: `galaxy_unavailable` if retrying may help, else `galaxy_http_<code>`."""
        if self.status is None or self.status in _RETRY_STATUS:
            return "galaxy_unavailable"
        return f"galaxy_http_{self.status}"


class NotFound(GalaxyError):
    pass


class GalaxyClient:
    """One request at a time, retrying timeouts, 429s and 5xx with backoff.

    Galaxy's own latency keeps the request rate low; on 429 or 5xx the client
    waits, honouring Retry-After. Running requests in parallel would need a
    shared rate limit across them.
    """

    def __init__(
        self,
        base_url: str = BASE_URL,
        *,
        timeout: float = 120,
        retries: int = 5,
        backoff: float = 5,
        max_wait: float = 300,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff  # seconds, times the attempt number
        self.max_wait = max_wait  # cap on a server's Retry-After
        self.requests = 0

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith(("http://", "https://")):
            # Pulp's pagination links come back as http://; use the base URL's scheme.
            parts = urllib.parse.urlsplit(path_or_url)
            base = urllib.parse.urlsplit(self.base_url)
            if parts.netloc == base.netloc:
                return urllib.parse.urlunsplit((base.scheme, *parts[1:]))
            return path_or_url
        return self.base_url + path_or_url

    def _get(self, path_or_url: str) -> bytes:
        url = self._url(path_or_url)
        last: Exception | None = None
        for attempt in range(self.retries):
            self.requests += 1
            # _url only ever returns an http(s) URL.
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})  # noqa: S310
            start = time.monotonic()
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310
                    data = resp.read()
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    log.debug("GET %s: 404", url)
                    raise NotFound(url, 404, "not found") from None
                if e.code not in _RETRY_STATUS:
                    raise GalaxyError(url, e.code, e.reason) from None
                last = e
                wait = _retry_after(e.headers.get("Retry-After"))
                wait = self.backoff * (attempt + 1) if wait is None else min(wait, self.max_wait)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                last = e
                wait = self.backoff * (attempt + 1)
            else:
                log.debug("GET %s: %d bytes in %.1fs", url, len(data), time.monotonic() - start)
                return data
            if attempt + 1 < self.retries:
                log.warning("GET %s: %s; retry %d/%d in %.0fs", url, last, attempt + 1, self.retries - 1, wait)
                time.sleep(wait)
        raise GalaxyError(url, None, f"gave up after {self.retries} attempts: {last}")

    def get_json(self, path_or_url: str) -> object:
        return json.loads(self._get(path_or_url))

    def _paged(self, url: str, what: str) -> Iterator[object]:
        """Every item of a paginated listing, following `links.next`, with progress logged."""
        next_url: str | None = url
        pages = items = 0
        while next_url:
            page = cast(GalaxyPage, self.get_json(next_url))
            data = page.get("data", page.get("results", []))
            yield from data
            pages += 1
            items += len(data)
            if pages % _PROGRESS_PAGES == 0:
                total = meta["count"] if (meta := page.get("meta")) else "?"
                log.info("%s: %d of %s read", what, items, total)
            next_url = links.get("next") if (links := page.get("links")) else None

    def download(self, url: str) -> bytes:
        # download_url is stable and redirects to a signed, short-lived URL;
        # urllib follows the redirect.
        return self._get(url)

    # Contract endpoints: the ones ansible-galaxy itself uses.

    def collection(self, namespace: str, name: str) -> GalaxyCollection | None:
        try:
            return cast(GalaxyCollection, self.get_json(f"/api/v3/collections/{namespace}/{name}/"))
        except NotFound:
            return None

    def versions(self, namespace: str, name: str) -> list[GalaxyVersion]:
        url = f"/api/v3/collections/{namespace}/{name}/versions/?limit=100"
        try:
            return cast(list[GalaxyVersion], list(self._paged(url, f"{namespace}.{name} versions")))
        except NotFound:
            return []

    def version_detail(self, namespace: str, name: str, version: str) -> GalaxyVersionDetail:
        quoted = urllib.parse.quote(version, safe="")
        return cast(GalaxyVersionDetail, self.get_json(f"/api/v3/collections/{namespace}/{name}/versions/{quoted}/"))

    # Best effort: callers must cope with these failing.

    def list_collections(self, namespace: str | None = None) -> Iterator[GalaxyListedCollection]:
        query = {"limit": "100"}
        if namespace:
            query["namespace"] = namespace
        url = "/api/v3/plugin/ansible/content/published/collections/index/?" + urllib.parse.urlencode(query)
        yield from cast(Iterator[GalaxyListedCollection], self._paged(url, "collection listing"))

    def repo_published(self) -> str:
        """When the `published` repo last changed, for any reason.

        Every upload, delete, deprecation or namespace edit creates a repo
        version, and this is the latest one's creation time. pulp_ansible's own
        sync uses it to skip remotes that haven't changed.
        """
        url = "/api/v3/plugin/ansible/content/published/collections/"
        return cast(GalaxyRepoMetadata, self.get_json(url))["published"]

    _SEARCH = "/api/v3/plugin/ansible/search/collection-versions/?"

    def published_feed(self) -> Iterator[GalaxySearchRow]:
        """Every version in the `published` repo, most recently uploaded first.

        Ordered by `pulp_created`, the upload time, so a release below a
        collection's highest version appears here even though the collection's
        `updated_at` doesn't change. Stop iterating to stop paging.
        """
        query = {"repository_name": "published", "order_by": "-pulp_created", "limit": "100"}
        url = self._SEARCH + urllib.parse.urlencode(query)
        yield from cast(Iterator[GalaxySearchRow], self._paged(url, "upload feed"))

    def published_count(self) -> int:
        """How many versions the `published` repo holds."""
        query = {"repository_name": "published", "limit": "1"}
        return cast(GalaxySearchPage, self.get_json(self._SEARCH + urllib.parse.urlencode(query)))["meta"]["count"]
