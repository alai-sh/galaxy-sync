"""Purging changed index files from Cloudflare's cache.

The index bucket is served through Cloudflare, which caches each file for
its `max-age`. Purging what a run changed means clients see it as soon as
the purge spreads, not up to a `max-age` later. It's configured from the
environment only, like R2, and a run without it just leaves the cache to
expire.
"""

import json
import logging
import os
import urllib.error
import urllib.request
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import cast

from .storage import StorageError

log = logging.getLogger(__name__)

API_URL = "https://api.cloudflare.com/client/v4"

# URLs per purge request. Cloudflare's per-request limit depends on the plan;
# this is within all of them.
BATCH = 30

PURGE_ENV = {
    "token": "CLOUDFLARE_PURGE_TOKEN",
    "zone_id": "CLOUDFLARE_ZONE_ID",
    "index_url": "ALAI_INDEX_URL",
}


class PurgeError(Exception):
    pass


@dataclass(frozen=True)
class Purger:
    """Purges index bucket keys, served under `index_url`, from one Cloudflare zone's cache."""

    token: str = field(repr=False)
    zone_id: str
    index_url: str
    api_url: str = API_URL

    @classmethod
    def from_env(cls, environ: Mapping[str, str] = os.environ) -> Purger | None:
        """A purger, or None if none of its variables are set; some but not all is an error."""
        values = {attr: environ.get(var, "") for attr, var in PURGE_ENV.items()}
        if not any(values.values()):
            return None
        if missing := [PURGE_ENV[attr] for attr, value in values.items() if not value]:
            raise StorageError(f"purging the CDN needs these environment variables set too: {', '.join(missing)}")
        return cls(**values)

    def url(self, key: str) -> str:
        return f"{self.index_url.rstrip('/')}/{key}"

    def purge(self, keys: Iterable[str]) -> None:
        """Purge each key's URL, in batches; raises PurgeError on the first batch that fails."""
        urls = [self.url(key) for key in keys]
        for start in range(0, len(urls), BATCH):
            batch = urls[start : start + BATCH]
            self._post(batch)
            log.debug("purged %s", " ".join(batch))

    def _post(self, urls: list[str]) -> None:
        request = urllib.request.Request(  # noqa: S310
            f"{self.api_url}/zones/{self.zone_id}/purge_cache",
            data=json.dumps({"files": urls}).encode(),
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                body: object = json.load(response)
        except urllib.error.HTTPError as e:
            raise PurgeError(f"HTTP {e.code}: {_errors(e.read())}") from None
        except (OSError, ValueError) as e:
            raise PurgeError(str(e)) from None
        if not isinstance(body, dict) or body.get("success") is not True:
            raise PurgeError(f"not purged: {_errors(json.dumps(body).encode())}")


def _errors(data: bytes) -> str:
    """Cloudflare's error messages from a response body, or the start of the body."""
    try:
        body: object = json.loads(data)
    except ValueError:
        return data.decode(errors="replace")[:200]
    errors = cast(dict[str, object], body).get("errors") if isinstance(body, dict) else None
    if isinstance(errors, list) and errors:
        return "; ".join(
            str(cast(dict[str, object], e).get("message", e)) if isinstance(e, dict) else str(e)
            for e in cast(list[object], errors)
        )
    return json.dumps(body)[:200]
