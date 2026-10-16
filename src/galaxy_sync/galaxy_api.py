"""A static copy of the Galaxy v3 endpoints `ansible-galaxy collection install` reads.

Built entirely from index lines, so it never needs another Galaxy request and
can be rebuilt at any time. Each endpoint is a JSON object stored at
`<path>index.html`, where `<path>` is the URL path the client requests (always
ending in `/`). The host maps a request for `<path>` to that key and ignores
the query string, see docs/galaxy-v3-view.md.

    /                                            available_versions, for a server URL without /api/
    api/                                         available_versions
    api/v3/collections/<ns>/<name>/              updated_at drives the client cache
    api/v3/collections/<ns>/<name>/versions/     every version on one page
    api/v3/collections/<ns>/<name>/versions/<ver>/
"""

from . import semver
from .models import IndexLine

SERVER_ROOT = "/"
API_ROOT = "/api/"
V3 = "/api/v3/"


def page_key(path: str) -> str:
    """Storage key for the endpoint at URL path `path`."""
    return path.lstrip("/") + "index.html"


def collection_path(namespace: str, name: str) -> str:
    return f"{V3}collections/{namespace}/{name}/"


def versions_path(namespace: str, name: str) -> str:
    return collection_path(namespace, name) + "versions/"


def version_path(namespace: str, name: str, vers: str) -> str:
    # ansible-galaxy puts the version in the path unquoted, `+` included.
    return f"{versions_path(namespace, name)}{vers}/"


def download_url(template: str, line: IndexLine) -> str:
    """Fill the index's `dl` template for one line, as described in the index spec."""
    namespace, _, name = line["name"].partition(".")
    return (
        template.replace("{sha256-checksum}", line["cksum"])
        .replace("{sha256-prefix}", line["cksum"][:2])
        .replace("{namespace}", namespace)
        .replace("{name}", name)
        .replace("{version}", line["vers"].replace("+", "%2B"))
    )


def root(path: str) -> dict[str, object]:
    """The API root at `path`. `available_versions` paths are relative to it.

    The client first tries the configured server URL as the API root, and only
    then `<url>/api` with no trailing slash, which a static host can't serve.
    So the server root is an API root too.
    """
    return {"available_versions": {"v3": V3.removeprefix(path)}}


def _created(lines: list[IndexLine]) -> str | None:
    return min((line["published"] for line in lines), default=None)


def _highest(lines: list[IndexLine]) -> str | None:
    """Highest stable version, or the highest pre-release if there's no stable one."""
    versions = [line["vers"] for line in lines]
    stable = [v for v in versions if not semver.is_prerelease(v)]
    return max(stable or versions, key=semver.sort_key, default=None)


def _available(lines: list[IndexLine]) -> list[IndexLine]:
    """Versions a client may pick. Galaxy has no yank: deleted versions are just gone."""
    return [line for line in lines if not line["yanked"]]


def collection(namespace: str, name: str, lines: list[IndexLine], updated: str) -> dict[str, object]:
    """`GET /api/v3/collections/<ns>/<name>/`.

    The client caches the version listing until `updated_at` changes, so it's
    the index file's last-changed time rather than Galaxy's.
    """
    available = _available(lines)
    highest = _highest(available)
    return {
        "href": collection_path(namespace, name),
        "namespace": namespace,
        "name": name,
        "deprecated": any(line.get("deprecated") for line in lines),
        "versions_url": versions_path(namespace, name),
        "highest_version": (
            {"href": version_path(namespace, name, highest), "version": highest} if highest is not None else None
        ),
        "created_at": _created(lines) or updated,
        "updated_at": updated,
    }


def versions(namespace: str, name: str, lines: list[IndexLine]) -> dict[str, object]:
    """`GET /api/v3/collections/<ns>/<name>/versions/?limit=100`, newest version first.

    A static host can't paginate, so every version is on the one page and
    `links.next` is null, whatever `limit` and `offset` were asked for.
    """
    available = sorted(_available(lines), key=lambda line: semver.sort_key(line["vers"]), reverse=True)
    path = versions_path(namespace, name)
    data: list[dict[str, object]] = []
    for line in available:
        item: dict[str, object] = {
            "version": line["vers"],
            "href": version_path(namespace, name, line["vers"]),
            "created_at": line["published"],
            "updated_at": line["published"],
            "requires_ansible": line.get("requires_ansible"),
            "marks": [],
        }
        data.append(item)
    return {
        "meta": {"count": len(data)},
        "links": {"first": path, "previous": None, "next": None, "last": path},
        "data": data,
    }


def version(line: IndexLine, dl_template: str) -> dict[str, object]:
    """`GET /api/v3/collections/<ns>/<name>/versions/<ver>/`."""
    namespace, _, name = line["name"].partition(".")
    return {
        "version": line["vers"],
        "href": version_path(namespace, name, line["vers"]),
        "created_at": line["published"],
        "updated_at": line["published"],
        "requires_ansible": line.get("requires_ansible"),
        "marks": [],
        "artifact": {
            "filename": f"{namespace}-{name}-{line['vers']}.tar.gz",
            "sha256": line["cksum"],
            "size": line["size"],
        },
        "collection": {"name": name, "href": collection_path(namespace, name)},
        "download_url": download_url(dl_template, line),
        "name": name,
        "namespace": {"name": namespace},
        "signatures": [],  # the index doesn't carry signatures
        "metadata": {"dependencies": {dep["name"]: dep["req"] for dep in line["deps"]}},
    }
