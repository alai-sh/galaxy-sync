# Galaxy v3 view

**Status:** draft, stopgap until the alai client exists.

A static copy of the Galaxy v3 endpoints that `ansible-galaxy collection
install` reads, so existing tooling can install from the mirror today:

```bash
ansible-galaxy collection install -s https://galaxy.alai.sh/ community.general
```

```ini
# ansible.cfg
[galaxy]
server_list = alai

[galaxy_server.alai]
url = https://galaxy.alai.sh/
```

It's built entirely from the [index](index-spec/README.md) files by
`galaxy_api.py`, never from Galaxy responses, so it can be rebuilt at any time
(`galaxy-sync rebuild`) and can't show anything the index doesn't.
Downloads point at the same artifacts as the index.

## What the client requests

From `lib/ansible/galaxy/api.py` in ansible-core. Only these fields are read;
the view adds a few more so responses look like Galaxy's.

| Request | Fields the client reads | Stored at |
|---|---|---|
| `GET <server>` | `available_versions.v3`, relative to `<server>` | `index.html` (`"api/v3/"`) and `api/index.html` (`"v3/"`) |
| `GET /api/v3/collections/<ns>/<name>/` | `updated_at`, only when the client's response cache is on (the default) | `api/v3/collections/<ns>/<name>/index.html` |
| `GET /api/v3/collections/<ns>/<name>/versions/?limit=100` | `data[].version`, `data[].requires_ansible`, `links.next` | `.../versions/index.html` |
| `GET /api/v3/collections/<ns>/<name>/versions/<ver>/` | `namespace.name`, `collection.name`, `version`, `download_url`, `artifact.sha256`, `metadata.dependencies`, `href`, `signatures[].signature` | `.../versions/<ver>/index.html` |
| `GET <download_url>` | The tarball; its sha256 must equal `artifact.sha256` | the `dl` URL of the index |

Notes:

- **The server URL can be the host or `/api/`.** The client first treats the
  configured URL as the API root. Only if that fails does it try
  `<url>/api`, without a trailing slash, which the rewrite rule below can't
  map. So the host root is an API root too, pointing `v3` at `api/v3/`.
- **`<ver>` is not percent-encoded** by the client. `1.0.0+build` is
  requested as `.../versions/1.0.0+build/`, and stored under that key.
- **One page per collection.** A static host can't paginate, so the listing
  holds every version and `links.next` is `null` whatever `limit` or `offset`
  asked for. The largest collection on Galaxy (~3.2k versions) is under 1 MB.
- **`updated_at`** of a collection is when its index file last changed, not
  Galaxy's. The client keeps its cached version listing until this changes.
- **A missing collection is a 404**, which the client reports as "not found".

## Mapping to the index

| Index | Galaxy view |
|---|---|
| `yanked: true` | Left out of the listing, so it can't be resolved; its version page stays. Galaxy has no yank, so this matches a Galaxy delete. Unlike the alai client, an exact pin (`ns.name:==1.0.0`) to a yanked version fails too |
| `deprecated` | Collection `deprecated: true` |
| `deps` | `metadata.dependencies` as `{name: req}` |
| `cksum`, `size` | `artifact.sha256`, `artifact.size` |
| `published` | `created_at` and `updated_at` of the version |
| `dl` template | `download_url` |

`signatures` is always empty: the index doesn't carry signatures.

`highest_version` is the highest non-yanked stable version, or the highest
pre-release if there's no stable one.

## Hosting requirements

The host must map each request to a stored object the way Python's
`http.server` does, which is what local testing uses
(`python -m http.server -d out/galaxy-root`):

1. **A path ending in `/` serves `<path>index.html`.** On Cloudflare this is
   a URL Rewrite (Transform) Rule on the hostname, with no Worker:
   when `ends_with(http.request.uri.path, "/")`, rewrite the path to
   `concat(http.request.uri.path, "index.html")`.
2. **The query string is ignored** for the lookup. Ignoring it in a cache
   rule's cache key needs an Enterprise plan, so the same rewrite rule also
   sets the query to empty. Rewrites run before the cache, so that should
   also stop `?limit=100` and `?limit=100&offset=0` being cached
   separately. If not, each variant is one extra read from R2 per cache
   lifetime, which is harmless.
3. **`+` in a path is literal**, not a space.
4. **Anything else is a `404`.** Content-Type doesn't matter to the client,
   but uploads to R2 should set `application/json` despite the `.html` name.
   The pages are named `index.html` only so that a stock static server
   serves them for a path ending in `/`.

Cache lifetimes follow the index: short (`max-age=60`) for everything under
`api/`, since listings change on every publish.

To verify on R2 before relying on it: that the rewrite applies before the R2
lookup, that two query strings for one page share a cache entry (the second
request's `cf-cache-status` is `HIT`), and that `+` isn't decoded to a
space.

## Not supported

- Search, roles (`v1`), namespaces, publishing and anything that needs a
  token. The client only checks for `v3` in `available_versions` before
  installing.
- `ansible-galaxy collection download` names each file after the last segment
  of `download_url`, so with content-addressed artifacts the files (and the
  `requirements.yml` it writes) use `<sha256>.tar.gz`. They still install,
  since the client reads the name and version from `MANIFEST.json`.
- Collections deleted from Galaxy keep a collection page with an empty
  listing rather than a 404, since storage never deletes objects. The client
  reports both the same way.
