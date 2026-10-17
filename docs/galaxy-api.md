# Galaxy API

What galaxy-sync requests from galaxy.ansible.com, and what's known about how
Galaxy behaves. Sources: ansible-core `5b1a96f0fa` (2026-10-09), galaxy_ng
`cede01d` (2026-10-13), pulp_ansible 0.30.2 (which galaxy_ng pins, and where
the v3 API, uploads and deletes live), pulpcore 3.105.22 and galaxy-importer
0.4.39, plus live requests. Galaxy's API is slow and unreliable: 3-60 s per
request, with timeouts and 502s.

## Contract endpoints

The endpoints `ansible-galaxy` itself uses (`lib/ansible/galaxy/api.py`,
`collection/galaxy_api_proxy.py`). They're the only ones treated as
guaranteed, and the only ones `reconcile` uses.

| Request | Fields used |
|---|---|
| `GET /api/v3/collections/<ns>/<name>/` | `created_at`, `updated_at`, `deprecated` (best effort). A 404 means the collection is deleted |
| `GET /api/v3/collections/<ns>/<name>/versions/?limit=100` | `data[]` (or `results[]`) `.version`, `.created_at`, `.requires_ansible`; follow `links.next`. A missing collection returns an empty list, never a 404 |
| `GET /api/v3/collections/<ns>/<name>/versions/<ver>/` | `artifact.sha256`, `download_url`, `metadata.dependencies`, `created_at`. A 404 means the version is deleted |
| `GET <download_url>` | 302 to a signed, time-limited URL; the bare content URL is 403 |

`artifact.size`, `manifest`, `files` and `deprecated` aren't in the contract:
size and plugins come from the tarball, and `deprecated` is best effort.
The contract paths 302 to `/api/v3/plugin/ansible/content/published/collections/index/...`;
request the contract paths anyway.

## Best-effort endpoints

Used only to decide which collections to reconcile. Base
`/api/v3/plugin/ansible`; timings from 2026-10-15.

| Endpoint | Used by | Gives |
|---|---|---|
| `content/published/collections/` | `sync` | `{"published": ...}`, the creation time of the repo's newest version. Moves on any change. pulp_ansible's own sync uses it to skip unchanged remotes. ~1 s |
| `search/collection-versions/?repository_name=published&order_by=-pulp_created&limit=100` | `sync` and its sweep | The upload feed: every published version, newest upload first, with `pulp_created`, `dependencies` and `requires_ansible`; `meta.count` is the published version count. Pages are capped at 100 rows, ~3.5-5 s each at any offset |
| `content/published/collections/index/?limit=100` | `sync` and its sweep | The listing: every collection with `updated_at`, `highest_version` and `deprecated`. ~46 pages, not strictly ordered, so read it to the end |
| `content/published/collections/index/?limit=100&namespace=<ns>` | `sync`, taking a `refresh <ns>` request | One namespace's collections |

The feed is a sorted snapshot of what's published now, not an event log:
each version appears once, at the time its published artifact was
uploaded.

`next` links come back as `http://`; `GalaxyClient._url` rewrites them to the
base URL's scheme.

### Ruled out

Probed anonymously on 2026-10-13:

| Endpoint | Result |
|---|---|
| `content/published/collections/all-collections/`, `all-versions/` | 401: `_deny_all` in galaxy_ng's access statements |
| `/api/v3/collections/all/`, `/api/v3/collection_versions/all/` | 404: galaxy_ng routes them to `NotFoundView` |
| `content/published/namespaces/`, `client-configuration/` | 401 |
| `/api/_ui/v1/collection-versions/?repository=published&sort=-pulp_created` | Works, but ~20 s per page; the feed plus `sign_state` and `repository_list` |
| `/api/_ui/v1/imports/collections/` | Works, but namespace, name and version are hidden anonymously |
| `.../versions/<ver>/docs-blob/` | Works: Galaxy's own extracted docs. A possible cross-check for extraction |
| `?fields=` on v3 endpoints | Ignored |

The Pulp API (`/api/pulp/api/v3`) answers anonymously with repository-version
diffs (`repositories/ansible/ansible/<id>/versions/<n>/` and
`content/ansible/collection_versions/?repository_version_added=<href>`), and
each content object's `sha256`, `manifest` and `files`. It isn't used: see
[sync.md](sync.md#why-not-pulps-repository-diffs).

## How Galaxy behaves

- **`updated_at` is the highest version's `pulp_last_updated`**
  (`CollectionViewSet.get_queryset`), not the time of the last change.
  Uploading or deleting any other version leaves it unchanged.
  The listing and the collection endpoint share a serializer, so their
  `updated_at` values compare directly.
- **Every content change makes one repository version:** an approved upload,
  a deleted version or collection, a deprecation change, a namespace edit.
  pulpcore discards a version that changes nothing. Only the newest two are
  kept: galaxy_ng sets `retain_repo_versions = 1` so deleted content can be
  purged; why a second survives isn't clear from the code. On 2026-10-15 they
  were about an hour apart.
- **Deletes are immediate and permanent:** `orphan_cleanup` removes the
  version and its artifact at once. Deleting the last version deletes the
  collection. A delete is refused while another collection depends on the
  version. There's no yank, and deprecation is collection-level only.
- **Uploads reach `published` 20-30 s after the import completes:** they go
  to staging first, then auto-approval moves them.
- **Versions from before the 2023-05 migration** all report the same
  `created_at` (e.g. `2023-05-08T20:24:33Z`), so lines with equal timestamps
  are ordered by version.

## Measurements

- **Galaxy (2026-10-09):** 4,576 collections, ~53.1k published versions. The
  latest version of each is ~1.2 GB gzipped (5.8 GB extracted); every version
  is ~20-25 GB. Growth is ~12k versions and ~5 GB a year. The largest uploader
  is `waldur` (~3.2k dev builds in a year).
- **Reconciling** `community.library_inventory_filtering_v1` and
  `ansible.utils` (73 versions, 14 MB, 2026-10-10) took 150 requests and
  ~6.5 min, almost all waiting on Galaxy; ~40 s of that was a 0.25 s pause
  per request the client no longer makes. Re-running with nothing changed
  took 2 requests.
- **A full sweep** (2026-10-15): 533 requests, 1,856 s, 53,261
  versions. That included a 0.25 s pause per request (~133 s) the client no
  longer makes, so ~29 min without it.
- **The plugin index** for the latest version of every collection: 13.6 MB
  raw, 1.17 MB gzipped (57.5k modules, 17.5k roles, 5.4k playbooks, 1.3k
  filters, 3.5k routing-only lines). Extraction findings are in
  [alai-plugin-extractor's AGENTS.md](https://github.com/alai-sh/alai-plugin-extractor/blob/main/AGENTS.md).
