# Storage

galaxy-sync writes four buckets. With `--store DIR` (the default, `./out`)
each is a subdirectory of `DIR`; with `--r2` they're R2 buckets. Clients
read the index and artifact buckets, `ansible-galaxy` reads the Galaxy view,
and only the sync reads the state bucket.

| Bucket | Local directory | Served as | Cache |
|---|---|---|---|
| Index | `index-root/` | `index.alai.sh` | Short (`max-age=60`) with ETags |
| Artifacts | `dl-root/` | `dl.alai.sh` | `public, max-age=31536000, immutable` |
| Galaxy view | `galaxy-root/` | `galaxy.alai.sh` | Short |
| State | `state/` | Private | |

`config.json` holds the artifact URL template, so the artifact bucket can
move without changing clients. It's set by `rebuild --dl TEMPLATE` (or the
`dl.alai.sh` default on a new store or a plain `rebuild`), which also
rewrites every Galaxy page's download URL to match; `sync` keeps whatever
it holds. To try a different location with alai clients
only, editing `config.json` is enough; the next `rebuild` resets it.

## Layout

```
index bucket
  config.json                  formats, artifact URL template, moved-to
  collections.json             every collection and when its index file last changed
  plugins.jsonl                the plugin index
  index/<ns>/<name>.jsonl      one line per version

artifact bucket
  sha256/<aa>/<sha256>.tar.gz  the exact bytes Galaxy published

Galaxy view bucket
  index.html, api/index.html   API roots
  api/v3/collections/<ns>/<name>/index.html
  api/v3/collections/<ns>/<name>/versions/index.html
  api/v3/collections/<ns>/<name>/versions/<ver>/index.html

state bucket
  state.json                   cursors, the queue, per-collection state, the failure ledger, conflicts
  requests/<time>-<id>.json    `refresh` requests waiting for the next sync
  failures.json, failures.md   the failure report
  builtin.jsonl                ansible.builtin's plugin lines (rebuild --builtin)
  extract/<ns>/<name>.json.zst each collection's stored extractions, by artifact sha256
  plugins/<ns>/<name>.jsonl    each collection's plugin lines, merged into plugins.jsonl
  conflicts/<aa>/<sha256>.tar.gz  Galaxy's artifact for each conflict
```

The formats clients read are specified in [index-spec/](index-spec/README.md)
and [galaxy-v3-view.md](galaxy-v3-view.md). The Galaxy view needs one rewrite
rule on its host, described there.

Every write is skipped when the stored content is already the same, so an
unchanged run writes nothing. A new version's Galaxy page is written
without that check, since it can't exist yet.

## state.json

| Key | Meaning |
|---|---|
| `feed_cursor` | `pulp_created` of the newest upload `sync` has seen |
| `repo_published` | The repo metadata timestamp at the last `sync` |
| `last_sweep` | When the last sweep finished; the next is due a day later |
| `plugins_pending` | Collections reconciled since `plugins.jsonl` was last merged; normally empty between runs |
| `pending.<ns.name>` | The queue: `source` (`requested`, `uploaded`, `listed` or `swept`), `queued`, and `revalidate` if asked for |
| `collections.<ns.name>` | Per collection: `updated` (when its index file last changed), `upstream_updated`, `upstream_deprecated` and `upstream_count` (Galaxy's values at the last reconcile), and `versions.<v>` with `created_at` and the cached `upstream` comparison verdict |
| `failures` | The failure ledger, keyed `<ns.name>@<version>#<kind>`: `collection`, `version`, `kind`, `message`, `first_seen`, `last_seen`, `attempts`, and `alerted` once it was notified as persistent |
| `conflicts` | Versions where Galaxy serves different contents from the mirror: `collection`, `version`, `ours`, `upstream`, `seen`, `changes`, `saved`, and `resolved` once Galaxy serves our contents again |

The failure kinds are listed in [sync.md](sync.md#failures-and-conflicts).

## R2

`--r2` is configured only through the environment, so credentials never end
up in a command line, shell history or workflow log:

| Variable | |
|---|---|
| `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | A scoped R2 API token |
| `ALAI_INDEX_BUCKET`, `ALAI_DL_BUCKET`, `ALAI_GALAXY_BUCKET`, `ALAI_STATE_BUCKET` | Bucket names |

Locally, keep them in a gitignored `.env` and run
`uv run --env-file .env galaxy-sync sync --r2 ...`.

The backend uses R2's S3 API. Each bucket's objects get its Cache-Control
from the table above (`private, no-store` for state), and a Content-Type by
extension: JSON for `.json` and the Galaxy view's `index.html` pages, plain
text for `.jsonl`. `state.json` is saved with a conditional write
(`If-Match` on the ETag of what the run read), so two runs can never both
save it.

## Hosting and cost

The jobs run on GitHub Actions in the `alai-sh` org
([scheduling](sync.md#scheduling)), with the R2 token as secrets in a
protected `r2` environment and the bucket names as variables; an Azure
Container Apps Job is the fallback. At R2 list prices the mirror is ~25 GB
at $0.015/GB-month. Egress is free, and clients mostly hit the CDN cache.
Writes stay inside the free tier: mirroring a version is a handful of
writes.

### Keeping reads down

Class B reads are what a run costs most of, so a run reads only what it
works on:

- **`plugins.jsonl`** is patched, not rebuilt. Reconcile writes each
  collection's plugin lines to `plugins/<ns>/<name>.jsonl` in the state
  bucket from the extractions it already read, and queues the collection in
  `plugins_pending`. The run ends by reading `plugins.jsonl`, replacing those
  collections' lines and writing it back. Only `rebuild` reads every
  collection's extractions (~4.6k objects once the mirror is full) to make
  them all again.
- **Extractions are one object per collection**, Zstandard-compressed JSON
  keyed by artifact sha256. Reconcile needs every version's extraction, to
  re-extract outdated ones and to build the plugin lines, so it reads one
  object rather than one per version, and writes it once if anything
  changed. Versions repeat each other's plugins and routing, which
  Zstandard's long window finds and gzip's 32 KB one doesn't:
  community.general's 243 versions are 41 MB of JSON, 5.3 MB gzipped and
  0.27 MB as stored.
- **Unchanged writes are skipped** by reading each object first. A change to
  a collection reads every one of its existing version pages, which is
  bounded by the size of the collections that changed. New versions' pages
  skip the read, so mirroring a version takes three calls: `HEAD` and `PUT`
  for the artifact (the `HEAD` skips re-uploading one left by a run that
  stopped before writing its index file), and a `PUT` for the Galaxy page.
- **A run with nothing to do** reads `config.json` and `state.json`, lists
  `requests/`, and writes nothing.

### Moving extractions to one object per collection

Releases before this layout stored each artifact's extraction as
`extract/<aa>/<sha256>.json`. They're still read when a collection has no
combined object yet, and removed once it's saved, so a collection moves
over the next time it's reconciled. After deploying, run `rebuild` once to
move every collection; the fallback can then be removed in a later release.

## Trying the output locally

Python's own HTTP server serves the Galaxy view and the artifacts the way the
production host must. On a fresh store, `sync --budget 1` reconciles the
requested collection first, then works the initial import's backlog until
the minute is up; the rest stays queued. Point the artifact URLs at the local server with `rebuild --dl`:

```bash
uv run galaxy-sync refresh community.library_inventory_filtering_v1
uv run galaxy-sync sync --budget 1
uv run galaxy-sync rebuild --dl 'http://127.0.0.1:8766/sha256/{sha256-prefix}/{sha256-checksum}.tar.gz'
python -m http.server -d out/galaxy-root 8765 &
python -m http.server -d out/dl-root 8766 &
ansible-galaxy collection install -s http://127.0.0.1:8765/ community.library_inventory_filtering_v1
```
