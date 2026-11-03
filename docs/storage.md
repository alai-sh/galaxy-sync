# Storage

galaxy-sync writes three buckets. With `--store DIR` (the default, `./out`)
each is a subdirectory of `DIR`; with `--r2` they're R2 buckets. Clients
read the index and artifact buckets, and only the sync reads the state
bucket.

| Bucket | Local directory | Served as | Cache |
|---|---|---|---|
| Index | `index-root/` | `index.alai.sh` | `public, max-age=600` with ETags, but `public, max-age=31536000, immutable` for `index/` |
| Artifacts | `dl-root/` | `dl.alai.sh` | `public, max-age=31536000, immutable` |
| State | `state/` | Private | |

The index itself is built by [alai-index](https://github.com/alai-sh/alai-index),
which works on files on disk. A run copies what it needs from the buckets
and publishes what alai-index wrote ([below](#the-alai-index-tree)).

`config.json` holds the artifact URL template, so the artifact bucket can
move without changing clients. A new store starts with the `dl.alai.sh`
one. alai-index refuses to change `config.json` once the index has
collections, as their files would be left where the old one put them.

## Layout

```
index bucket
  config.json                  formats, index file and artifact URL templates, moved-to
  collections.json             every collection, when its index file last changed and its sha256
  plugins.jsonl                the plugin index
  index/<aa>/<sha256>.jsonl    each collection's index file, one line per version, by its sha256
  conflicts/<ns>/<name>.json   every conflict a collection has had, for audit

artifact bucket
  sha256/<aa>/<sha256>.tar.gz  the exact bytes Galaxy published

state bucket
  state.json                   cursors, the queue, per-collection state, the failure ledger, conflicts
  requests/<time>-<id>.json    `request`s (mirror, sweep, generate-plugins or generate-indexes) waiting for the next sync
  plugin-cache/<ns>/<name>.jsonl.zst  alai-index's plugin cache, each collection's file zstd-compressed
  conflicts/<aa>/<sha256>.tar.gz  Galaxy's artifact for each conflict
```

The formats clients read are specified in [index-spec/](index-spec/README.md).
`ansible.builtin`'s plugins aren't in `plugins.jsonl`, as alai-cli has them
itself.

Every write is skipped when the stored content is already the same, so an
unchanged run writes nothing.

## Index files

Each collection's index file is published once, by its sha256:
`index/<aa>/<sha256>.jsonl` in the index bucket. Clients find it through the
template `index/{sha256-prefix}/{sha256-checksum}.jsonl` in `config.json`
and the collection's sha256 in `collections.json`, and the sync finds it the
same way. `collections.json` is the index's record of each collection's
current index file. A published file never changes, so it's cached for a
year, like an artifact, and never purged.

When a file changes, the new one is published straight away. Nothing is
overwritten, and a published file stays until pruned. `collections.json` is
published later, at the end of the run and before each save of `state.json`,
after every file it names. A run that stops before then leaves the published
`collections.json` naming the old file. The collection is still queued, so
the next run reconciles it again from there.

**Pruning.** Once a day, a run that writes `collections.json` lists the
index bucket's `index/` and retires each file that listing doesn't name,
recording when in `state.json` (`retired_index_files`). One still retired
7 days later (`store.RETIRED_GRACE`) is deleted; one named again, as when a
yank is undone, stops being retired. The spec asks for 7 days, far longer
than any copy of `collections.json` lives in a cache, so a client holding an
older one still finds the files it names. Pruning also clears files left by
a run that stopped.

## state.json

| Key | Meaning |
|---|---|
| `feed_cursor` | `pulp_created` of the newest upload `sync` has seen |
| `repo_published` | The repo metadata timestamp at the last `sync` |
| `last_sweep` | When the last sweep finished; the next is due a day later |
| `jobs` | Requested jobs that aren't done yet, in order: `action` (`generate-plugins` or `generate-indexes`), `requested`, `targets` (none for every collection), `from_plugin_cache`, and `after`, the last collection done |
| `retired_index_files` | Index bucket keys under `index/` that no `collections.json` names, and when a prune first found each |
| `last_prune` | When retired index files were last pruned; the next is due a day later |
| `pending.<ns.name>` | The queue: `source` (`requested`, `uploaded`, `listed` or `swept`), `queued`, `revalidate` if asked for, and what change detection saw, so a reconcile that changes nothing can say why it ran: `uploads` (versions the upload feed saw), `unseen` (mirrored versions the sweep didn't see) and `reasons` (e.g. `updated_at changed` from the listing, `gone: 1.1.0` from the sweep) |
| `collections.<ns.name>` | Per collection, what change detection compares Galaxy with. `upstream_updated`, `upstream_deprecated` and `upstream_count` are Galaxy's values at the last reconcile, and `mirrored` is how many of its versions the index has that aren't yanked. `unindexed` lists versions Galaxy serves that its search index leaves out. `verdicts.<v>` is the last comparison of the artifact Galaxy serves for a version with ours, when they differ, with its `sha256`, `same_contents` and `uploaded` |
| `failures` | The failure ledger, keyed `<ns.name>@<version>#<kind>`: `collection`, `version`, `kind`, `message`, `first_seen`, `last_seen`, `attempts`, and `alerted` once it was notified as persistent |
| `conflicts` | Versions where Galaxy serves different contents from the mirror ([conflicts](sync.md#conflicts)): `collection`, `version`, `ours`, `opened`, `observed` (each differing artifact: `sha256`, `uploaded`, `seen`, `changes` and `saved`, its key in this bucket), and `resolved` once Galaxy serves our contents again. At most one is open per version, and none is ever removed |

The failure kinds are listed in [sync.md](sync.md#failures-and-conflicts).

## The alai-index tree

Each `sync` works on an alai-index tree in a temporary directory. At the
start of the run it holds `config.json` and `collections.json`. A
collection's index file and plugin cache file are fetched into it when it's
reconciled, and `plugins.jsonl` before the first change. alai-index's JSON
output lists every file a command wrote, and they're published straight after
it, apart from `plugins.jsonl` and `collections.json`.

- **Artifacts** go to the artifact bucket first, at the path of their URL,
  and are deleted from the tree, so a long run doesn't fill the disk.
- **The plugin cache** goes to `plugin-cache/<ns>/<name>.jsonl.zst` in the
  state bucket, zstd-compressed (`tree.ZSTD_LEVEL`). alai-index documents
  where each collection's file is, but what's in it is alai-index's own, so
  the sync only copies it. A file missing from the bucket is made again by
  alai-index from the artifacts.
- **Index files** go to the index bucket, in the order alai-index gives.
- **`plugins.jsonl` and `collections.json`** go to the index bucket at the
  end of the run, and before each save of `state.json`. So state never says a
  collection is done while the published `collections.json` names its old
  index file.

An extraction that's missing or out of date, after an alai-index upgrade or
a lost cache file, is listed in the output as `outdated`. Reconcile fetches
those artifacts from the artifact bucket, extracts their plugins again, and
publishes the result. `request generate-plugins --from-plugin-cache` does the
same for every collection over as many runs as it takes.

`scripts/migrate_to_alai_index.py` builds every collection's plugin cache up
front from the published index, rather than as each collection is next
reconciled.

## R2

`--r2` is configured only through the environment, so credentials never end
up in a command line, shell history or workflow log:

| Variable | |
|---|---|
| `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | A scoped R2 API token |
| `ALAI_INDEX_BUCKET`, `ALAI_DL_BUCKET`, `ALAI_STATE_BUCKET` | Bucket names |

Locally, keep them in a gitignored `.env` and run
`uv run --env-file .env galaxy-sync sync --r2 ...`.

The backend uses R2's S3 API. Each bucket's objects get its Cache-Control
from the table above (`private, no-store` for state), and a Content-Type by
extension: JSON for `.json`, plain text for `.jsonl`. The index bucket's `index/` gets the
artifacts' Cache-Control instead. `state.json` is saved with a conditional write
(`If-Match` on the ETag of what the run read), so two runs can never both
save it.

## Caching

The index bucket's files that change (`config.json`, `collections.json`,
`plugins.jsonl` and the conflict records) are cached for their `max-age`,
10 minutes, by Cloudflare and by browsers and other caches after it. Once
Cloudflare's copy expires, the next request at that Cloudflare location
waits while it fetches the file from R2 again, about 0.65 s from Brisbane.
There's no `stale-while-revalidate` (dropped 2026-11-03). It let Cloudflare
answer at once from an expired copy, but at a location with few requests
that copy could be hours old, and a client then saw an index that far
behind. Waiting for R2 once every 10 minutes is worth always getting a copy
at most one sync old. Index files are named by their sha256, so they never
change and are cached for a year.

Nothing is purged (2026-10-26). A change reaches clients up to 10 minutes
after the sync that made it, and as `collections.json` names index files by
sha256, an older copy only means newer versions show up later: every file it
names is still there. Purging what each sync changed would show changes
within seconds, but a purge removes Cloudflare's copy rather than marking it
stale, so the first request at each Cloudflare location after almost every
sync would wait for R2 (about 0.65 s from Brisbane), and Cloudflare would
fetch the 16.5 MB `plugins.jsonl` from R2 again after most of them. It also
needed a Cloudflare API token in the sync's environment.

10 minutes is the sync's schedule (`*/10` in `scheduler/wrangler.toml`), so
a client's copy is at most about one sync behind; change
`storage.INDEX_CACHE` with the schedule.

There's no `s-maxage`, so Cloudflare and clients keep a copy for the same
10 minutes.

A file keeps the Cache-Control it was written with until it's written
again, and the sync skips writes whose content is unchanged, so after
changing `storage.INDEX_CACHE` a file that rarely changes, `config.json`
above all, keeps the old one; see below.

### Changing an object's headers

To give an existing index bucket file new headers without changing it, copy
it onto itself with them, which R2 does in place, so it's never missing.
Cloudflare's copy keeps the old headers until it expires. With the R2
variables in `.env`:

```bash
uv run --env-file .env python - config.json <<'EOF'
import sys
from galaxy_sync.storage import INDEX_CACHE, R2Config, content_type

config = R2Config.from_env()
for key in sys.argv[1:]:
    config.client().copy_object(
        Bucket=config.index_bucket, Key=key, CopySource={"Bucket": config.index_bucket, "Key": key},
        MetadataDirective="REPLACE", ContentType=content_type(key), CacheControl=INDEX_CACHE,
    )
EOF
```

Name more keys after `config.json` to do several. Check with
`curl -sI https://index.alai.sh/config.json`.

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

- **A collection's files are fetched only when it's reconciled.** That's
  two objects, its plugin cache file and its index file. `plugins.jsonl` is
  fetched once, before the first change. Only a job without targets
  fetches every collection's files (~4.6k of each once the mirror is full).
- **The plugin cache is one object per collection,** zstd-compressed.
  Versions repeat each other's plugins and routing, which Zstandard's long
  window finds and gzip's 32 KB one doesn't. community.general's 243
  versions are 41 MB of JSON, 5.3 MB gzipped and 0.27 MB as stored.
- **Only what alai-index wrote is published.** It doesn't rewrite a file
  whose bytes wouldn't change, and an artifact already in the bucket isn't
  uploaded again, which costs a `HEAD` each.
- **Pruning lists the index bucket once a day**, one request per thousand
  index files, rather than on every run.
- **A run with nothing to do** reads `config.json`, `collections.json` and
  `state.json`, lists `requests/`, and writes nothing.

## Trying the output locally

On a fresh store, `sync --budget 1` reconciles the requested collection
first, then works the initial import's backlog until the minute is up; the
rest stays queued. The index lands in `out/index-root/` and the artifacts in
`out/dl-root/`:

```bash
uv run galaxy-sync request mirror community.library_inventory_filtering_v1
uv run galaxy-sync sync --budget 1
```
