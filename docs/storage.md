# Storage

galaxy-sync writes three buckets. With `--store DIR` (the default, `./out`)
each is a subdirectory of `DIR`; with `--r2` they're R2 buckets. Clients
read the index and artifact buckets, and only the sync reads the state
bucket.

| Bucket | Local directory | Served as | Cache |
|---|---|---|---|
| Index | `index-root/` | `index.alai.sh` | `public, max-age=600, stale-while-revalidate=86400` with ETags, but `public, max-age=31536000, immutable` for `index/` |
| Artifacts | `dl-root/` | `dl.alai.sh` | `public, max-age=31536000, immutable` |
| State | `state/` | Private | |

`config.json` holds the artifact URL template, so the artifact bucket can
move without changing clients. A new store starts with the `dl.alai.sh`
one, and `refresh --dl TEMPLATE` moves it; every other run keeps whatever
it holds, so editing `config.json` by hand works too.

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
  requests/<time>-<id>.json    `refresh` requests (reconcile, sweep, rebuild, dl or acknowledge) waiting for the next sync
  failures.json                the failure report
  builtin.jsonl                ansible.builtin's plugin lines, from ansible-core's sdist
  extract/<ns>/<name>.json.zst each collection's stored extractions, by artifact sha256
  conflicts/<aa>/<sha256>.tar.gz  Galaxy's artifact for each conflict
```

The formats clients read are specified in [index-spec/](index-spec/README.md).

Every write is skipped when the stored content is already the same, so an
unchanged run writes nothing.

## Index files

Each collection's index file is published once, by its sha256:
`index/<aa>/<sha256>.jsonl` in the index bucket. Clients find it through the
template `index/{sha256-prefix}/{sha256-checksum}.jsonl` in `config.json`
and the collection's sha256 in `collections.json`; the sync finds it through
the same sha256 in `state.json` (`collections.<ns.name>.sha256`), which is
what `collections.json` is written from. A published file never changes, so
it's cached for a year, like an artifact, and never purged.

When a file changes, the new one is published, then state points at it.
Nothing is overwritten: a published file stays until pruned. A run that
stops before saving state leaves it pointing at the file it was replacing,
so `collections.json` never names one that isn't published, and the next
run reconciles that collection again from there: what it had mirrored since
state was last saved, at most 100 versions, is downloaded again. A second
copy by name, in the state bucket, would avoid that, for a GET and PUT more
on every change; a run stopping part way is rare enough not to.

**Pruning.** Once a day, a run that writes `collections.json` lists the
index bucket's `index/` and retires each file that listing doesn't name,
recording when in `state.json` (`retired_index_files`). One still retired
7 days later (`store.RETIRED_GRACE`) is deleted; one named again, as when a
yank is undone, stops being retired. The spec asks for 7 days, far longer
than any copy of `collections.json` lives in a cache, so a client holding an
older one still finds the files it names. Pruning also clears files left by
a run that stopped.

**Dropping fields.** Index lines used to carry `origin` and
`upstream_conflict`, which the spec no longer has (`store.DROPPED_FIELDS`).
The first syncs after they were dropped publish every collection's index
file again without them, after the requested collections, in (namespace,
name) order, keeping their place in `state.json` (`migration.after`) so a
run that runs out of budget carries on in the next. Until a file is
rewritten, `read_index` leaves those fields out of its lines, so a
reconcile writes it without them too. The old files are left for pruning.
Remove this once `migration.done` is set in the stored state.

## state.json

| Key | Meaning |
|---|---|
| `feed_cursor` | `pulp_created` of the newest upload `sync` has seen |
| `repo_published` | The repo metadata timestamp at the last `sync` |
| `last_sweep` | When the last sweep finished; the next is due a day later |
| `builtin` | The daily ansible-core check: `checked`, and the `version` and `format` (`EXTRACTION_FORMAT`) `builtin.jsonl` was extracted with |
| `rebuild` | A requested rebuild that isn't finished: `requested`, and `after`, the last collection done |
| `migration` | Rewriting index files without the dropped fields: `after`, the last collection rewritten, until `done` |
| `retired_index_files` | Index bucket keys under `index/` that no `collections.json` names, and when a prune first found each |
| `last_prune` | When retired index files were last pruned; the next is due a day later |
| `plugins_pending` | Collections reconciled since `plugins.jsonl` was last merged; normally empty between runs |
| `pending.<ns.name>` | The queue: `source` (`requested`, `uploaded`, `listed` or `swept`), `queued`, `revalidate` if asked for, and what change detection saw, so a reconcile that changes nothing can say why it ran: `uploads` (versions the upload feed saw), `unseen` (mirrored versions the sweep didn't see) and `reasons` (e.g. `updated_at changed` from the listing, `gone: 1.1.0` from the sweep) |
| `collections.<ns.name>` | Per collection: `updated` (when its index file last changed), `sha256` (of its index file, the key it's published under: the only record of which file is the collection's; a collection without one has no index file), `upstream_updated`, `upstream_deprecated` and `upstream_count` (Galaxy's values at the last reconcile), `unindexed` (versions Galaxy serves that its search index leaves out), and `versions.<v>` with `created_at` and the cached `upstream` comparison verdict |
| `failures` | The failure ledger, keyed `<ns.name>@<version>#<kind>`: `collection`, `version`, `kind`, `message`, `first_seen`, `last_seen`, `attempts`, and `alerted` once it was notified as persistent |
| `conflicts` | Versions where Galaxy serves different contents from the mirror ([conflicts](sync.md#conflicts)): `collection`, `version`, `ours`, `opened`, `observed` (each differing artifact: `sha256`, `uploaded`, `seen`, `changes` and `saved`, its key in this bucket), `acknowledged` (`at` and `note`) once a person has, and `resolved` once Galaxy serves our contents again. At most one is open per version, and none is ever removed |

The failure kinds are listed in [sync.md](sync.md#failures-and-conflicts).

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
Cloudflare's copy expires, `stale-while-revalidate` has it answer the next
request from that copy at once, `UPDATING`, while it fetches the file from
R2 for the requests after, so no request waits for R2 unless Cloudflare has
dropped its copy, or hasn't had one. Index files are named by their sha256,
so they never change and are cached for a year.

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

There's no `s-maxage`. Cloudflare treats it as `proxy-revalidate` and so
never serves an expired copy, which turns `stale-while-revalidate` off
([Cloudflare's Cache-Control directives](https://developers.cloudflare.com/cache/concepts/cache-control/)):
with `max-age=60, s-maxage=600, stale-while-revalidate=86400`, every
request after Cloudflare's copy expired came back `REVALIDATED`, having
waited for R2 (tested 2026-10-26). Enabling Always Online on the zone would
turn it off too.

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

- **`plugins.jsonl`** is patched, not rebuilt. Reconcile works out each
  collection's plugin lines from the extractions it already read, and
  queues the collection in `plugins_pending`. The run ends by reading
  `plugins.jsonl`, replacing those collections' lines, and
  `ansible.builtin`'s with `builtin.jsonl`, and writing it back.
  A collection left pending by a run that stopped before merging has its
  lines worked out again from its index file and extractions. Only a
  rebuild reads every collection's (~4.6k of each once the mirror is
  full) to make them all again.
- **Extractions are one object per collection**, Zstandard-compressed JSON
  keyed by artifact sha256. Reconcile needs every version's extraction, to
  re-extract outdated ones and to build the plugin lines, so it reads one
  object rather than one per version, and writes it once if anything
  changed. Versions repeat each other's plugins and routing, which
  Zstandard's long window finds and gzip's 32 KB one doesn't:
  community.general's 243 versions are 41 MB of JSON, 5.3 MB gzipped and
  0.27 MB as stored.
- **Unchanged writes are skipped** by reading each object first. Mirroring a
  version takes two calls: `HEAD` and `PUT` for the artifact (the `HEAD`
  skips re-uploading one left by a run that stopped before writing its
  index file). A changed index file is one `PUT`, as it was by name; an
  unchanged one is told by its sha256 in state, without a request.
- **Pruning lists the index bucket once a day**, one request per thousand
  index files, rather than on every run.
- **A run with nothing to do** reads `config.json` and `state.json`, lists
  `requests/`, and writes nothing.

## Trying the output locally

On a fresh store, `sync --budget 1` reconciles the requested collection
first, then works the initial import's backlog until the minute is up; the
rest stays queued. The index lands in `out/index-root/` and the artifacts in
`out/dl-root/`:

```bash
uv run galaxy-sync refresh community.library_inventory_filtering_v1
uv run galaxy-sync sync --budget 1
```
