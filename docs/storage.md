# Storage

galaxy-sync writes three buckets. With `--store DIR` (the default, `./out`)
each is a subdirectory of `DIR`; with `--r2` they're R2 buckets. Clients
read the index and artifact buckets, and only the sync reads the state
bucket.

| Bucket | Local directory | Served as | Cache |
|---|---|---|---|
| Index | `index-root/` | `index.alai.sh` | `public, max-age=60, s-maxage=600, stale-while-revalidate=86400` with ETags, but `public, max-age=31536000, immutable` for `index/` |
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

## state.json

| Key | Meaning |
|---|---|
| `feed_cursor` | `pulp_created` of the newest upload `sync` has seen |
| `repo_published` | The repo metadata timestamp at the last `sync` |
| `last_sweep` | When the last sweep finished; the next is due a day later |
| `builtin` | The daily ansible-core check: `checked`, and the `version` and `format` (`EXTRACTION_FORMAT`) `builtin.jsonl` was extracted with |
| `rebuild` | A requested rebuild that isn't finished: `requested`, and `after`, the last collection done |
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

Purging the CDN ([below](#cdn-purging)) is configured the same way, and
only with `--r2`. It's optional: with none of these set, a run leaves the
cache to expire. Setting some but not all of them is an error.

| Variable | |
|---|---|
| `CLOUDFLARE_PURGE_TOKEN` | A Cloudflare API token that can only purge the zone's cache |
| `CLOUDFLARE_ZONE_ID` | The zone serving the index bucket (`alai.sh`) |
| `ALAI_INDEX_URL` | Where the index bucket is served, `https://index.alai.sh`; purged URLs are this plus the key |

The backend uses R2's S3 API. Each bucket's objects get its Cache-Control
from the table above (`private, no-store` for state), and a Content-Type by
extension: JSON for `.json`, plain text for `.jsonl`. The index bucket's `index/` gets the
artifacts' Cache-Control instead. `state.json` is saved with a conditional write
(`If-Match` on the ETag of what the run read), so two runs can never both
save it.

## CDN purging

Cloudflare caches the index bucket's files that change (`config.json`,
`collections.json`, `plugins.jsonl` and the conflict records) for their
`s-maxage`, 10 minutes, and browsers and other caches for their `max-age`,
a minute, which no purge reaches. With `stale-while-revalidate`, the first
request after Cloudflare's copy expires gets it at once while Cloudflare
checks R2 behind it. So that clients see a change as soon as the purge
spreads, `sync` purges every one of those files it changed, in the
order [the spec](index-spec/README.md#publishing-order) asks:
`collections.json` is written last, the other changed files are purged
before it's written, and it's purged after. Index files are never purged:
each is published under a new key. Only the index bucket is purged.

10 minutes is the sync's schedule (`*/10` in `scheduler/wrangler.toml`), so
a failed purge leaves Cloudflare at most one sync behind; change
`storage.INDEX_CACHE` with the schedule. A failed purge doesn't stop the
run, and isn't retried, since the old files expire by then anyway. A stale
`collections.json` only names older index files, which are kept for a week,
so clients see an older index, never a broken one. It's recorded in the
failure ledger as `*` / `purge` / `cdn_purge` and cleared by the next purge
that works, so a token that's expired or been revoked shows up on the
tracking issue.

### The purge token

The token can purge `alai.sh`'s cache and nothing else. It's separate from
the R2 token: R2's S3 credentials can't call Cloudflare's API, and one
token for both would give each job permissions it doesn't need.

To create it, in the Cloudflare dashboard of the account holding the
`alai.sh` zone:

1. Open *My Profile > API Tokens* (or, for a token owned by the account
   rather than a person, *Manage Account > Account API Tokens*) and choose
   **Create Token**.
2. Under *Custom token*, choose **Get started**.
3. Name it `galaxy-sync cache purge`.
4. **Permissions:** one row, `Zone` | `Cache Purge` | `Purge`.
5. **Zone Resources:** `Include` | `Specific zone` | `alai.sh`.
6. Leave *Client IP Address Filtering* empty: GitHub's runners don't have
   fixed addresses. Set a *TTL* end date if you want it to expire, and put
   a reminder to replace it before then.
7. **Continue to summary**, check it reads *alai.sh - Cache Purge:Purge*,
   then **Create Token** and copy the token. It isn't shown again.

Check it works (the response has `"status": "active"`; a user token checks
at `/user/tokens/verify`, an account token at
`/accounts/<account id>/tokens/verify`):

```bash
curl -s https://api.cloudflare.com/client/v4/user/tokens/verify -H "Authorization: Bearer <token>"
```

The zone ID is under *Domains > alai.sh > Overview*, in the *API* section
near the bottom of the page.

Then, in this repository's *Settings > Environments > r2*:

- under *Environment secrets*, add `CLOUDFLARE_PURGE_TOKEN` with the token;
- under *Environment variables*, add `CLOUDFLARE_ZONE_ID` with the zone ID
  and `ALAI_INDEX_URL` with `https://index.alai.sh`.

To replace the token, create a new one the same way, update the secret,
then delete the old token from the page it was created on.

## Hosting and cost

The jobs run on GitHub Actions in the `alai-sh` org
([scheduling](sync.md#scheduling)), with the R2 and purge tokens as secrets in a
protected `r2` environment and the bucket names, zone ID and index URL as variables; an Azure
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
