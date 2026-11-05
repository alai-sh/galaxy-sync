# How a sync works

`sync` finds collections that may differ from Galaxy, queues them, and runs
`reconcile` on each queued collection until its time budget runs out.
Reconcile is the only code that changes the mirror; everything else only
decides which collections to look at. Which Galaxy endpoints each step uses
is in [galaxy-api.md](galaxy-api.md), and where the results go is in
[storage.md](storage.md).

## reconcile

`reconcile(ns, name)` (`reconcile.py`) brings one collection's index file in
line with Galaxy by running alai-index's
[`galaxy-mirror`](https://alai.sh/alai-index/mirroring/), which uses only
the contract endpoints `ansible-galaxy` itself uses. That page describes
what it adds, yanks, checks and never overwrites. galaxy-sync runs it on a
local copy of the files it reads, then publishes what it wrote
([storage](storage.md#the-alai-index-tree)).

When alai-index lists a version's plugin extraction as out of date, reconcile
fetches its artifact from the artifact bucket and extracts its plugins again.
A collection's plugins catch up with an alai-index upgrade as it's synced.

reconcile keeps what alai-index doesn't. That's the failure ledger, the
collection's change-detection state and the conflicts. A version Galaxy
serves another artifact for is reported by `galaxy-mirror` without being
written, and reconcile compares its contents with ours
([below](#artifact-checks)).

New versions are mirrored oldest upload first, `CHUNK` (10) at a time. Each
chunk is published before the next starts, so a run that stops part way
through a long collection loses at most one chunk. Past the run's deadline,
reconcile mirrors no more and leaves the rest for the next run. So a
collection with thousands of versions is imported across several runs, with
its lines still in publication order.

## The queue

What to reconcile is kept in `state.json` (`pending`), not in the run. A run
that's stopped, times out or crashes loses nothing, and every run does the
same work, so it doesn't matter which run picks it up. Each `sync`:

1. **Finds changes** (below) and queues them, then saves the feed cursor.
   Something found is saved in the queue before the cursor moves past it.
2. **Takes requests.** `request` doesn't do anything itself. It writes a
   request object under `requests/` in the state bucket, which the next sync
   takes and deletes ([below](#requests)). A namespace that can't be listed
   keeps its request for the next run.
3. **Works what was requested,** ahead of everything else: the collections
   to mirror, then the jobs in order ([below](#jobs)).
4. **Sweeps** once a day (below), queueing everything that differs. It comes
   after the requested work, since it can take most of the budget,
   and isn't started once the budget has run out: it runs to the end once
   started, so the next run does it instead.
5. **Works the rest of the queue** in order: new uploads, then what the
   listing and the sweep found, which includes the initial import's backlog.
   Collections with outstanding failures come last. A collection leaves the
   queue once reconciled; one that ran out of time part way stays queued.
6. **Stops starting new work** once `--budget` minutes (default 45) have
   passed since the run started, sweep included. That applies to requested
   collections too, so a requested collection too big for one run carries on
   in the next, still first in the queue. The run then publishes
   `plugins.jsonl` and `collections.json`, and writes the failure report.

`state.json` is saved after every 100 units of work (each version mirrored
counts one, and so does each collection reconciled or worked by a job), at
the end of the run, and after finding changes, taking each request and the
sweep. It isn't saved after every collection, as
once the mirror is full it's several MB. If a run stops between saves, the
collections finished since stay queued and are reconciled again. Their
index files are published already, but the published `collections.json` may
still name the old ones, as it's only published before each save. Reconciling
them again costs a few Galaxy requests each, and downloads again what wasn't
in the published `collections.json`.

The initial import is just a sync with no saved state. Its listing queues
every collection, and runs work through the backlog 45 minutes at a time,
with new uploads going ahead of it. The first sweep is a day after it starts,
as the listing already covered everything.

## Finding what changed

### Every run

`changes.discover` works out what changed since the last run, as cheaply as
possible:

1. **Repo metadata.** Galaxy's `published` repo records when it last changed,
   for any reason. If that hasn't moved, finding changes stops after one
   request, and with an empty queue that's the whole run.
2. **Upload feed.** Every published version, newest upload first. It's read
   back to the saved cursor (minus an hour, for uploads waiting on approval
   and for offset paging shifting under concurrent changes). This finds
   every new version, including releases below a collection's highest.
3. **Collection listing.** About 46 pages. It runs on the first run (the
   initial import), when the feed fails, and when the repo changed but the
   feed shows no new upload: that's a deprecation, a deleted collection or
   highest version, or a namespace edit. It compares each collection's
   `updated_at` and `deprecated` flag with what the last reconcile saw.
4. **Queue** what was found, then save the cursor and repo timestamp.
5. **Version count**, after reconciling, when the repo changed or a sweep
   ran. Galaxy's published count is compared with the total the mirror last
   saw. Deleting a version below a collection's highest
   changes nothing else `sync` can see, but lowers the count; a shortfall is
   recorded as `deletions_suspected`. Versions known to be missing from
   Galaxy's search index (below) aren't in its count, so they aren't
   expected.

If neither the feed nor the listing can be read, Galaxy is most likely down.
The run reconciles nothing and saves no cursor, so the next run looks again.

### Daily: the sweep

When the last sweep is over a day old, or `request sweep` asked
for one, `changes.sweep` compares everything:

1. Read the whole upload feed (about 533 pages, pages are capped at 100
   rows) and compare every published version with the index: versions
   deleted on Galaxy but not yet yanked, versions published but not mirrored,
   and versions whose upload time changed since the last reconcile.
2. Read the collection listing, queueing what it finds as on every run.
   If only the listing fails, the rest still stands.
3. Queue what differs, then check the version count again after
   reconciling.

Rows can shift between pages during the walk. New uploads only make rows
appear twice. A delete can make a row look gone, but reconcile checks
before yanking. If any feed page fails, nothing is queued, since a partial
walk would make everything after it look deleted. The next run sweeps again.

Galaxy's search index, which the feed and count come from, can also lose
versions Galaxy still serves. Each mirrored version the sweep doesn't see is
passed to reconcile, which records those Galaxy still lists as `unindexed`
in the collection's state. Later sweeps report them as unindexed rather
than gone and leave them out of the count, but still queue the collection
each day, since only reconcile can tell when one is really deleted. A
version back in the feed is dropped from `unindexed`.

The walk takes 30-40 minutes, so new uploads wait for that run to finish.

### How soon a change reaches the mirror

| Change on Galaxy | Picked up by |
|---|---|
| New version, any number | next `sync` |
| Deleted collection or highest version | next `sync` |
| Deprecation | next `sync`, or the next sweep if it coincided with an upload |
| A version's upload time changes | next `sync` if it's the highest version, otherwise next sweep |
| Deleted version below the highest | flagged by the next `sync`, yanked by the next sweep |
| Anything a failed run missed | next sweep |

"Next `sync`" means queued by it. While a backlog is being worked through,
new uploads go ahead of it, but a requested collection goes ahead of both.

## Artifact checks

The mirror never overwrites, so a version's `sha256` never changes. When
Galaxy serves a different sha256 for a version the mirror already holds,
ours is kept and the index line doesn't change. Every such artifact is a
conflict, even a rebuild of the same files. Reconcile downloads Galaxy's
artifact once, saves it under `conflicts/` in the state bucket and records
it in the version's conflict ([below](#conflicts)).

The conflict also says how Galaxy's files differ from ours
(`artifacts.contents_digest`). Each file's bytes and each link's target are
compared, with `MANIFEST.json` and `FILES.json` compared as parsed JSON.
Timestamps, owners, modes and member order don't count, so a rebuild of the
same files says so.

Reconcile gives the latest upload time of each open conflict's artifacts to
`galaxy-mirror` as `--checked`, so it doesn't check the version again until
Galaxy's `created_at` changes. The sweep reads them too, so it doesn't queue
the version again as re-uploaded. A conflict whose artifacts have no upload
time is checked on every reconcile until the next sighting fills it in.

### Conflicts

A version has at most one open conflict, which collects every artifact
Galaxy serves for it, oldest first. Each has its sha256, Galaxy's upload
time, when a reconcile first saw it, and the files added, removed and
changed from ours. A conflict goes through these steps.

1. **Opened** by the first artifact that isn't ours. It's listed in the tracking
   issue's description, sorted by collection, until it's resolved. No
   comment is posted.
2. **More artifacts** (an author republishing the same version) are added
   to it. The description shows the latest.
3. **Resolved** if Galaxy serves our artifact again. A later difference
   opens a new conflict.

Conflicts never change the index. Nothing is ever removed from
`state.json`, which keeps every conflict, resolved ones included. The
tracking issue lists the open ones, and Galaxy's artifacts stay in the
private state bucket.

## Requests

`galaxy-sync request ACTION` asks the next sync to do something, without
contacting Galaxy. Each action is named after what it does to the index.

| Action | Does |
|---|---|
| `mirror NS[.NAME]... [--revalidate]` | Reconciles those collections, or every collection in those namespaces, ahead of everything else. `--revalidate` also checks every mirrored artifact against Galaxy's |
| `sweep` | Runs the sweep on this sync rather than once a day. It only finds and queues what differs, and the queue reconciles those |
| `generate-plugins [NS[.NAME]...] [--from-plugin-cache]` | A [job](#jobs) that fetches each version's artifact from the artifact bucket and extracts its plugins again with alai-index's `generate-plugins`, for every collection when there are no targets. `--from-plugin-cache` fetches only the artifacts whose extraction is missing or out of date, as after an alai-index upgrade that changed extraction. A lost `plugins.jsonl` is made again |
| `generate-indexes [NS[.NAME]...]` | A [job](#jobs) that fetches each version's artifact and makes its line again with alai-index's `generate-indexes`, for every collection when there are no targets. Each line that changes is listed in the run's output |

`mirror` and `sweep` differ in what they do. `mirror` reconciles the
collections it names, at a few Galaxy requests each. `sweep` reads Galaxy's
whole upload feed and listing, about 600 requests, to find the collections
that need it, and only queues those.

The `request.yml` workflow makes one request for each dispatch.

### Jobs

A `generate-*` request becomes a job in `state.json` (`jobs`). Jobs are
worked in the order they were requested, after the collections to mirror,
a collection at a time in name order. Each job's progress is kept
(`after`, the last collection done), so one that runs out of budget, or a
run that stops, carries on from there. A version whose artifact isn't in
the artifact bucket is recorded as `artifact_missing`, and a collection
alai-index refuses as `alai_index`, and the job carries on. The next
reconcile of the collection downloads a missing artifact from Galaxy again,
if Galaxy still serves the bytes the index line's sha256 names, but doesn't
redo the job for that version.

A collection's artifacts are fetched and worked 50 at a time (`BATCH`), and
each batch is deleted before the next is fetched. That's for jobs and for a
reconcile extracting out-of-date plugins again. A collection with many
large artifacts then still fits in the runner's temporary space, which the
sync workflow puts in RAM (`/dev/shm`).

A `generate-plugins` job with `--from-plugin-cache` costs two reads and one
alai-index command for a collection with nothing out of date.

## Failures and conflicts

A failure never stops a run. It's recorded in the ledger in `state.json`,
retried by every `sync` (in its place if the collection is queued, otherwise
after the queue if there's time left), and cleared once it succeeds. An `extract_error` is
the exception, as reconciling the collection again doesn't extract the
version again, and so is an `artifact_lost`, which needs a person. Neither
they nor `artifact_missing` are cleared by a reconcile that checks the
version without a failure: only extracting it again, or putting the
artifact back, does. A collection is still reconciled for other reasons,
and each reconcile tries to restore its missing artifacts. Before working,
each `sync` lists the outstanding failures, so a run's log shows what it
will retry.
Recorded failures exit 0; `--strict` exits 1.

| Kind | Meaning |
|---|---|
| `galaxy_unavailable` | Timeouts, 429 or 5xx; retried automatically |
| `galaxy_http_<code>` | Any other HTTP error |
| `sha256_mismatch` | Downloaded bytes don't match Galaxy's sha256 |
| `manifest_mismatch`, `bad_artifact` | The tarball's `MANIFEST.json` is wrong or unreadable |
| `bad_detail` | Galaxy's page for the version has no download URL or sha256 |
| `extract_error` | Extraction failed. The version is still mirrored, and the plugin index has none of its plugins until it's extracted again. When the extractor is fixed, a `generate-plugins` request for the collection tries again, and so does the collection's next reconcile once alai-index lists its extraction as out of date |
| `alai_index` | alai-index refused a command, which leaves the tree as it was |
| `artifact_missing` | A version's artifact isn't in the artifact bucket, so it can't be installed or extracted again. The next reconcile downloads it from Galaxy again, writing it only if its sha256 is the index line's |
| `artifact_lost` | An `artifact_missing` Galaxy can't give back: it deleted the version or serves other bytes. Not retried on its own; a person puts the artifact back |
| `deletions_suspected` | Galaxy publishes fewer versions than the mirror last saw |
| `unexpected` | Any other exception while reconciling a collection |

Failures of the Galaxy-wide checks are recorded under the collection `*`,
with the version naming the check: `published`, `feed`, `listing`, `count`,
`sweep` or `requests`. Each clears when that check next works.

Conflicts aren't failures: they aren't retried, and they stay open until
Galaxy serves our artifact again. Both are kept in `state.json`.

### Notifications

`state.json` stays in the private state bucket. People read the report on a
tracking issue instead:

- `sync --report FILE` writes the outstanding failures and the open
  conflicts as a Markdown block. It starts with a count of each kind of
  problem there is (`report.SECTIONS`), linking to its heading. Each heading
  has a sentence on what the problem means, and its table in a collapsed
  section. The block goes between `<!-- galaxy-sync:report -->` and
  `<!-- /galaxy-sync:report -->` markers. The scheduled workflow keeps the
  block at the top of the issue's description (`.github/scripts/issue_body.py`
  replaces the previous block, or puts the first one above whatever is
  there, so notes kept below it survive), editing it only when it changed.
  The block has no timestamp or attempt counts, so it changes only when what
  is outstanding does. Editing the description doesn't notify anyone, which
  suits routine churn, and resolved failures just disappear from it.
- `sync --alerts DIR` writes what a person should hear about as a comment
  file: the failures outstanding for over a day (each failure once). The
  workflow posts it as a comment, which emails the issue's subscribers.
  Transient failures that clear within a day never notify. Conflicts are
  only in the description, so a new one notifies no one.

These leave out what shouldn't be public: the state bucket key of a
conflict's saved artifact, and URLs in `unexpected` messages, which can name
the storage endpoint. `state.json` has them.

A run can't report that runs have stopped. So each successful run also pings
a heartbeat URL, and the heartbeat service alerts when the pings stop. Set it
to expect a ping every hour with an hour's grace. Successful runs normally
ping every 10-15 minutes, and the longest one, with the sweep, takes about an
hour, so an alert means about two hours without a successful run. One failed
run doesn't alert; the next success pings.

## Scheduling

The workflows are in `.github/workflows/`:

| Workflow | Trigger | Runs |
|---|---|---|
| `sync.yml` | dispatched every 10 minutes by the scheduler; runs once `GALAXY_SYNC_REF` is set | `sync --report --alerts`, then the tracking issue update, the notification and the heartbeat |
| `request.yml` | manual, with an action, targets, and `revalidate` or `from-plugin-cache` | `request`, which only writes a request |

GitHub's `schedule` trigger runs late or not at all too often to rely on, so
`sync.yml` has none. A Cloudflare Worker with a cron trigger, in
`scheduler/`, sends it a `workflow_dispatch` every 10 minutes instead,
unless the workflow is disabled. A dispatch starts within seconds. To pause
syncs, disable `sync.yml` (`gh workflow disable sync.yml`); to run one now,
dispatch it by hand.

Syncs share one concurrency group that never cancels a running job. Runs
must not overlap: each reads `state.json` and writes it back whole, and
reconcile rewrites `collections.json` and the plugin cache the same way. GitHub keeps only the newest
waiting run in a group and cancels the others. For syncs that loses nothing,
because the work is in the queue. It's why `request` writes a request object
instead of starting a job itself.

As a last guard, `save_state` refuses to save if `state.json` changed since
the run read it, e.g. because of a run started by hand. On R2 that's a
conditional write, so it can't race. The run that's refused stops with an
error, and its work stays queued for the next.

### The scheduler

The Worker reads `sync.yml`'s state and dispatches it on `main`, using a
fine-grained personal access token limited to this repository with
**Actions: Read and write** and nothing else. It fits Cloudflare's free
plan: one of its 5 cron triggers per account, 144 of its 100,000 requests a
day, and two API calls per run, well inside the CPU limit since waiting on
GitHub doesn't count. The Worker has no R2 bindings or credentials.

To set it up, with Node.js installed:

1. Create the token under GitHub *Settings > Developer settings >
   Fine-grained tokens*, with resource owner `alai-sh` and only this
   repository selected.
2. From `scheduler/`, log in to the Cloudflare account that holds the R2
   buckets and deploy:

   ```bash
   npx wrangler login
   npx wrangler deploy
   npx wrangler secret put GITHUB_TOKEN
   ```

   Cron triggers need the account to have a workers.dev subdomain, even
   though this Worker isn't served on it. On an account that has never
   used Workers, the deploy fails with code 10063 until you open *Workers &
   Pages* in the dashboard once, which creates one; then deploy again.
   Where there's no browser for `wrangler login`, set `CLOUDFLARE_API_TOKEN`
   (an API token from the "Edit Cloudflare Workers" template) and
   `CLOUDFLARE_ACCOUNT_ID` instead.
3. Within 10 minutes a `Sync` run appears with the event `workflow_dispatch`.
   The Worker's *Logs* tab in the Cloudflare dashboard shows each trigger,
   or follow them live with `npx wrangler tail`.

A failed dispatch fails the cron event in the Worker's logs, and the
heartbeat alerts once syncs stop.

#### Replacing the token

GitHub emails the token's owner before it expires. To replace it:

1. On the token's page under *Fine-grained tokens*, choose **Regenerate
   token** and pick a new expiry. The repository and permission stay the
   same. The old token stops working straight away; a trigger that fires
   before the next step fails harmlessly and the following one catches up.
2. From `scheduler/`, store the new token:

   ```bash
   npx wrangler secret put GITHUB_TOKEN
   ```

   It's used from the next trigger on, with no deploy needed.
3. Check that the next `Sync` run appears, or that `npx wrangler tail`
   shows `dispatched sync.yml on main`.

A token that has already expired is replaced the same way. Syncs missed
meanwhile lose nothing: the work is in the queue.

#### Changing the Worker

Changes to `wrangler.toml` or `src/index.js` take effect on the next
`npx wrangler deploy` from `scheduler/`, not on merge. CI builds the Worker
on every push and pull request with `wrangler deploy --dry-run`, which needs
no Cloudflare credentials and deploys nothing, so a broken change fails
before it's merged. It pins the wrangler version in `ci.yml`; deploy with the
same one, e.g. `npx wrangler@4.151.1 deploy`. A deploy replaces the
Worker in place and keeps its token. Vars and the cron schedule always come
from `wrangler.toml`, so dashboard edits to them are lost on the next deploy.
Renaming the Worker (`name`) creates a second one: delete the old one with
`npx wrangler delete --name <old name>` and set the token on the new one.

### Releases

The workflows always run from `main`, so they check out the
code at the `GALAXY_SYNC_REF` repository variable instead: a release tag such
as `v0.3.0`, or a commit. A merge to `main` changes nothing until the
variable is moved. To release, tag the commit, push the tag, and set the
variable; to roll back, set it to the previous tag. Until it's set, syncs
are skipped and the manual workflows fail. Dependencies come from
that ref's `uv.lock` (`uv run --locked`).

Only the code is pinned. The workflow files themselves always come from
`main`, so an edit to one takes effect when it's merged.

Rolling back past a change to `state.json`'s format can leave state the
older code doesn't expect. Older code keeps fields it doesn't know without
using them, but check what changed in between before going back.
