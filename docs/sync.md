# How a sync works

`sync` finds collections that may differ from Galaxy, queues them, and runs
`reconcile` on each queued collection until its time budget runs out.
Reconcile is the only code that changes the mirror; everything else only
decides which collections to look at. Which Galaxy endpoints each step uses
is in [galaxy-api.md](galaxy-api.md), and where the results go is in
[storage.md](storage.md).

## reconcile

`reconcile(ns, name)` (`reconcile.py`) brings one collection's index file in
line with Galaxy, using only the contract endpoints `ansible-galaxy` itself
uses:

1. **Collection:** a 404 yanks every version. Nothing else counts as a
   deletion, so a flaky or truncated listing can never remove anything.
2. **Versions Galaxy lists that the index doesn't:** fetch the version
   detail, download the artifact, check it against Galaxy's sha256 and its
   own `MANIFEST.json`, store it by sha256, extract its plugins, and add an
   index line.
3. **Versions the index has:** the sha256 of the artifact Galaxy publishes
   now is compared with the mirrored artifact's, but only when the version
   is yanked, when `--revalidate` asks, or when Galaxy's `created_at`
   changed (see [Artifact checks](#artifact-checks)).
4. **Versions the index has that Galaxy no longer lists:** yanked, once
   the version's own detail page returns 404. A delete while a long listing
   is paged can shift another version past the reader, so the listing alone
   is never enough. A version whose page is still there is kept, and one
   Galaxy can't answer for is kept and retried as a failure.
5. **Deprecation** is copied from the collection onto every line.

Writes go artifacts, then index files, then aggregate files, so the index
never points at an artifact that isn't there. A collection's
[Galaxy v3 view](galaxy-v3-view.md) is written right after its index file,
version pages before the listing.

Reconcile also re-extracts mirrored versions whose stored extraction is
missing or older than `plugins.EXTRACTION_FORMAT`, so extractor fixes reach
versions that were already mirrored.

New versions are mirrored oldest upload first. Past the run's deadline,
reconcile mirrors no more: it writes what it has and leaves the rest for the
next run. So a collection with thousands of versions is imported across
several runs, with its lines still in publication order.

Every 100 versions mirrored or rechecked, reconcile also saves progress part
way through: the index file with the lines so far, those versions' Galaxy
pages and the collection's plugin lines, then `state.json`. A run that stops
part way through a long collection loses at most the last 100 versions, not
the whole collection. Saving part way makes the end of the reconcile rewrite
the whole Galaxy view, since yanks, deprecation and rechecks can change lines
already written.

## The queue

What to reconcile is kept in `state.json` (`pending`), not in the run. A run
that's stopped, times out or crashes loses nothing, and every run does the
same work, so it doesn't matter which run picks it up. Each `sync`:

1. **Finds changes** (below) and queues them, then saves the feed cursor.
   Something found is saved in the queue before the cursor moves past it.
2. **Takes requests.** `refresh` doesn't reconcile anything itself. It writes
   a request object under `requests/` in the state bucket, which the next
   sync queues and deletes. A request names collections or namespaces,
   asks for a sweep, or acknowledges conflicts. A namespace that can't be listed keeps its request for
   the next run.
3. **Works what was requested,** ahead of everything else.
4. **Sweeps** once a day (below), queueing everything that differs. It comes
   after the requested collections, since it can take most of the budget,
   and isn't started once the budget has run out: it runs to the end once
   started, so the next run does it instead.
5. **Works the rest of the queue** in order: new uploads, then what the
   listing and the sweep found, which includes the initial import's backlog.
   Collections with outstanding failures come last. A collection leaves the
   queue once reconciled; one that ran out of time part way stays queued.
6. **Stops starting new work** once `--budget` minutes (default 45) have
   passed since the run started, sweep included. That applies to requested
   collections too, so a requested collection too big for one run carries on
   in the next, still first in the queue. The run then writes the aggregate
   files and the failure report.

`state.json` is saved after every 100 units of work (each version mirrored
or rechecked counts one, and so does each collection reconciled), at the
end of the run, and whenever something is queued. It isn't saved after
every collection: once the mirror is full it's several MB. If a run stops
between saves, the collections finished since stay queued and are
reconciled again; their index files already have the new lines, so that
costs two Galaxy requests each, and reconcile records any version state
that was lost.

The initial import is just a sync with no saved state. Its listing queues
every collection, and runs work through the backlog 45 minutes at a time,
with new uploads going ahead of it.

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
5. **Version count**, after reconciling. Galaxy's published count is compared with the total
   the mirror last saw. Deleting a version below a collection's highest
   changes nothing else `sync` can see, but lowers the count; a shortfall is
   recorded as `deletions_suspected`. Versions known to be missing from
   Galaxy's search index (below) aren't in its count, so they aren't
   expected.

If neither the feed nor the listing can be read, Galaxy is most likely down.
The run reconciles nothing and saves no cursor, so the next run looks again.

### Daily: the sweep

When the last sweep is over a day old, or a `refresh` without targets asked
for one, `changes.sweep` compares everything:

1. Read the whole upload feed (about 533 pages, pages are capped at 100
   rows) and compare every published version with the index: versions
   deleted on Galaxy but not yet yanked, versions published but not mirrored,
   and versions whose upload time changed since the last reconcile.
2. Read the collection listing for deprecation changes.
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

### Why not Pulp's repository diffs

Galaxy's Pulp API lists exactly what each repository version added and
removed, which would make deletions immediate. It was dropped: Galaxy keeps
only the newest one or two repository versions, so a burst of uploads
between runs loses them; its deprecation diffs can't say which collection
changed; and it's the API most likely to be locked down.

## Artifact checks

The mirror never overwrites: a version's `cksum` never changes. If Galaxy
ever serves a different sha256 for a version the mirror already holds,
reconcile downloads it and compares its contents with ours (`artifacts.contents_digest`): each
file's bytes and each link's target, with `MANIFEST.json` and `FILES.json`
compared as parsed JSON. Timestamps, owners, modes and member order don't
count.

- **Same contents** (a rebuild of unchanged sources): ours is kept and
  nothing is flagged. Reported as `repacked`.
- **Different contents:** ours is kept, the index line gets
  `upstream_conflict: true`, Galaxy's artifact is saved under `conflicts/` in
  the state bucket, and it's recorded in the version's conflict
  ([below](#conflicts)).

Each version's verdict is cached with Galaxy's sha256, so the same
artifact is downloaded once.

### Conflicts

A version has at most one open conflict, which collects every differing
artifact Galaxy serves for it, oldest first: its sha256, Galaxy's upload
time, when a reconcile first saw it, and the files added, removed and
changed from ours. Its life:

1. **Opened** by the first differing artifact. It's listed in the tracking
   issue's description and posted as a comment of its own.
2. **More artifacts** (an author republishing the same version) are added
   to it without another comment. The description shows the latest.
3. **Acknowledged** once a person has inspected it and decided the mirror's
   original stands: `refresh --ack NS.NAME:VERSION --note TEXT`, or
   `refresh.yml` with `acknowledge` and `note`. The next sync records who
   decided when, with the note. The conflict leaves the description, its
   comment gets the note and is marked resolved (minimized), and later
   artifacts are still recorded but never reported. `--strict` ignores it.
   Acknowledging again replaces the note.
4. **Resolved** if Galaxy serves our contents again: the index line's flag
   clears. A later difference opens a new conflict.

Acknowledging doesn't change the index: the flag stays while Galaxy serves
something else, since that's what clients need to know. Nothing is ever
removed. Each collection's conflicts, in every state, are published as
`conflicts/<ns>/<name>.json` in the index bucket
([format](index-spec/README.md#conflict-records)), the public audit trail of
why the mirror's artifact differs from Galaxy's. Galaxy's artifacts stay in
the private state bucket.

## Failures and conflicts

A failure never stops a run. It's recorded in the ledger in `state.json`,
retried by every `sync`, and cleared once it succeeds. Each `sync` starts by
listing the outstanding failures, so a run's log shows what it will retry.
Recorded failures exit 0; `--strict` exits 1.

| Kind | Meaning |
|---|---|
| `galaxy_unavailable` | Timeouts, 429 or 5xx; retried automatically |
| `galaxy_http_<code>` | Any other HTTP error |
| `sha256_mismatch` | Downloaded bytes don't match Galaxy's sha256 |
| `manifest_mismatch`, `bad_artifact` | The tarball's `MANIFEST.json` is wrong or unreadable |
| `extract_error` | Extraction failed; the version is still mirrored and re-extracted on later reconciles |
| `deletions_suspected` | Galaxy publishes fewer versions than the mirror last saw |
| `unexpected` | Any other exception while reconciling a collection |
| `cdn_purge` | Purging changed index files from the CDN failed ([CDN purging](storage.md#cdn-purging)) |

Failures of the Galaxy-wide checks are recorded under the collection `*`,
with the version naming the check: `published`, `feed`, `listing`, `count`,
`sweep`, `requests` or `purge`. Each clears when that check next works.

Conflicts aren't failures: they aren't retried, and they stay open until
Galaxy serves our contents again. Every run writes `failures.json` with the
outstanding failures and open conflicts, acknowledged or not.

### Notifications

`failures.json` stays in the private state bucket. People read the report on
a tracking issue instead:

- `sync --report FILE` writes the outstanding failures and the open
  conflicts no one has acknowledged as a Markdown block, between `<!-- galaxy-sync:report -->` and
  `<!-- /galaxy-sync:report -->` markers. The scheduled workflow keeps the
  block at the top of the issue's description (`.github/scripts/issue_body.py`
  replaces the previous block, or puts the first one above whatever is
  there, so notes kept below it survive), editing it only when it changed.
  The block has no timestamp or attempt counts, so it changes only when what
  is outstanding does. Editing the description doesn't notify anyone, which
  suits routine churn, and resolved failures just disappear from it.
- `sync --alerts DIR` writes what a person should hear about, one file per
  comment: each conflict opened, and the failures outstanding for over a
  day (each failure once) together. The workflow posts each as a comment,
  which emails the issue's subscribers. Transient failures that clear within
  a day never notify. A conflict's comment starts with a hidden
  `<!-- galaxy-sync:conflict NS.NAME VERSION OPENED -->` marker.
- `sync --acknowledged FILE` lists the conflicts acknowledged this run, by
  marker, with the note. The workflow finds each one's comment
  (`.github/scripts/resolve_comments.py`), adds the note and minimizes it as
  resolved. Comments posted before the markers existed have to be hidden by
  hand.

These and the conflict records leave out what shouldn't be public: the state bucket key of a
conflict's saved artifact, and URLs in `unexpected` messages, which can name
the storage endpoint. `failures.json` has them.

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
| `refresh.yml` | manual, with targets and `revalidate` | `refresh`, which only writes a request |
| `rebuild.yml` | manual, with `dl`, an ansible-core ref and `galaxy_view` | pauses syncs by disabling `sync.yml`, waits for a running sync, `rebuild`, resumes; syncs already paused by hand stay paused |

GitHub's `schedule` trigger runs late or not at all too often to rely on, so
`sync.yml` has none. A Cloudflare Worker with a cron trigger, in
`scheduler/`, sends it a `workflow_dispatch` every 10 minutes instead,
unless the workflow is disabled. A dispatch starts within seconds. To pause
syncs, disable `sync.yml` (`gh workflow disable sync.yml`); to run one now,
dispatch it by hand.

Syncs share one concurrency group that never cancels a running job. Runs
must not overlap: each reads `state.json` and writes it back whole, and
reconcile rewrites index files the same way. GitHub keeps only the newest
waiting run in a group and cancels the others. For syncs that loses nothing,
because the work is in the queue. It's why `refresh` writes a request instead
of queueing a job, and why `rebuild` pauses syncs rather than joining the
group.

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

## Yanks and takedowns

- **A Galaxy delete is a yank.** The line and artifact stay, with
  `"yanked": true`. New resolutions skip it; lock files still install it. A
  version that comes back on Galaxy with the same bytes is un-yanked.
- **Takedowns** (malware, leaked secrets, legal requests) remove the line
  and artifact with a recorded reason. They're manual, and not built yet.
