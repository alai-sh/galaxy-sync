# galaxy-sync

Mirrors every collection on galaxy.ansible.com into the alai static index and
artifact store, so the alai client can install collections without
depending on Galaxy. The index is built by
[alai-index](https://github.com/alai-sh/alai-index), and galaxy-sync decides
what to mirror and when, and publishes the result to the buckets.

> [!WARNING]
> This isn't designed for public use. It exists only to sync the alai index
> at index.alai.sh, and its commands, options, storage layout and state can
> change at any time without notice. To install collections, use the alai
> client, which reads the published index.

## Goals

- **Mirror everything Galaxy publishes**, pre-releases and dev builds
  included. The client decides what to resolve.
- **Keep Galaxy out of the download path.** Its API can be slow and unreliable
  at times, so artifacts are mirrored, not linked.
- **No long-lived server.** Scheduled batch jobs write static files that any
  static host can serve.
- **Be polite to Galaxy:** one request at a time, backoff, a descriptive
  User-Agent, and as few requests as possible when nothing has changed.

## Usage

```bash
uv run galaxy-sync COMMAND [OPTIONS] ...
```

| Command | Purpose |
|---|---|
| `sync [--budget MINUTES] [--dry-run]` | Find what changed on Galaxy and queue it, take requests and work them first, sweep once a day, then reconcile the rest of the queue until the budget (default 45 minutes) runs out. The rest stays queued for the next run, so the initial import is built up over many runs. `--dry-run` only reports what changed |
| `request mirror NS[.NAME]... [--revalidate]` | Ask the next `sync` to reconcile these collections, or every collection in these namespaces, ahead of everything else. `--revalidate` also checks every mirrored version's artifact against the one Galaxy publishes now |
| `request sweep` | Ask the next `sync` to sweep, comparing every version Galaxy publishes with the index, rather than waiting for the daily one |
| `request generate-plugins [NS[.NAME]...] [--from-plugin-cache]` | Ask the next `sync` to extract plugins again from the artifacts, for every collection when there are no targets. `--from-plugin-cache` only extracts what's missing or out of date, as after an alai-index upgrade |
| `request generate-indexes [NS[.NAME]...]` | Ask the next `sync` to make index lines again from the artifacts, for every collection when there are no targets, as after a fix to how alai-index reads artifacts |

| Option | Commands | |
|---|---|---|
| `--store DIR` | all | Write to a local directory, one subdirectory per bucket (default `./out`) |
| `--r2` | all | Write to the R2 buckets, configured from the environment ([storage](docs/storage.md)) |
| `-v` | all | Debug logging |
| `--galaxy-url URL` | `sync` | Galaxy server (default `https://galaxy.ansible.com`) |
| `--strict` | `sync` | Exit 1 while any failure or conflict is outstanding |
| `--report FILE` | `sync` | Write the outstanding failures and open conflicts as a Markdown block for the tracking issue |
| `--alerts DIR` | `sync` | Write a Markdown comment for the failures that became persistent this run into `DIR` |

A run never stops on a failure: it records it in the failure ledger
(in `state.json` in the state bucket) and carries on, and exits 0 unless
`--strict` is given.

## Scheduled and manual runs

The workflows are described in [scheduling](docs/sync.md#scheduling).

| Trigger | Workflow | Why |
|---|---|---|
| Every 10 minutes, dispatched by the scheduler in `scheduler/` | `sync.yml` | Picks up new versions, deletions and deprecations; sweeps once a day for what that can't see quickly; works through the initial import 45 minutes at a time |
| Manual | `request.yml` with `mirror` | Resyncs specific collections on the next run, ahead of any backlog: a release needed urgently, a collection that looks out of date, or a version that keeps failing |
| Manual | `request.yml` with `sweep` | Sweeps on the next run, e.g. after `deletions_suspected`, instead of waiting for the daily one |
| After an alai-index upgrade that changed plugin extraction | `request.yml` with `generate-plugins` and `from-plugin-cache` | Extracts every out of date version's plugins again over the next syncs, rather than as each collection is next synced |
| After an alai-index fix to how artifacts are read | `request.yml` with `generate-indexes` | Makes the index lines again from the artifacts over the next syncs |

The scheduled run keeps the outstanding failures and open conflicts at the
top of a tracking issue's description, and posts failures outstanding for
over a day as a comment there. A
heartbeat ping from every successful run catches the runs stopping.

## Documentation

- [How a sync works](docs/sync.md): reconcile, the queue, change detection,
  failures, notifications, scheduling, yanks and takedowns.
- [Galaxy API](docs/galaxy-api.md): the endpoints used, how Galaxy behaves,
  and measurements.
- [Storage](docs/storage.md): the buckets, their layout, state and
  credentials.
- [Index format](docs/index-spec/README.md) and [plugin index](docs/index-spec/plugins.md):
  what clients read.

Development notes are in [AGENTS.md](AGENTS.md).
