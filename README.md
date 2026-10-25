# galaxy-sync

Mirrors every collection on galaxy.ansible.com into the alai static index and
artifact store, so the alai client (and, meanwhile, `ansible-galaxy`) can
install collections without depending on Galaxy.

## Goals

- **Mirror everything Galaxy publishes**, pre-releases and dev builds
  included. The client decides what to resolve.
- **Keep Galaxy out of the download path.** Its API can be slow and unreliable
  at times, so artifacts are mirrored, not linked.
- **No long-lived server.** Scheduled batch jobs write static files that any
  static host can serve.
- **Be polite to Galaxy:** one request at a time, backoff, a descriptive
  User-Agent, and as few requests as possible when nothing has changed.
- **Support existing tooling** with a static copy of the Galaxy endpoints
  `ansible-galaxy collection install` reads, until the alai client exists.

## Usage

```bash
uv run galaxy-sync COMMAND [OPTIONS] ...
```

| Command | Purpose |
|---|---|
| `sync [--budget MINUTES] [--dry-run]` | Find what changed on Galaxy and queue it, reconcile `refresh` requests first, sweep once a day, then reconcile the rest of the queue until the budget (default 45 minutes) runs out. The rest stays queued for the next run, so the initial import is built up over many runs. `--dry-run` only reports what changed |
| `refresh [NS[.NAME]...] [--revalidate]` | Ask the next `sync` to reconcile these namespaces or collections ahead of everything else, or with no targets to sweep: compare every published version with the mirror. `--revalidate` also checks every mirrored version's artifact against the one Galaxy publishes now. Writes a request and returns; it doesn't contact Galaxy |
| `refresh --ack NS.NAME:VERSION... --note TEXT` | Ask the next `sync` to acknowledge these versions' conflicts once a person has inspected them, so they're no longer reported. The note is published with the conflict ([conflicts](docs/sync.md#conflicts)) |
| `rebuild [--dl TEMPLATE] [--no-galaxy-view]` | Regenerate the files derived from the index without contacting Galaxy. `--dl` sets the artifact URL template used by `config.json` and the Galaxy view (default: `dl.alai.sh`); later runs keep it, and a plain `rebuild` resets it. `--no-galaxy-view` leaves the Galaxy view and the artifact URL template alone, which skips most of a rebuild's time (it can't be combined with `--dl`) |
| `builtin SRC` | Re-extract `ansible.builtin`'s plugins from an ansible-core checkout or sdist and replace its lines in `plugins.jsonl`. Every later `sync` keeps them, so it can run alongside one |

| Option | Commands | |
|---|---|---|
| `--store DIR` | all | Write to a local directory, one subdirectory per bucket (default `./out`) |
| `--r2` | all | Write to the R2 buckets, configured from the environment ([storage](docs/storage.md)) |
| `-v` | all | Debug logging |
| `--galaxy-url URL` | `sync` | Galaxy server (default `https://galaxy.ansible.com`) |
| `--strict` | `sync` | Exit 1 while any failure or unacknowledged conflict is outstanding |
| `--report FILE` | `sync` | Write the outstanding failures and unacknowledged conflicts as a Markdown block for the tracking issue |
| `--alerts DIR` | `sync` | Write a Markdown comment per conflict opened this run, and one for the failures that became persistent, into `DIR` |
| `--acknowledged FILE` | `sync` | Write the conflicts acknowledged this run, so the workflow can mark their comments resolved |

A run never stops on a failure: it records it in the failure report
(`failures.json` in the state bucket) and carries on, and exits 0 unless
`--strict` is given.

## Scheduled and manual runs

The workflows are described in [scheduling](docs/sync.md#scheduling).

| Trigger | Workflow | Why |
|---|---|---|
| Every 10 minutes, dispatched by the scheduler in `scheduler/` | `sync.yml` | Picks up new versions, deletions and deprecations; sweeps once a day for what that can't see quickly; works through the initial import 45 minutes at a time |
| Manual | `refresh.yml` with targets | Resyncs specific collections on the next run, ahead of any backlog: a release needed urgently, a collection that looks out of date, or a version that keeps failing |
| Manual | `refresh.yml` without targets | Sweeps on the next run, e.g. after `deletions_suspected`, instead of waiting for the daily one |
| Manual, after inspecting a conflict | `refresh.yml` with `acknowledge` and `note` | Stops reporting the conflict, records the note publicly, and marks its alert comment resolved |
| After moving the artifacts or changing the Galaxy view format | `rebuild.yml` with `dl` | Rewrites every derived file |
| After an ansible-core minor release | `builtin.yml` with its release tag, e.g. `v2.21.0` | Updates `ansible.builtin` in the plugin index |

The scheduled run keeps the outstanding failures and unacknowledged
conflicts at the top of a tracking issue's description, and posts each new
conflict, and failures outstanding for over a day, as comments there. A
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
- [Galaxy v3 view](docs/galaxy-v3-view.md): the static copy of Galaxy's API
  for `ansible-galaxy`.

Development notes are in [AGENTS.md](AGENTS.md).
