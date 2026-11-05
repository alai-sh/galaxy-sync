# galaxy-sync

Mirrors galaxy.ansible.com into the alai index. Usage is in
[README.md](README.md), behaviour in [`docs/`](docs/). Python 3.15, pinned in
`.python-version`.

The index is built by [alai-index](https://github.com/alai-sh/alai-index),
whose [docs](https://alai.sh/alai-index/) specify the format. galaxy-sync
runs its command line and reads its JSON output, never imports it. Index
files and versions are read with
[alai-core](https://github.com/alai-sh/alai-core), never parsed by hand.
alai-index is pinned as a git source in `pyproject.toml`: bump `rev` and
run `uv lock` to update it, or `uv pip install -e ../alai-index` to try a
local checkout (the next `uv sync` or `uv run` restores the pin).

## Commands

```bash
uv run pytest                     # tests
uv run ruff check .               # lint
uv run ruff format . --check      # formatting
uv run basedpyright --warnings    # type check (warnings are failures)
uv run tox                        # all of the above, as CI runs them
```

All checks must pass before work is done. After changing the Worker in
`scheduler/`, run `npx wrangler@<version in ci.yml> deploy --dry-run` there,
as CI does.

Tests never reach the network: `tests/conftest.py` runs `MockGalaxy`, a local
HTTP server answering like Galaxy, and stores into `tmp_path`. Shared
builders are in `tests/helpers.py`. R2 is tested with botocore's `Stubber`.

## Layout

| Module | |
|---|---|
| `cli.py` | The `sync` and `request` commands. The only module that `print`s; others log decisions at INFO and steps at DEBUG |
| `reconcile.py` | `reconcile()`, the one per-collection code path: runs `galaxy-mirror`, records the result, opens conflicts |
| `tree.py` | The alai-index tree: fetching it from the buckets, running alai-index, publishing what it wrote |
| `changes.py` | Picking collections to reconcile: `discover()`, `sweep()`, `check_count()`, `resolve_targets()` |
| `galaxy.py` | The Galaxy client. Contract endpoints unless a docstring says best effort |
| `artifacts.py` | Comparing tarballs in memory for conflicts |
| `store.py` | State on top of the backends: queue, requests, failures, conflicts, pruning |
| `report.py` | The tracking issue's report block and alert comment |
| `storage.py` | `Backend`, `LocalBackend` and `R2Backend` |
| `models.py` | TypedDicts for Galaxy responses, alai-index output and state |
| `semver.py`, `times.py` | Version sorting; RFC 3339 timestamps |

## Rules the code depends on

- **Never import, execute or extract collection code.** Only alai-index and
  `artifacts.py` (in memory) read artifact contents.
- **A version's `sha256` never changes.** Different bytes from Galaxy open a
  conflict; ours is kept.
- **Only reconcile changes the mirror**, through alai-index and contract
  endpoints. Change detection only picks collections. Only a 404 from the
  collection or version endpoint is a deletion. The one write outside
  alai-index is putting back a missing artifact, only with Galaxy bytes
  matching the index line's sha256.
- **Request the `/api/v3/collections/...` paths**, not the
  `plugin/ansible/content/published/...` paths they redirect to.
- **Runs never overlap, and work lives in the queue.** Each run reads and
  writes `state.json` whole. Queue what's found before moving the cursor.
  `request` only writes a request object, never state.
- **Failures never stop a run.** Record them in the ledger and carry on;
  exit `0` unless `--strict`.
- **Write order:** artifacts, then plugin cache, then index files, after each
  alai-index command. `plugins.jsonl` and `collections.json` only at the end
  of the run and before each save of `state.json` (`Store.before_save`).
- **The plugin cache is copied, never read.** Its contents are alai-index's.
- **Index files are published by sha256, never overwritten**, and deleted
  only by pruning.
- **Conflicts are passed to galaxy-mirror as `--checked`**, and dropped only
  on its `unchanged` result.
- **`ansible.builtin` isn't in `plugins.jsonl`.**
