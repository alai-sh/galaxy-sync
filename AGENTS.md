# galaxy-sync

Mirrors galaxy.ansible.com into the alai static index. What it does and how
it's run is in [README.md](README.md); how a sync works, the Galaxy API and
storage are in [`docs/`](docs/). Python 3.15 (standard GIL build, pinned in
`.python-version`).

The index itself, index lines, plugin extraction, `plugins.jsonl` and
`collections.json`, is built by
[alai-index](https://github.com/alai-sh/alai-index). galaxy-sync runs its
command line and never imports it, as its modules are private. Everything
it needs comes from alai-index's JSON output. Index files and versions are
read with [alai-core](https://github.com/alai-sh/alai-core), the format
library alai-index uses, never parsed by hand; its version is the one
alai-index needs. alai-index isn't on PyPI yet, so
`pyproject.toml` pins it as a git source at a commit. To pick up alai-index
changes, bump `rev` and run `uv lock`. To develop against a local checkout
without changing the pin, use `uv pip install -e ../alai-index` (the next
`uv sync` or `uv run` puts the pinned version back).

## Commands

```bash
uv run pytest                     # tests
uv run ruff check .               # lint
uv run ruff format . --check      # formatting
uv run basedpyright --warnings    # type check (warnings are failures)
uv run tox run -e sanity          # the three checks above, as CI would run them
uv run tox                        # sanity plus tests with coverage
```

All checks must pass before work is done. Tooling config follows
`~/dev/msauthlib`. CI (`.github/workflows/ci.yml`) runs the same tox
environments, and builds the scheduler Worker in `scheduler/` with
`npx wrangler@<version in ci.yml> deploy --dry-run` (no credentials needed;
run it from `scheduler/` after changing the Worker).

Tests never reach the network: `tests/conftest.py` runs `MockGalaxy`, a local
HTTP server answering like Galaxy (including the upload feed and listing),
and stores into `tmp_path`. Shared builders live in
`tests/helpers.py`. The R2 backend is tested against botocore's `Stubber`,
never a real bucket.

## Layout

| Module | |
|---|---|
| `cli.py` | The `sync` and `request` commands. The only module allowed to `print`; the others log decisions at INFO and each request or step at DEBUG (`-v`) |
| `reconcile.py` | `reconcile()`, the single per-collection code path. It runs alai-index's `galaxy-mirror`, records what it did, and is the only code that opens a conflict |
| `tree.py` | The alai-index tree a run works on: fetching it from the buckets, running alai-index, and publishing what it wrote |
| `changes.py` | Finding which collections to reconcile: `discover()` every run, the daily `sweep()`, `check_count()`, `resolve_targets()` |
| `galaxy.py` | The Galaxy HTTP client, with retries. Contract endpoints unless a method's docstring says best effort |
| `artifacts.py` | Comparing tarballs held in memory, to describe how a conflict's files differ from ours |
| `store.py` | The sync's own state on top of the backends: the queue, requests, the failure ledger, conflicts and pruning |
| `report.py` | Rendering the tracking issue's report block and alert comment |
| `storage.py` | The `Backend` interface (one per bucket), `LocalBackend` and `R2Backend` |
| `models.py` | TypedDicts for Galaxy responses, alai-index's JSON output, and state |
| `semver.py`, `times.py` | Sorting versions with alai-core's SemVer precedence; RFC 3339 timestamps |

`docs/index-spec/` specifies the index format; `out/` is local run output,
gitignored.

## Rules the code depends on

- **Never import or execute collection code.** Collection contents are read
  only by alai-index and `artifacts.py`, which reads member bytes from a
  tarball in memory and never extracts to disk.
- **A version's `sha256` never changes.** When Galaxy serves different bytes
  for a mirrored version, ours is kept and a conflict is opened, even when
  only the packing changed. Never an overwrite.
- **Only reconcile changes the mirror, through alai-index, and only from contract endpoints.**
  Change detection only picks collections. Only a 404 from
  `GET /api/v3/collections/<ns>/<name>/`, or for one version from
  `.../versions/<ver>/`, counts as a deletion, so a flaky, truncated or
  shifting feed or listing can't yank anything. The one write outside
  alai-index is reconcile putting back an artifact missing from the dl
  bucket, and only Galaxy's bytes whose sha256 is the index line's.
- **Request the CLI-shaped `/api/v3/collections/...` paths**, not the
  `plugin/ansible/content/published/...` paths they redirect to.
- **Runs never overlap, and work lives in the queue.** Every run reads
  `state.json` and writes it back whole. Anything found is queued in state
  before the cursor moves past it, so a run that stops early loses nothing.
  `request` only writes a request object, never state, and a job keeps its
  progress in state like the queue.
- **Failures never stop a run.** Record them in the ledger (per version where
  possible) and carry on. Extraction failures still mirror the artifact and
  index line. Recorded failures exit `0`; `--strict` exits `1`.
- **Write order:** what alai-index wrote is published straight after each
  command. Artifacts go first, then the plugin cache, then index files in the
  order alai-index gives. `plugins.jsonl` and `collections.json` are
  published at the end of the run and before each save of `state.json`
  (`Store.before_save`), so they never name a file that isn't published yet,
  and state never says a collection is done while `collections.json` names
  its old index file. Nothing is purged from the CDN (docs/storage.md,
  "Caching").
- **The plugin cache is only ever copied here, never read.** Its path for
  each collection is alai-index's interface, but what's in it is
  alai-index's own (docs/storage.md, "The alai-index tree").
- **Index files are published by sha256, never overwritten.** Published
  files are deleted only by pruning, 7 days after no listing names them
  (docs/storage.md, "Index files").
- **Conflict comparisons are given to galaxy-mirror as `--checked`**, so a
  version is checked again only when Galaxy's upload changes. A comparison is
  dropped only on galaxy-mirror's `unchanged` result, which says the check matched.
- **`ansible.builtin` isn't in `plugins.jsonl`.** alai-cli has it itself.
