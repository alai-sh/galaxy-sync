# galaxy-sync

Mirrors galaxy.ansible.com into the alai static index. What it does and how
it's run is in [README.md](README.md); how a sync works, the Galaxy API and
storage are in [`docs/`](docs/). Python 3.15 (standard GIL build, pinned in
`.python-version`).

Plugin extraction is done by
[alai-plugin-extractor](https://github.com/alai-sh/alai-plugin-extractor), a
separate package whose AGENTS.md has the extraction rules and edge cases.
It isn't on PyPI yet, so `pyproject.toml` pins it as a git source at a
commit. To pick up extractor changes, bump `rev` and run `uv lock`. To
develop against a local checkout without changing the pin, use
`uv pip install -e ../alai-plugin-extractor` (the next `uv sync` or `uv run`
puts the pinned version back).

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
| `cli.py` | The `sync` and `refresh` commands. The only module allowed to `print`; the others log decisions at INFO and each request or step at DEBUG (`-v`) |
| `reconcile.py` | `reconcile()`, the single per-collection code path, and the only code that adds, yanks or flags a version |
| `changes.py` | Finding which collections to reconcile: `discover()` every run, the daily `sweep()`, `check_count()`, `resolve_targets()` |
| `galaxy.py` | The Galaxy HTTP client, with retries. Contract endpoints unless a method's docstring says best effort |
| `artifacts.py` | Reading tarballs held in memory: `MANIFEST.json` and the contents digest |
| `plugins.py` | What's stored from an extraction, and how it becomes plugin index lines |
| `builtin.py` | Finding ansible-core's latest minor release on PyPI and extracting `ansible.builtin` from its sdist |
| `store.py` | The mirror's files and state on top of the backends; the queue, `refresh` requests, the failure ledger and conflicts |
| `report.py` | Rendering `failures.json`, the tracking issue's report block and alert comments, and the public conflict records |
| `storage.py` | The `Backend` interface (one per bucket), `LocalBackend` and `R2Backend` |
| `models.py` | TypedDicts for Galaxy responses, index and plugin lines, and state |
| `semver.py`, `times.py` | SemVer 2.0.0 precedence; RFC 3339 timestamps |

`docs/index-spec/` specifies the index format; `out/` is local run output,
gitignored.

## Rules the code depends on

- **Never import or execute collection code.** Collection contents are read
  only by alai-plugin-extractor and `artifacts.py`, which reads member bytes
  from a tarball in memory and never extracts to disk.
- **A version's `cksum` never changes.** When Galaxy serves different bytes
  for a mirrored version, reconcile compares contents: a repack is kept
  silently, a content change keeps ours and opens a conflict. Never an
  overwrite.
- **Only reconcile changes the mirror, and only from contract endpoints.**
  Change detection only picks collections. Only a 404 from
  `GET /api/v3/collections/<ns>/<name>/`, or for one version from
  `.../versions/<ver>/`, counts as a deletion, so a flaky, truncated or
  shifting feed or listing can't yank anything.
- **Request the CLI-shaped `/api/v3/collections/...` paths**, not the
  `plugin/ansible/content/published/...` paths they redirect to.
- **Runs never overlap, and work lives in the queue.** Every run reads
  `state.json` and writes it back whole. Anything found is queued in state
  before the cursor moves past it, so a run that stops early loses nothing.
  `refresh` only writes a request object, never state; a rebuild is a
  request too, and keeps its progress in state like the queue.
- **Failures never stop a run.** Record them in the ledger (per version where
  possible) and carry on. Extraction failures still mirror the artifact and
  index line. Recorded failures exit `0`; `--strict` exits `1`.
- **Write order:** artifacts, then index files, then aggregate files, with
  `collections.json` last, as it names the index files. The index must never
  point at an artifact that isn't written yet. Nothing is purged from the
  CDN (docs/storage.md, "Caching").
- **Index files are published by sha256, never overwritten.** Each change
  publishes `index/<aa>/<sha256>.jsonl` before state's `sha256`, the only
  record of which file is a collection's, points at it, so
  `collections.json` never names an unpublished file. Published files are
  deleted only by pruning, 7 days after no listing names them
  (docs/storage.md, "Index files").
- **Bump `plugins.EXTRACTION_FORMAT`** when a change here, or an
  alai-plugin-extractor upgrade whose release notes say its output changed,
  would extract the same artifact differently. Reconcile re-extracts every
  stored result with an older format.
- **`ansible.builtin`'s `since` is its documented `version_added`**, not
  derived from comparing releases (`plugins.builtin_lines`). ansible-core
  documents it on nearly every plugin (243 of 261 in 2.23), so it's reliable
  there; collections don't use it consistently, so they keep the version
  comparison in `plugin_lines`.
