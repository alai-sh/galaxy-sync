"""The alai-index tree a run works on.

alai-index builds and changes the index as files on disk, and never reads or
writes a bucket. A run fetches what each alai-index command reads into a
local tree, runs it, and publishes the files its JSON output says it wrote:

    index bucket   config.json, collections.json, plugins.jsonl, index/
    dl bucket      the artifacts, at the dl template's URL path
    state bucket   plugin-cache/<ns>/<name>.jsonl.zst, alai-index's plugin cache

The public files are the index's record. The plugin cache can always be made
again from the artifacts. Its file for each collection is at a path
alai-index documents, but what's in it is alai-index's own, so it's only ever
copied, never read here. See docs/storage.md.
"""

import concurrent.futures
import json
import logging
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
from collections.abc import Iterable, Mapping
from compression import zstd
from pathlib import Path
from typing import cast

from alai_core.index import CollectionIndex, Collections, CollectionVersion, Config

from .models import IndexFiles, IndexOutput
from .storage import Buckets, StorageError

log = logging.getLogger(__name__)

# Where clients find a published index file, from collections.json's sha256.
INDEX_TEMPLATE = "index/{sha256-prefix}/{sha256-checksum}.jsonl"

# The artifact URL template a new index starts with.
DEFAULT_DL = "https://dl.alai.sh/sha256/{sha256-prefix}/{sha256-checksum}.tar.gz"

# Where alai-index's plugin cache is kept in the state bucket.
PLUGIN_CACHE = "plugin-cache/"

# Zstandard level for the plugin cache. A collection's versions repeat each
# other's plugins and routing, which Zstandard's long window finds: on
# community.general's 243 versions, 41 MB of JSON becomes 0.27 MB in 0.04 s.
ZSTD_LEVEL = 10

# Objects fetched at once when a step needs many collections' files. Keep it below
# storage.MAX_CONNECTIONS, the R2 client's connection pool.
FETCHERS = 16

# The files alai-index rewrites on every change, published when the run says.
AGGREGATES = ("plugins.jsonl", "collections.json")

ALAI_INDEX = [sys.executable, "-m", "alai_index"]


class AlaiIndexError(Exception):
    """alai-index refused a command, which leaves the tree as it was."""


def _no_files() -> IndexFiles:
    return {"dl": [], "index": [], "plugin-cache": []}


def _parse(fqcn: str, data: bytes) -> tuple[CollectionVersion, ...]:
    """A collection's index lines, all of them: one that can't be read is never left out, which would make the
    version look unmirrored or deleted."""
    index = CollectionIndex.parse(data, fqcn)
    if index.invalid or index.newer:
        problems = [f"line {n}: {why}" for n, why in index.invalid]
        problems += [f"{index.newer} lines in a newer format"] if index.newer else []
        raise StorageError(f"{fqcn}'s index file can't be read: {'; '.join(problems)}")
    return index.lines


class Tree:
    def __init__(self, buckets: Buckets, workdir: Path, dl_url: str = DEFAULT_DL) -> None:
        """Fetch the files every run reads into `workdir`, or start a new index there if the index bucket has none.

        `dl_url` is the artifact URL template a new index starts with.
        """
        self.buckets = buckets
        self.root = workdir / "tree"
        self.cache = workdir / "plugin-cache"
        self.dl = workdir / "dl"
        self.session = workdir / "session.json"
        self.checked = workdir / "checked.json"
        # Each collection the index has, with its current index file's sha256.
        self.listing: dict[str, str] = {}
        self._fetched: set[str] = set()
        self._plugins_fetched = False
        # Aggregate files alai-index wrote that aren't published yet.
        self._unpublished: set[str] = set()

        config = buckets.index.read("config.json")
        if config is None:
            log.info("the index bucket has no config.json, so starting a new index")
            _ = self.run("config", "--index", INDEX_TEMPLATE, "--dl-template", dl_url)
            # All at once, so a run that stops leaves an index the next one can read.
            self._publish_files("index", ["config.json", *AGGREGATES])
            self._plugins_fetched = True
        else:
            self._save(self.root / "config.json", config)
            listing = buckets.index.read("collections.json")
            if listing is None:
                # Never started afresh, which would mirror every collection again.
                raise StorageError(
                    "the index bucket has config.json but no collections.json, its record of collections"
                )
            self._save(self.root / "collections.json", listing)
        self.config = Config.parse((self.root / "config.json").read_bytes())
        self._read_listing()

    def _read_listing(self) -> None:
        listing = Collections.parse((self.root / "collections.json").read_bytes())
        self.listing = {}
        for fqcn in listing:
            if (entry := listing.get(fqcn)) is None or entry.sha256 is None:
                # Never left out, which would mirror the whole collection again.
                raise StorageError(f"collections.json has no index file for {fqcn}")
            self.listing[fqcn] = entry.sha256

    @staticmethod
    def _save(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        _ = path.write_bytes(data)

    @staticmethod
    def cache_key(fqcn: str) -> str:
        """A collection's plugin cache file in the state bucket, at the path alai-index gives it."""
        ns, _, name = fqcn.partition(".")
        return f"{PLUGIN_CACHE}{ns}/{name}.jsonl.zst"

    def index_file(self, fqcn: str) -> str:
        """A collection's current index file, as a key in the index bucket."""
        return self.config.index_path(name=fqcn, sha256=self.listing[fqcn])

    def artifact_key(self, line: CollectionVersion) -> str:
        """A version's artifact's key in the dl bucket, the path of its URL."""
        url = self.config.artifact_path(name=line.name, version=line.version, sha256=line.sha256)
        return urllib.parse.unquote(urllib.parse.urlsplit(url).path.lstrip("/"))

    def read_artifact(self, line: CollectionVersion) -> bytes | None:
        return self.buckets.dl.read(self.artifact_key(line))

    def fetch(self, fqcns: Iterable[str]) -> None:
        """Fetch what alai-index reads of these collections, those the index has and this run hasn't fetched yet: each
        one's current index file and its plugin cache file."""
        todo = [f for f in dict.fromkeys(fqcns) if f in self.listing and f not in self._fetched]
        if not todo:
            return
        jobs: list[tuple[str, str, Path]] = []
        for fqcn in todo:
            ns, _, name = fqcn.partition(".")
            jobs.append(("state", self.cache_key(fqcn), self.cache / ns / f"{name}.jsonl"))
            jobs.append(("index", self.index_file(fqcn), self.root / self.index_file(fqcn)))
        if len(todo) > 1:
            log.info("fetching the files of %d collections", len(todo))

        def get(job: tuple[str, str, Path]) -> None:
            bucket, key, path = job
            data = getattr(self.buckets, bucket).read(key)
            if data is not None:
                self._save(path, zstd.decompress(data) if bucket == "state" else data)
            elif bucket == "index":
                # Never taken as empty, which would mirror the whole collection again.
                raise StorageError(f"{key}, which collections.json names, is missing from the index bucket")
            # A plugin cache file that's missing is made again by alai-index.

        with concurrent.futures.ThreadPoolExecutor(FETCHERS) as pool:
            for _ in pool.map(get, jobs):
                pass
        self._fetched.update(todo)

    def fetch_artifacts(self, outdated: Iterable[Mapping[str, str]]) -> list[Mapping[str, str]]:
        """Fetch the artifacts alai-index listed as `outdated`, so it can extract their plugins again. Returns the
        entries whose artifact is missing from the dl bucket."""
        missing: list[Mapping[str, str]] = []

        def get(entry: Mapping[str, str]) -> None:
            data = self.buckets.dl.read(entry["dl"]) if "dl" in entry else None
            if data is None:
                missing.append(entry)
            else:
                self._save(self.dl / entry["dl"], data)

        with concurrent.futures.ThreadPoolExecutor(FETCHERS) as pool:
            for _ in pool.map(get, outdated):
                pass
        return missing

    def fetch_plugins(self, *, optional: bool = False) -> None:
        """Fetch plugins.jsonl, which every change rewrites. With `optional`, for a job that works out every
        collection's lines again, one that's missing is started empty."""
        if not self._plugins_fetched:
            data = self.buckets.index.read("plugins.jsonl")
            if data is None and not optional:
                raise StorageError("the index bucket has no plugins.jsonl, which config.json says the index has")
            # One that's lost is started empty, and every collection's lines are merged into it again.
            self._save(self.root / "plugins.jsonl", data or b"")
            self._plugins_fetched = True

    def plugin_lines(self) -> int | None:
        """How many lines plugins.jsonl has, if this run has it."""
        path = self.root / "plugins.jsonl"
        if not self._plugins_fetched or not path.exists():
            return None
        return sum(1 for line in path.read_bytes().splitlines() if line.strip())

    def lines(self, fqcn: str) -> tuple[CollectionVersion, ...]:
        """A collection's index lines, fetching its files if they aren't yet, or none if the index doesn't have it."""
        if fqcn not in self.listing:
            return ()
        self.fetch([fqcn])
        return _parse(fqcn, (self.root / self.index_file(fqcn)).read_bytes())

    def published_lines(self) -> dict[str, tuple[CollectionVersion, ...]]:
        """Every collection's index lines, read from the index bucket rather than fetched into the tree."""

        def get(fqcn: str) -> tuple[CollectionVersion, ...]:
            if (data := self.buckets.index.read(key := self.index_file(fqcn))) is None:
                # Never taken as empty, which would make every version look deleted.
                raise StorageError(f"{fqcn}'s index file {key} is missing")
            return _parse(fqcn, data)

        with concurrent.futures.ThreadPoolExecutor(FETCHERS) as pool:
            return dict(zip(self.listing, pool.map(get, self.listing), strict=True))

    def write_checked(self, checked: Mapping[str, Mapping[str, str]]) -> Path:
        """Write galaxy-mirror's --checked file."""
        self._save(self.checked, json.dumps(checked).encode())
        return self.checked

    def run(self, command: str, *args: str) -> IndexOutput:
        """Run an alai-index command on the tree and return its JSON output, empty for one with none.

        Its progress and warnings are logged as it writes them, so a long
        command can be followed. Raises AlaiIndexError if it refused.
        """
        if command != "config":
            self.fetch_plugins()
        paths = ["--root", str(self.root), "--plugin-cache", str(self.cache), "--dl", str(self.dl)]
        argv = [*ALAI_INDEX, command, *paths, *args]
        if command != "config":
            argv += ["--output-format", "json"]
        log.debug("running alai-index %s", " ".join(argv[len(ALAI_INDEX) :]))
        errors: list[str] = []
        lines: list[str] = []
        # stdout goes to a file, as a long JSON output would fill a pipe nobody reads while stderr is followed.
        with tempfile.TemporaryFile() as stdout:
            # Only ever alai-index, with paths and names this module made.
            proc = subprocess.Popen(argv, stdout=stdout, stderr=subprocess.PIPE, text=True)  # noqa: S603
            for raw in cast(Iterable[str], proc.stderr):
                line = raw.rstrip("\n")
                lines.append(line)
                if line.startswith("error: "):
                    errors.append(line.removeprefix("error: "))
                elif line.startswith("debug: "):
                    log.debug("alai-index: %s", line.removeprefix("debug: "))
                elif line.strip():
                    log.info("alai-index: %s", line)
            returncode = proc.wait()
            _ = stdout.seek(0)
            output = stdout.read().decode()
        if returncode != 0:
            raise AlaiIndexError(f"alai-index {command} failed: {' '.join(errors) or ' '.join(lines)}")
        if not output.strip():
            return {
                "versions": [],
                "written": _no_files(),
                "deleted": _no_files(),
                "stale": [],
                "outdated": [],
            }
        return cast(IndexOutput, json.loads(output))

    def publish(self, out: IndexOutput) -> None:
        """Publish what an alai-index command wrote, as its JSON output lists it, but plugins.jsonl and
        collections.json, which `publish_aggregates` does.

        Artifacts go first, then the plugin cache, then index files in the
        order alai-index gives, so nothing published names a file that isn't
        there yet. An artifact is deleted from the tree once it's published, so
        a long run doesn't fill the disk.
        """
        written, deleted = out["written"], out["deleted"]
        self._publish_files("dl", written["dl"])
        for rel in written["dl"]:
            (self.dl / rel).unlink(missing_ok=True)
        self._publish_files("plugin-cache", written["plugin-cache"])
        self._publish_files("index", [rel for rel in written["index"] if rel not in AGGREGATES])
        self._unpublished.update(rel for rel in written["index"] if rel in AGGREGATES)
        for rel in deleted["plugin-cache"]:
            self.buckets.state.delete(f"{PLUGIN_CACHE}{rel}.zst")
        for rel in deleted["index"]:
            self.buckets.index.delete(rel)
        for rel in deleted["dl"]:
            self.buckets.dl.delete(rel)
        self._read_listing()

    def publish_aggregates(self) -> bool:
        """Publish plugins.jsonl, then collections.json, if alai-index changed them since they were last published.

        Every file they name is published already. Returns whether anything was published.
        """
        todo = [rel for rel in AGGREGATES if rel in self._unpublished]
        self._publish_files("index", todo)
        self._unpublished.clear()
        return bool(todo)

    def drop_artifacts(self) -> None:
        """Delete the artifacts fetched for extracting plugins again, once that's done."""
        shutil.rmtree(self.dl, ignore_errors=True)

    def _publish_files(self, group: str, paths: list[str]) -> None:
        for rel in paths:
            if group == "dl":
                # Content-addressed: the same key is the same bytes.
                if not self.buckets.dl.exists(rel):
                    self.buckets.dl.write(rel, (self.dl / rel).read_bytes())
            elif group == "plugin-cache":
                data = zstd.compress((self.cache / rel).read_bytes(), ZSTD_LEVEL)
                self.buckets.state.write(f"{PLUGIN_CACHE}{rel}.zst", data)
            else:
                self.buckets.index.write(rel, (self.root / rel).read_bytes())
