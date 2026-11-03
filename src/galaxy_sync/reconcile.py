"""reconcile(): bring one collection's index file in line with Galaxy.

Every command ends here. alai-index's galaxy-mirror does the mirroring, from
the contract endpoints: it adds new versions, yanks deleted ones, and reports
a version Galaxy serves other bytes for as a conflict without writing it. This
records what it found in the failure ledger and the collection's state, and
compares a conflict's contents with ours. The change detection in changes.py
only decides which collections to reconcile. See docs/sync.md.
"""

import hashlib
import logging
import re
import tarfile
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from alai_core.index import CollectionVersion

from . import semver
from .artifacts import contents_digest, describe_changes
from .galaxy import GalaxyClient, GalaxyError, NotFound
from .models import IndexChange, IndexOutput, Observation
from .store import KEPT, Store
from .times import now
from .tree import Tree

log = logging.getLogger(__name__)

# The index only allows lowercase names, and names become storage keys.
NAME = re.compile(r"[a-z0-9_]+")

# A version's artifact missing from the dl bucket: not yet restored from Galaxy, or Galaxy can't give it back.
_MISSING = ("artifact_missing", "artifact_lost")

# Versions galaxy-mirror downloads in one call, so a run notices its deadline
# part way through a large collection.
CHUNK = 10

# Artifacts fetched at once to extract plugins or make index lines again, so a
# collection with many large artifacts still fits in the run's temporary space.
BATCH = 50


def check_name(namespace: str, name: str) -> None:
    if not (NAME.fullmatch(namespace) and NAME.fullmatch(name)):
        raise ValueError(f"invalid collection name {namespace}.{name}")


class ReconcileFailed(Exception):
    """The collection couldn't be reconciled at all, e.g. listing its versions failed. Recorded by the caller."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


@dataclass
class Result:
    added: list[str] = field(default_factory=list)
    yanked: list[str] = field(default_factory=list)
    unyanked: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)  # Galaxy serves an artifact for them not seen before
    resolved: list[str] = field(default_factory=list)  # Galaxy serves our artifact again
    reextracted: list[str] = field(default_factory=list)  # plugins extracted again, as their extraction was out of date
    restored: list[str] = field(default_factory=list)  # artifacts missing from the dl bucket, downloaded again
    deferred: list[str] = field(default_factory=list)  # left unmirrored because the deadline passed
    errors: list[str] = field(default_factory=list)
    deprecated: bool | None = None  # the collection's new deprecation flag, if Galaxy changed it
    listed: set[str] = field(default_factory=set)  # the versions Galaxy lists
    downloaded: int = 0  # bytes of new artifacts
    mirrored: int = 0  # versions in the index afterwards
    upstream: int = 0  # versions Galaxy lists


def reconcile(
    client: GalaxyClient,
    store: Store,
    tree: Tree,
    namespace: str,
    name: str,
    *,
    revalidate: bool = False,
    deadline: float | None = None,
    unseen: Sequence[str] = (),
) -> Result:
    """Bring one collection's index file in line with Galaxy.

    Problems with individual versions are recorded in the failure ledger and
    skipped. A failed version is absent from the index, or keeps its previous
    state, so the next reconcile retries it. `revalidate` compares every
    mirrored artifact's sha256 with the artifact Galaxy publishes now.

    New versions are mirrored oldest first, CHUNK at a time. Once
    `time.monotonic()` passes `deadline`, the rest are left in
    `Result.deferred`, so a large collection can be imported over several runs.

    A version whose artifact was recorded missing from the dl bucket gets it
    back from Galaxy, if Galaxy still serves the same bytes.

    `unseen` are mirrored versions the sweep didn't see in the upload feed.
    Those Galaxy still serves are recorded as unindexed, so the version count
    stops expecting them and the next sweep doesn't report them as gone.

    Raises ReconcileFailed, or GalaxyError if the collection's page can't be read.
    """
    check_name(namespace, name)
    return _Reconcile(client, store, tree, namespace, name, revalidate, deadline, unseen).run()


class _Reconcile:
    def __init__(
        self,
        client: GalaxyClient,
        store: Store,
        tree: Tree,
        namespace: str,
        name: str,
        revalidate: bool,
        deadline: float | None,
        unseen: Sequence[str],
    ) -> None:
        self.client = client
        self.store = store
        self.tree = tree
        self.namespace = namespace
        self.name = name
        self.revalidate = revalidate
        self.deadline = deadline
        self.unseen = unseen
        self.fqcn = f"{namespace}.{name}"
        self.res = Result()
        self.cstate = store.collection_state(self.fqcn)
        # Versions galaxy-mirror couldn't mirror or check, which Galaxy may still list.
        self.failed: set[str] = set()
        # Versions with a failure recorded this run.
        self.erred: set[str] = set()

    def run(self) -> Result:
        # Read before galaxy-mirror, so a change after it is seen by the next listing rather than lost.
        meta = self.client.collection(self.namespace, self.name)
        ours = {line.version.text: line for line in self.tree.lines(self.fqcn)}
        checked = self.tree.write_checked({self.fqcn: self.store.checked(self.fqcn)})

        first = True
        while True:
            args = [self.client.base_url, self.fqcn, "--limit", str(CHUNK)]
            args += ["--session", str(self.tree.session), "--checked", str(checked)]
            # Once is enough, and a session is carried on from only without it.
            if self.revalidate and first:
                args.append("--revalidate")
            out = self.tree.run("galaxy-mirror", *args)
            self.tree.publish(out)
            first = False
            deferred = self._results(out["versions"], ours)
            if not deferred:
                break
            if self.deadline is not None and time.monotonic() >= self.deadline:
                self.res.deferred = deferred
                log.info("%s: out of time, %d versions left for the next run", self.fqcn, len(deferred))
                break

        lines = self.tree.lines(self.fqcn)
        self._restore_missing(lines)
        self._extract_again(out["outdated"], ours)
        for line in lines:
            if line.version.text in self.res.added:
                self.res.downloaded += line.size

        if meta is None:
            upstream: set[str] = set()
            _ = self.cstate.pop("upstream_updated", None)  # the listing check stops looking for it
        else:
            # What Galaxy lists: versions in the index it didn't yank, and those it couldn't mirror yet.
            upstream = {line.version.text for line in lines if not line.yanked} | self.failed | set(self.res.deferred)
            if not self.res.deferred:
                # Only once complete, so the listing keeps finding a partial import.
                self.cstate["upstream_updated"] = meta["updated_at"]
            deprecated = bool(meta.get("deprecated"))
            if deprecated != self.cstate.get("upstream_deprecated", False):
                self.res.deprecated = deprecated
            self.cstate["upstream_deprecated"] = deprecated

        indexed = {line.version.text for line in lines}
        for vers in {e["version"] for e in self.store.state["failures"].values() if e["collection"] == self.fqcn}:
            if not vers or vers in self.erred:
                continue
            if vers not in upstream and vers not in indexed:
                # Failed, but no longer on Galaxy, so it doesn't need retrying.
                self.store.clear_failures(self.fqcn, vers)
            elif vers in indexed:
                # Checked without a failure this time.
                self.store.clear_failures(self.fqcn, vers, exclude=KEPT)
        self.cstate["upstream_count"] = len(upstream)
        if unindexed := (set(self.cstate.get("unindexed", [])) | set(self.unseen)) & upstream:
            self.cstate["unindexed"] = sorted(unindexed, key=semver.sort_key)
        else:
            _ = self.cstate.pop("unindexed", None)
        self.cstate["mirrored"] = sum(1 for line in lines if not line.yanked)
        self.res.mirrored = len(lines)
        self.res.upstream = len(upstream)
        self.res.listed = upstream
        self.store.clear_collection_failures(self.fqcn)
        return self.res

    def _results(self, versions: list[IndexChange], ours: dict[str, CollectionVersion]) -> list[str]:
        """Record what one galaxy-mirror call did, returning the versions it left for a later call."""
        deferred: list[str] = []
        for change in versions:
            vers = change["version"]
            details = {k: str(v) for k, v in (change["details"] or {}).items() if isinstance(v, str)}
            match change["result"]:
                case "added" if vers:
                    self.res.added.append(vers)
                    self.store.add_work()
                    # Mirrored: every earlier failure for this version is fixed. An extraction that failed never
                    # blocks mirroring, but is recorded for a person to look at.
                    self.store.clear_failures(self.fqcn, vers)
                    _record_extraction(self.store, self.fqcn, vers, details.get("error"))
                case "changed" if vers:
                    yanked = (change["details"] or {}).get("yanked")
                    if yanked is True:
                        log.info("%s %s: deleted from Galaxy, yanked", self.fqcn, vers)
                        self.res.yanked.append(vers)
                        self.store.clear_failures(self.fqcn, vers, exclude=KEPT)
                    elif yanked is False:
                        self.res.unyanked.append(vers)
                case "conflict" if vers:
                    self._observe(vers, ours[vers], details["sha256"], details["url"], details.get("published"))
                case "unchanged" if vers:
                    # Checked against Galaxy, which serves our artifact again.
                    if self.store.resolve_conflicts(self.fqcn, vers):
                        self.res.resolved.append(vers)
                    self.store.clear_failures(self.fqcn, vers, exclude=KEPT)
                case "extracted" if vers:
                    self.res.reextracted.append(vers)
                    _record_extraction(self.store, self.fqcn, vers, details.get("error"))
                case "failed" if vers:
                    self.failed.add(vers)
                    self._fail(vers, details["kind"], details["error"])
                case "failed":
                    raise ReconcileFailed(details["kind"], details["error"])
                case "deferred" if vers:
                    deferred.append(vers)
                case "deferred":
                    pass  # only once the limit is reached before the collection, which a call for one never does
                case _:
                    log.debug("%s: galaxy-mirror said %s for %s", self.fqcn, change["result"], vers)
        return deferred

    def _fail(self, vers: str, kind: str, message: str) -> None:
        self.store.record_failure(self.fqcn, vers, kind, message)
        self.erred.add(vers)
        self.res.errors.append(f"{vers}: {kind}: {message}")

    def _restore_missing(self, lines: tuple[CollectionVersion, ...]) -> None:
        """Put back each artifact recorded missing from the dl bucket, downloading it from Galaxy again. Only the
        same bytes are written, as a version's sha256 never changes. One Galaxy deleted or serves other bytes for
        becomes an artifact_lost, which isn't retried on its own, but is tried again whenever the collection is."""
        failures = self.store.state["failures"].values()
        missing = {e["version"] for e in failures if e["collection"] == self.fqcn and e["kind"] in _MISSING}
        for line in lines:
            if (vers := line.version.text) not in missing:
                continue
            key = self.tree.artifact_key(line)
            if not self.tree.buckets.dl.exists(key):
                try:
                    detail = self.client.version(self.namespace, self.name, vers)
                    data = self.client.download(detail["download_url"])
                except NotFound:
                    self._lost(vers, f"{key} isn't in the dl bucket, and Galaxy no longer has it")
                    continue
                except GalaxyError as e:
                    self._fail(vers, e.kind, f"downloading its missing artifact again: {e}")
                    continue
                if (actual := hashlib.sha256(data).hexdigest()) != line.sha256:
                    self._lost(
                        vers, f"{key} isn't in the dl bucket, and Galaxy now serves other bytes, sha256 {actual}"
                    )
                    continue
                self.tree.buckets.dl.write(key, data)
                log.info("%s %s: downloaded its missing artifact %s again", self.fqcn, vers, key)
            self.store.clear_failures(self.fqcn, vers, kinds=_MISSING)
            self.res.restored.append(vers)

    def _lost(self, vers: str, message: str) -> None:
        self.store.clear_failures(self.fqcn, vers, kinds=("artifact_missing",))
        self._fail(vers, "artifact_lost", message)

    def _extract_again(self, outdated: list[dict[str, str]], ours: dict[str, CollectionVersion]) -> None:
        """Extract the plugins of versions alai-index listed as out of date, fetching their artifacts from the dl
        bucket BATCH at a time. Done as part of the reconcile, so a collection's plugins catch up as it's synced."""
        mine = [entry for entry in outdated if entry["name"] == self.fqcn]
        if not mine:
            return
        log.info("%s: extracting the plugins of %d versions again", self.fqcn, len(mine))

        def run(targets: str) -> None:
            out = self.tree.run("generate-plugins", targets)
            self.tree.publish(out)
            _ = self._results(out["versions"], ours)

        _in_batches(self.store, self.tree, self.fqcn, mine, run)

    def _observe(self, vers: str, ours: CollectionVersion, theirs: str, url: str, published: str | None) -> None:
        """Add the artifact Galaxy serves for a version, uploaded at `published`, to the version's conflict. Ours is
        never overwritten. Galaxy's artifact is downloaded and saved for inspection once, the first time it's seen.
        """
        if self.store.known_artifact(self.fqcn, vers, theirs, published):
            self.store.clear_failures(self.fqcn, vers, exclude=KEPT)
            return
        log.info("%s %s: Galaxy serves sha256 %s, ours is %s; a conflict", self.fqcn, vers, theirs, ours.sha256)
        try:
            data = self.client.download(url)
        except GalaxyError as e:
            self._fail(vers, e.kind, f"download for the conflict: {e}")
            return
        if (actual := hashlib.sha256(data).hexdigest()) != theirs:
            self._fail(vers, "sha256_mismatch", f"downloaded {actual}, Galaxy says {theirs}")
            return
        observation: Observation = {
            "sha256": theirs,
            "uploaded": published,
            "seen": now(),
            "changes": self._changes(ours, data),
            "saved": self.store.save_upstream_artifact(theirs, data),
        }
        _ = self.store.record_conflict(self.fqcn, vers, ours.sha256, observation)
        self.store.clear_failures(self.fqcn, vers, exclude=KEPT)
        self.res.conflicts.append(vers)

    def _changes(self, line: CollectionVersion, theirs: bytes) -> str:
        """How the files of Galaxy's artifact differ from ours."""
        ours = self.tree.read_artifact(line)
        if ours is None:
            return f"the mirror's artifact {line.sha256} is missing"
        try:
            theirs_digest = contents_digest(theirs)
        except (tarfile.TarError, ValueError) as e:
            return f"Galaxy's artifact can't be read: {type(e).__name__}: {e}"
        try:
            ours_digest = contents_digest(ours)
        except (tarfile.TarError, ValueError) as e:
            return f"the mirror's artifact can't be read: {type(e).__name__}: {e}"
        if ours_digest == theirs_digest:
            return "none, the same files packed again"
        return describe_changes(ours_digest, theirs_digest)


def _record_extraction(store: Store, fqcn: str, vers: str, error: object) -> None:
    """Record a version's extraction failing, or clear an earlier failure once it extracted."""
    if isinstance(error, str) and error:
        store.record_failure(fqcn, vers, "extract_error", error)
    else:
        store.clear_failures(fqcn, vers, kinds=("extract_error",))


def _all_artifacts(tree: Tree, fqcn: str) -> list[dict[str, str]]:
    """Every version of a collection, with its artifact's key, as alai-index lists an outdated one."""
    return [{"name": fqcn, "version": line.version.text, "dl": tree.artifact_key(line)} for line in tree.lines(fqcn)]


def _fetch(store: Store, tree: Tree, fqcn: str, wanted: list[dict[str, str]]) -> list[str]:
    """Fetch artifacts from the dl bucket, recording each that's missing. Returns the versions fetched."""
    missing = tree.fetch_artifacts(wanted)
    for entry in missing:
        store.record_failure(fqcn, entry["version"], "artifact_missing", f"{entry.get('dl')} isn't in the dl bucket")
    return [entry["version"] for entry in wanted if entry not in missing]


def _targets(tree: Tree, fqcn: str, versions: list[str], every: int) -> str:
    """The collection, when it's every version, or else an @FILE of each version, as a collection can have thousands."""
    if len(versions) == every:
        return fqcn
    path = tree.root.parent / "targets.txt"
    _ = path.write_text("".join(f"{fqcn}:{vers}\n" for vers in versions))
    return f"@{path}"


def _in_batches(store: Store, tree: Tree, fqcn: str, wanted: list[dict[str, str]], run: Callable[[str], None]) -> None:
    """Fetch the artifacts wanted BATCH at a time, and call `run` with the alai-index targets of each batch fetched.
    A batch's artifacts are deleted before the next is fetched."""
    every = len(tree.lines(fqcn))
    for start in range(0, len(wanted), BATCH):
        try:
            if fetched := _fetch(store, tree, fqcn, wanted[start : start + BATCH]):
                run(_targets(tree, fqcn, fetched, every))
        finally:
            tree.drop_artifacts()


def generate_plugins(store: Store, tree: Tree, fqcn: str, *, from_plugin_cache: bool = False) -> list[str]:
    """Extract a collection's plugins again from its artifacts, fetched from the dl bucket, and publish the result.

    Every version, whatever the plugin cache says, or with `from_plugin_cache`
    only those alai-index lists as out of date. Returns the versions
    extracted. Failures are recorded in the ledger.
    """
    extracted: list[str] = []

    def publish(out: IndexOutput) -> None:
        tree.publish(out)
        for change in out["versions"]:
            if change["result"] != "extracted" or not (vers := change["version"]):
                continue
            extracted.append(vers)
            _record_extraction(store, fqcn, vers, (change["details"] or {}).get("error"))

    if from_plugin_cache:
        tree.fetch([fqcn])
        out = tree.run("generate-plugins", fqcn, "--from-plugin-cache")
        publish(out)
        wanted = [entry for entry in out["outdated"] if entry["name"] == fqcn]
    else:
        wanted = _all_artifacts(tree, fqcn)
    _in_batches(store, tree, fqcn, wanted, lambda targets: publish(tree.run("generate-plugins", targets)))
    return extracted


def generate_indexes(store: Store, tree: Tree, fqcn: str) -> list[str]:
    """Make a collection's index lines again from its artifacts, fetched from the dl bucket, and publish the result.

    Returns the versions whose line changed.
    """
    changed: list[str] = []

    def run(targets: str) -> None:
        out = tree.run("generate-indexes", targets)
        tree.publish(out)
        changed.extend(c["version"] for c in out["versions"] if c["result"] == "changed" and c["version"])

    _in_batches(store, tree, fqcn, _all_artifacts(tree, fqcn), run)
    return changed
