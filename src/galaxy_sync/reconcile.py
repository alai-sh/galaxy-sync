"""reconcile(): bring one collection's index file in line with Galaxy.

Every command ends here. It only uses the contract endpoints, and it's the
only code that adds, yanks or flags a version; the change detection in
changes.py just decides which collections to reconcile. See docs/sync.md.
"""

import hashlib
import logging
import re
import tarfile
import time
from dataclasses import dataclass, field

import alai_plugin_extractor as ext

from . import semver
from .artifacts import contents_digest, describe_changes, manifest_info
from .galaxy import GalaxyClient, GalaxyError, NotFound
from .models import Conflict, GalaxyVersion, GalaxyVersionDetail, IndexLine, VersionState
from .plugins import EXTRACTION_FORMAT, extraction_data
from .store import Store
from .times import now, rfc3339

log = logging.getLogger(__name__)

# The index only allows lowercase names, and names become storage keys.
NAME = re.compile(r"[a-z0-9_]+")


def check_name(namespace: str, name: str) -> None:
    if not (NAME.fullmatch(namespace) and NAME.fullmatch(name)):
        raise ValueError(f"invalid collection name {namespace}.{name}")


@dataclass
class Result:
    added: list[str] = field(default_factory=list)
    yanked: list[str] = field(default_factory=list)
    unyanked: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)  # Galaxy serves different contents
    repacked: list[str] = field(default_factory=list)  # different bytes, same contents
    resolved: list[str] = field(default_factory=list)  # Galaxy serves our contents again
    reextracted: list[str] = field(default_factory=list)
    deferred: list[str] = field(default_factory=list)  # left unmirrored because the deadline passed
    errors: list[str] = field(default_factory=list)
    changed: bool = False
    downloaded: int = 0  # bytes of new artifacts
    mirrored: int = 0  # versions in the index afterwards
    upstream: int = 0  # versions Galaxy lists


def _ordered(new_lines: list[IndexLine]) -> list[IndexLine]:
    """New lines in publication order, to append to the index. Galaxy reports
    the same created_at for everything older than its 2023-05 migration, so
    ties are broken by version."""
    return sorted(new_lines, key=lambda line: (line["published"], semver.sort_key(line["vers"])))


class _VersionFailed(Exception):
    """One version can't be processed this run; recorded, and the rest carry on."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


def reconcile(
    client: GalaxyClient,
    store: Store,
    namespace: str,
    name: str,
    *,
    revalidate: bool = False,
    deadline: float | None = None,
) -> Result:
    """Bring one collection's index file in line with Galaxy.

    Problems with individual versions are recorded in the failure ledger and
    skipped; a failed version is simply absent from the index (or keeps its
    previous state), so the next reconcile retries it. `revalidate` compares
    every mirrored artifact's sha256 with the artifact Galaxy publishes now.

    New versions are mirrored oldest first. Once `time.monotonic()` passes
    `deadline`, the rest are left in `Result.deferred` and what's mirrored so
    far is written, so a large collection can be imported over several runs.
    """
    check_name(namespace, name)
    return _Reconcile(client, store, namespace, name, revalidate, deadline).run()


class _Reconcile:
    def __init__(
        self,
        client: GalaxyClient,
        store: Store,
        namespace: str,
        name: str,
        revalidate: bool,
        deadline: float | None,
    ) -> None:
        self.client = client
        self.store = store
        self.namespace = namespace
        self.name = name
        self.revalidate = revalidate
        self.deadline = deadline
        self.fqcn = f"{namespace}.{name}"
        self.res = Result()
        self.cstate = store.collection_state(self.fqcn)
        self.lines = store.read_index(namespace, name)
        # Each mirrored artifact's extraction, by cksum, for the plugin lines.
        self.extractions = store.load_extractions(namespace, name, (line["cksum"] for line in self.lines))
        # New versions to mirror, and how many have been tried, for the progress log.
        self.missing = 0
        self.attempted = 0
        # Versions added since progress was last saved, whose Galaxy pages aren't written yet.
        self.unwritten: list[str] = []
        self.saved_progress = False

    def run(self) -> Result:
        self._heal_extractions()
        meta = self.client.collection(self.namespace, self.name)
        if meta is None:
            return self._deleted()

        upstream = self.client.versions(self.namespace, self.name)
        by_vers = {line["vers"]: line for line in self.lines}
        self.missing = sum(1 for v in upstream if v["version"] not in by_vers)
        if self.missing:
            log.info("%s: %d of %d versions on Galaxy aren't mirrored yet", self.fqcn, self.missing, len(upstream))
        new_lines: list[IndexLine] = []
        # Oldest first, so a collection mirrored over several runs keeps its
        # lines in publication order.
        for v in sorted(upstream, key=lambda v: (rfc3339(v.get("created_at")) or "", semver.sort_key(v["version"]))):
            try:
                if (line := by_vers.get(v["version"])) is None:
                    if self.deadline is not None and time.monotonic() >= self.deadline:
                        self.res.deferred.append(v["version"])
                    else:
                        self.attempted += 1
                        new_lines.append(self._mirror(v))
                        self.store.add_work()
                elif self._needs_recheck(line, v):
                    self._recheck(line, v)
                    self.store.add_work()
                elif line["vers"] not in self.cstate["versions"]:
                    # Mirrored by a run that stopped before saving state.
                    self.cstate["versions"][line["vers"]] = {"created_at": rfc3339(v.get("created_at"))}
            except _VersionFailed as e:
                self.store.record_failure(self.fqcn, v["version"], e.kind, str(e))
                self.res.errors.append(f"{v['version']}: {e.kind}: {e}")
            if self.store.save_due():
                self._save_progress(self.lines + _ordered(new_lines))

        upstream_vers = {v["version"] for v in upstream}
        for line in self.lines:
            if line["vers"] not in upstream_vers and not line["yanked"] and self._deleted_upstream(line["vers"]):
                line["yanked"] = True
                self.res.yanked.append(line["vers"])
        # Versions that failed but are no longer on Galaxy don't need retrying.
        for vers in {e["version"] for e in self.store.state["failures"].values() if e["collection"] == self.fqcn}:
            if vers and vers not in upstream_vers and vers not in by_vers:
                self.store.clear_failures(self.fqcn, vers)

        self.lines.extend(_ordered(new_lines))

        # Galaxy deprecation is collection-level and best effort.
        deprecated = bool(meta.get("deprecated"))
        for line in self.lines:
            if deprecated:
                line["deprecated"] = True
            else:
                _ = line.pop("deprecated", None)

        if self.res.deferred:
            log.info("%s: out of time, %d versions left for the next run", self.fqcn, len(self.res.deferred))
        else:
            # Only once complete, so the listing keeps finding a partial import.
            self.cstate["upstream_updated"] = meta.get("updated_at")
        self.cstate["upstream_count"] = len(upstream_vers)
        self.cstate["upstream_deprecated"] = deprecated
        self.res.mirrored = len(self.lines)
        self.res.upstream = len(upstream_vers)
        if self.lines or not self.res.deferred:  # no empty index file for an import that hasn't started
            # After saving progress, the whole view is rewritten: yanks,
            # deprecation and rechecks can have changed lines already written.
            self.store.save_extractions(self.namespace, self.name, self.extractions)
            changed = self.store.write_index(
                self.namespace, self.name, self.lines, self.unwritten, view=self.saved_progress
            )
            self.res.changed = changed or self.saved_progress
        self._write_plugins(self.lines)
        self.store.clear_collection_failures(self.fqcn)
        return self.res

    def _save_progress(self, lines: list[IndexLine]) -> None:
        """Write what's mirrored so far and save state, so a run that stops loses little of a long collection.

        Only the pages for versions added since the last save are written
        here; the end of the reconcile writes the rest of the view.
        """
        log.info("%s: saving progress, %d of %d new versions done", self.fqcn, self.attempted, self.missing)
        self.store.save_extractions(self.namespace, self.name, self.extractions)
        _ = self.store.write_index(self.namespace, self.name, lines, self.unwritten, partial=True)
        self._write_plugins(lines)
        self.store.save_state()
        self.unwritten = []
        self.saved_progress = True

    def _deleted_upstream(self, vers: str) -> bool:
        """Whether a version the listing left out is really gone: its own page returns 404.

        A delete while a long listing is paged can shift a row past us, so the
        listing alone never yanks anything.
        """
        try:
            _ = self.client.version_detail(self.namespace, self.name, vers)
        except NotFound:
            log.info("%s %s: deleted from Galaxy, yanking", self.fqcn, vers)
            self.store.clear_failures(self.fqcn, vers, exclude=("extract_error",))
            return True
        except GalaxyError as e:
            self.store.record_failure(self.fqcn, vers, e.kind, f"checking whether it was deleted: {e}")
            self.res.errors.append(f"{vers}: {e.kind}: {e}")
            return False
        log.info("%s %s: missing from the listing but still on Galaxy, keeping it", self.fqcn, vers)
        # An earlier check that couldn't reach Galaxy is over; a recheck's
        # failures for this version aren't.
        mine = (
            e["kind"]
            for e in self.store.state["failures"].values()
            if (e["collection"], e["version"]) == (self.fqcn, vers)
        )
        if kinds := tuple({k for k in mine if k == "galaxy_unavailable" or k.startswith("galaxy_http_")}):
            self.store.clear_failures(self.fqcn, vers, kinds=kinds)
        return False

    def _heal_extractions(self) -> None:
        """Re-extract mirrored versions with no stored result, or one in an older format."""
        for line in self.lines:
            stored = self.extractions.get(line["cksum"])
            if (
                (stored is None or stored.get("format", 0) < EXTRACTION_FORMAT)
                and (data := self.store.read_artifact(line["cksum"])) is not None
                and self._extract(line["vers"], line["cksum"], data, "re-extracted")
            ):
                self.res.reextracted.append(line["vers"])

    def _write_plugins(self, lines: list[IndexLine]) -> None:
        if lines:
            self.store.write_collection_plugins(
                self.namespace,
                self.name,
                {line["vers"]: self.extractions[line["cksum"]] for line in lines if line["cksum"] in self.extractions},
            )

    def _deleted(self) -> Result:
        """Only a 404 from the contract endpoint counts as a deletion: yank everything."""
        log.info("%s: Galaxy returns 404 for the collection, yanking every version", self.fqcn)
        for line in self.lines:
            if not line["yanked"]:
                line["yanked"] = True
                self.res.yanked.append(line["vers"])
        self.store.save_extractions(self.namespace, self.name, self.extractions)
        self.res.changed = self.store.write_index(self.namespace, self.name, self.lines) if self.lines else False
        self._write_plugins(self.lines)
        _ = self.cstate.pop("upstream_updated", None)  # the listing check stops looking for it
        self.cstate["upstream_count"] = 0
        self.res.mirrored = len(self.lines)
        self.store.clear_collection_failures(self.fqcn)
        return self.res

    def _mirror(self, v: GalaxyVersion) -> IndexLine:
        """Download, check and store a version the index doesn't have yet."""
        vers = v["version"]
        detail = self._detail(vers)
        sha = detail["artifact"]["sha256"]
        data = self._download(detail, "download")

        try:
            info = manifest_info(data)
        except (tarfile.TarError, ValueError) as e:
            raise _VersionFailed("bad_artifact", f"can't read MANIFEST.json: {type(e).__name__}: {e}") from None
        if (info.get("namespace"), info.get("name"), info.get("version")) != (self.namespace, self.name, vers):
            raise _VersionFailed(
                "manifest_mismatch",
                f"MANIFEST.json says {info.get('namespace')}.{info.get('name')} {info.get('version')}",
            )

        self.store.write_artifact(sha, data)
        self.res.downloaded += len(data)
        # Mirrored: every earlier failure for this version is fixed. Extraction
        # then records its own failure if it breaks, without blocking the line.
        self.store.clear_failures(self.fqcn, vers)
        _ = self._extract(vers, sha, data, f"mirrored {self.attempted}/{self.missing} ({len(data)} bytes)")

        created = rfc3339(v.get("created_at"))
        line: IndexLine = {
            "v": 1,
            "name": self.fqcn,
            "vers": vers,
            "deps": [{"name": d, "req": r} for d, r in (detail["metadata"].get("dependencies") or {}).items()],
            "cksum": sha,
            "size": len(data),
            "yanked": False,
            "origin": "galaxy",
            "published": rfc3339(detail.get("created_at")) or created or now(),
        }
        if req := (v.get("requires_ansible") or detail.get("requires_ansible")):
            line["requires_ansible"] = req
        self.cstate["versions"][vers] = {"created_at": created}
        self.res.added.append(vers)
        self.unwritten.append(vers)
        return line

    def _needs_recheck(self, line: IndexLine, v: GalaxyVersion) -> bool:
        """Re-check a mirrored version that's yanked, revalidated, or whose created_at changed."""
        seen = self.cstate["versions"].get(line["vers"])
        seen_created = seen["created_at"] if seen else None
        created = rfc3339(v.get("created_at"))
        if line["yanked"]:
            reason = "yanked here but listed by Galaxy again"
        elif created and seen_created and created != seen_created:
            reason = f"Galaxy's created_at changed from {seen_created} to {created}"
        elif self.revalidate:
            reason = "revalidating"
        else:
            return False
        log.info("%s %s: %s, rechecking its artifact", self.fqcn, line["vers"], reason)
        return True

    def _recheck(self, line: IndexLine, v: GalaxyVersion) -> None:
        """Compare what Galaxy serves now with our line. Ours is never overwritten."""
        vers = line["vers"]
        detail = self._detail(vers)
        sha = detail["artifact"]["sha256"]
        seen = self.cstate["versions"].get(vers)
        state: VersionState = {"created_at": rfc3339(v.get("created_at"))}
        if sha == line["cksum"]:
            log.info("%s %s: Galaxy's sha256 matches ours", self.fqcn, vers)
            same = True
        elif seen and (verdict := seen.get("upstream")) and verdict["sha256"] == sha:
            same = verdict["same_contents"]  # already compared this artifact
            log.info(
                "%s %s: Galaxy's sha256 %s was compared before: %s",
                self.fqcn,
                vers,
                sha,
                "same contents" if same else "different contents",
            )
            state["upstream"] = verdict
        else:
            log.info(
                "%s %s: Galaxy serves sha256 %s, ours is %s; comparing contents", self.fqcn, vers, sha, line["cksum"]
            )
            same = self._compare(line, detail)
            log.info(
                "%s %s: %s", self.fqcn, vers, "same contents, a repack" if same else "different contents, a conflict"
            )
            state["upstream"] = {"sha256": sha, "same_contents": same}

        # Checked: earlier download, sha256 or conflict failures for this
        # version are over. Extraction failures heal separately.
        self.store.clear_failures(self.fqcn, vers, exclude=("extract_error",))
        if same:
            flagged = line.pop("upstream_conflict", None) is not None
            if self.store.resolve_conflicts(self.fqcn, vers) or flagged:
                self.res.resolved.append(vers)
            elif sha != line["cksum"]:
                self.res.repacked.append(vers)
        else:
            line["upstream_conflict"] = True
            self.res.conflicts.append(vers)
        if line["yanked"]:
            line["yanked"] = False  # back upstream
            self.res.unyanked.append(vers)
        self.cstate["versions"][vers] = state

    def _compare(self, line: IndexLine, detail: GalaxyVersionDetail) -> bool:
        """Whether Galaxy's artifact has the same contents as ours.

        An artifact with different contents (or one that can't be compared)
        opens a conflict, with Galaxy's artifact saved for inspection.
        """
        data = self._download(detail, "download for comparison")
        changes = self._changes(line["cksum"], data)
        if changes is None:
            return True
        sha = detail["artifact"]["sha256"]
        conflict: Conflict = {
            "collection": self.fqcn,
            "version": line["vers"],
            "ours": line["cksum"],
            "upstream": sha,
            "seen": now(),
            "changes": changes,
            "saved": self.store.save_upstream_artifact(sha, data),
        }
        _ = self.store.open_conflict(conflict)
        return False

    def _changes(self, cksum: str, theirs: bytes) -> str | None:
        """How Galaxy's artifact differs from ours, or None if the contents are the same."""
        ours = self.store.read_artifact(cksum)
        if ours is None:
            return f"the mirror's artifact {cksum} is missing"
        try:
            theirs_digest = contents_digest(theirs)
        except (tarfile.TarError, ValueError) as e:
            return f"Galaxy's artifact can't be read: {type(e).__name__}: {e}"
        try:
            ours_digest = contents_digest(ours)
        except (tarfile.TarError, ValueError) as e:
            return f"the mirror's artifact can't be read: {type(e).__name__}: {e}"
        return None if ours_digest == theirs_digest else describe_changes(ours_digest, theirs_digest)

    def _detail(self, vers: str) -> GalaxyVersionDetail:
        try:
            return self.client.version_detail(self.namespace, self.name, vers)
        except GalaxyError as e:
            raise _VersionFailed(e.kind, str(e)) from None

    def _download(self, detail: GalaxyVersionDetail, what: str) -> bytes:
        """Download an artifact and check it against Galaxy's sha256."""
        try:
            data = self.client.download(detail["download_url"])
        except GalaxyError as e:
            raise _VersionFailed(e.kind, f"{what}: {e}") from None
        sha = detail["artifact"]["sha256"]
        if (actual := hashlib.sha256(data).hexdigest()) != sha:
            raise _VersionFailed("sha256_mismatch", f"downloaded {actual}, Galaxy says {sha}")
        return data

    def _extract(self, vers: str, cksum: str, data: bytes, done: str) -> bool:
        """Extract plugin data. Failures are recorded but never block mirroring."""
        try:
            result = ext.extract_bytes(data)
        except Exception as e:  # untrusted input: any failure is data, not a crash
            log.debug("%s %s: %s, extraction failed: %s: %s", self.fqcn, vers, done, type(e).__name__, e)
            self.store.record_failure(self.fqcn, vers, "extract_error", f"{type(e).__name__}: {e}")
            return False
        log.debug("%s %s: %s, %d plugins", self.fqcn, vers, done, len(result.plugins))
        self.extractions[cksum] = extraction_data(result)
        self.store.clear_failures(self.fqcn, vers, kinds=("extract_error",))
        return True
