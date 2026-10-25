"""The mirror's files and state, on top of one storage backend per bucket.

    index bucket   config.json, collections.json, plugins.jsonl, index/, conflicts/
    dl bucket      sha256/<aa>/<sha256>.tar.gz
    galaxy bucket  the static Galaxy v3 view (galaxy_api.py)
    state bucket   state.json, failures.json, builtin.jsonl, extract/, conflicts/, requests/

See docs/storage.md. Writes go through `_write`, which skips objects whose
content hasn't changed, except where the object is known to be new.
"""

import datetime
import hashlib
import json
import logging
import secrets
from collections.abc import Collection, Sequence
from compression import zstd
from dataclasses import dataclass
from typing import cast

from . import galaxy_api, report
from .cdn import PurgeError, Purger
from .models import (
    CollectionState,
    Conflict,
    ExtractionData,
    Failure,
    IndexLine,
    Observation,
    PluginLine,
    Request,
    SyncState,
)
from .plugins import plugin_lines
from .storage import Backend, Buckets, PreconditionFailed, StorageError
from .times import now

log = logging.getLogger(__name__)

# The artifact URL template a new store starts with; `rebuild --dl` changes it.
DEFAULT_DL = "https://dl.alai.sh/sha256/{sha256-prefix}/{sha256-checksum}.tar.gz"

# Ledger "collection" for failures of the Galaxy-wide checks (feed, listing,
# repo metadata, count, sweep); their "version" names the check.
GLOBAL = "*"

# Why a collection is queued, in the order the queue is worked: what a person
# asked for, then new uploads, then the listing's and the sweep's findings
# (which include the initial import's backlog).
SOURCES = ("requested", "uploaded", "listed", "swept")

# Zstandard level for each collection's extractions. Versions repeat each
# other's plugins and routing, which Zstandard's long window finds: on
# community.general's 243 versions, 41 MB of JSON becomes 0.27 MB in 0.04 s,
# where gzip only gets it to 5.3 MB.
EXTRACTIONS_LEVEL = 10

# State is saved after this much work: each version mirrored or rechecked
# counts one, and so does each collection reconciled.
SAVE_EVERY = 100

# A failure outstanding this long is reported once as needing a person.
PERSISTENT = datetime.timedelta(days=1)

# Field order for index lines, matching the spec.
LINE_FIELDS = (
    "v",
    "name",
    "vers",
    "deps",
    "cksum",
    "size",
    "requires_ansible",
    "yanked",
    "deprecated",
    "origin",
    "published",
    "upstream_conflict",
)


def _dump(obj: object) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def _jsonl(items: Sequence[object]) -> str:
    return "".join(_dump(item) + "\n" for item in items)


def _write(backend: Backend, key: str, text: str) -> bool:
    """Write `text`, returning False if the stored content is already the same."""
    data = text.encode()
    if backend.read(key) == data:
        return False
    backend.write(key, data)
    return True


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_jsonl(data: bytes | None) -> list[object]:
    return [json.loads(line) for line in (data or b"").decode().splitlines() if line.strip()]


def ordered_line(line: IndexLine) -> dict[str, object]:
    """An index line with its fields in spec order, unknown ones last."""
    fields: dict[str, object] = dict(line)
    out = {k: fields[k] for k in LINE_FIELDS if k in fields}
    out.update({k: v for k, v in fields.items() if k not in out})
    return out


def _fold_conflicts(state: SyncState) -> bool:
    """Fold conflicts saved with one Galaxy artifact each into one per version; True if any were.

    Before conflicts collected observations, each differing artifact was its
    own entry, with `upstream`, `seen`, `changes` and `saved`. Remove this
    once the stored state has been converted.
    """
    if all("observed" in c for c in state["conflicts"]):
        return False
    folded: list[Conflict] = []
    open_by_version: dict[tuple[str, str], Conflict] = {}
    entries = cast(list[dict[str, str]], cast(object, state["conflicts"]))
    for legacy in sorted(entries, key=lambda c: c.get("opened") or c["seen"]):
        if "observed" in legacy:
            current = cast(Conflict, cast(object, legacy))
            folded.append(current)
            if "resolved" not in current:
                open_by_version[current["collection"], current["version"]] = current
            continue
        observation: Observation = {
            "sha256": legacy["upstream"],
            "uploaded": None,
            "seen": legacy["seen"],
            "changes": legacy["changes"],
            "saved": legacy["saved"],
        }
        version = (legacy["collection"], legacy["version"])
        if "resolved" not in legacy and (current := open_by_version.get(version)) is not None:
            current["observed"].append(observation)
            continue
        conflict: Conflict = {
            "collection": legacy["collection"],
            "version": legacy["version"],
            "ours": legacy["ours"],
            "opened": legacy["seen"],
            "observed": [observation],
        }
        if "resolved" in legacy:
            conflict["resolved"] = legacy["resolved"]
        else:
            open_by_version[version] = conflict
        folded.append(conflict)
    state["conflicts"] = folded
    return True


@dataclass(frozen=True)
class Rebuilt:
    """What `rebuild` did for one collection."""

    versions: int
    pages: int  # Galaxy view pages that changed
    plugins: int  # plugin lines


class StateChanged(StorageError):
    """Another run saved state.json since this one read it."""


class Store:
    def __init__(self, buckets: Buckets, dl_url: str | None = None, purger: Purger | None = None) -> None:
        """Open a store. The artifact URL template is `dl_url`, else the one in
        config.json, else DEFAULT_DL. With `purger`, index files this run
        changes are purged from the CDN when collections.json is written."""
        self.buckets = buckets
        self.purger = purger
        self.index = buckets.index
        self.dl = buckets.dl
        self.galaxy = buckets.galaxy
        self.state_bucket = buckets.state
        self.dl_url = dl_url or self._configured_dl() or DEFAULT_DL
        self.state: SyncState = {"collections": {}, "conflicts": [], "failures": {}, "pending": {}}
        self._saved = self.state_bucket.read("state.json")
        if self._saved is not None:
            self.state.update(json.loads(self._saved))
        folded = _fold_conflicts(self.state)
        # Ledger keys that appeared or cleared during this run, for the report.
        self._new_failures: set[str] = set()
        self._resolved_failures: set[str] = set()
        self._new_conflicts: list[Conflict] = []
        self._acknowledged: list[Conflict] = []
        # Collections whose public conflict record needs writing.
        self._conflicts_changed: set[str] = self.conflicted_collections() if folded else set()
        # Plugin lines written this run, so merging them needn't read them back.
        self._plugin_lines: dict[str, list[PluginLine]] = {}
        # Each collection's extractions object as read or last saved, to skip unchanged saves.
        self._saved_extractions: dict[str, bytes] = {}
        # Index bucket keys changed since the last purge, in the order written.
        self._unpurged: dict[str, None] = {}
        self._unsaved_work = 0

    def _configured_dl(self) -> str | None:
        if (data := self.index.read("config.json")) is None:
            return None
        dl: object = json.loads(data).get("dl")
        return dl if isinstance(dl, str) and dl else None

    def save_state(self) -> None:
        """Save state.json, refusing if another run saved it since this one read it.

        Scheduling keeps runs from overlapping; this catches one started some
        other way, e.g. by hand. On R2 the check is part of the write, so it
        can't race.
        """
        self._unsaved_work = 0
        data = (json.dumps(self.state, indent=1, sort_keys=True) + "\n").encode()
        if data == self._saved:
            return
        try:
            self.state_bucket.replace("state.json", data, self._saved)
        except PreconditionFailed:
            raise StateChanged("state.json was changed by another run; stopping without saving") from None
        self._saved = data

    def add_work(self) -> None:
        self._unsaved_work += 1

    def save_due(self) -> bool:
        """Whether enough work was done since state was last saved to save it again.

        A run that stops loses at most this much: whatever's mirrored is
        found again in the index files, and the rest stays queued.
        """
        return self._unsaved_work >= SAVE_EVERY

    def collection_state(self, fqcn: str) -> CollectionState:
        return self.state["collections"].setdefault(fqcn, {"versions": {}})

    # Failures never stop a run. Each is recorded with enough detail to act on,
    # retried on every later run, and cleared once it succeeds.

    @staticmethod
    def _failure_key(fqcn: str, vers: str | None, kind: str) -> str:
        return f"{fqcn}@{vers or '*'}#{kind}"

    def record_failure(self, fqcn: str, vers: str | None, kind: str, message: str) -> None:
        log.warning("%s%s: %s: %s", fqcn, f" {vers}" if vers else "", kind, message)
        key = self._failure_key(fqcn, vers, kind)
        failures = self.state["failures"]
        previous = failures.get(key)
        seen = now()
        failures[key] = {
            "collection": fqcn,
            "version": vers,
            "kind": kind,
            "message": message,
            "first_seen": previous["first_seen"] if previous else seen,
            "last_seen": seen,
            "attempts": (previous["attempts"] if previous else 0) + 1,
        }
        if previous and "alerted" in previous:
            failures[key]["alerted"] = previous["alerted"]
        self._new_failures.add(key)

    def clear_failures(
        self,
        fqcn: str,
        vers: str | None = None,
        *,
        kinds: tuple[str, ...] | None = None,
        exclude: tuple[str, ...] = (),
    ) -> None:
        """Clear a collection's failures, or one version's, optionally only some kinds."""
        for key, entry in list(self.state["failures"].items()):
            if (
                entry["collection"] == fqcn
                and (vers is None or entry["version"] == vers)
                and (kinds is None or entry["kind"] in kinds)
                and entry["kind"] not in exclude
            ):
                del self.state["failures"][key]
                self._resolved_failures.add(key)

    def clear_collection_failures(self, fqcn: str) -> None:
        """Clear failures that weren't tied to a version, e.g. listing its versions failed."""
        for key, entry in list(self.state["failures"].items()):
            if entry["collection"] == fqcn and entry["version"] is None:
                del self.state["failures"][key]
                self._resolved_failures.add(key)

    # Collections waiting for a reconcile. A run that stops early leaves the
    # rest queued for the next one.

    def enqueue(
        self,
        fqcn: str,
        source: str,
        *,
        revalidate: bool = False,
        reasons: Sequence[str] = (),
        uploads: Sequence[str] = (),
        unseen: Sequence[str] = (),
    ) -> None:
        """Queue a collection, keeping its earlier place if that's sooner.

        `reasons` and `uploads` say what change detection saw, so the run
        that reconciles it can explain a reconcile that changed nothing.
        `unseen` are mirrored versions the sweep didn't see, for reconcile to
        note which ones Galaxy still serves.
        """
        entry = self.state["pending"].get(fqcn)
        if entry is None:
            entry = self.state["pending"][fqcn] = {"source": source, "queued": now()}
        elif SOURCES.index(source) < SOURCES.index(entry["source"]):
            entry["source"] = source
        if revalidate:
            entry["revalidate"] = True
        if reasons:
            entry["reasons"] = list(dict.fromkeys([*entry.get("reasons", []), *reasons]))
        if uploads:
            entry["uploads"] = list(dict.fromkeys([*entry.get("uploads", []), *uploads]))
        if unseen:
            entry["unseen"] = list(dict.fromkeys([*entry.get("unseen", []), *unseen]))

    def queued(self) -> list[tuple[str, str]]:
        """Pending collections in the order to work them."""
        pending = self.state["pending"]
        order = sorted(
            pending, key=lambda fqcn: (SOURCES.index(pending[fqcn]["source"]), pending[fqcn]["queued"], fqcn)
        )
        return [(ns, name) for ns, _, name in (fqcn.partition(".") for fqcn in order)]

    # A `refresh` writes a request object instead of touching state.json, so
    # it can run alongside a sync; the next sync queues it and deletes it.

    def add_request(
        self, targets: list[str], revalidate: bool, acknowledge: list[str] | None = None, note: str = ""
    ) -> str:
        requested = now()
        key = f"requests/{requested.replace(':', '')}-{secrets.token_hex(4)}.json"
        request: Request = {"requested": requested, "targets": targets, "revalidate": revalidate}
        if acknowledge:
            request["acknowledge"] = acknowledge
            request["note"] = note
        self.state_bucket.write(key, (json.dumps(request) + "\n").encode())
        return key

    def requests(self) -> list[tuple[str, Request]]:
        return [
            (key, cast(Request, json.loads(data)))
            for key in self.state_bucket.list("requests/")
            if (data := self.state_bucket.read(key)) is not None
        ]

    def delete_request(self, key: str) -> None:
        self.state_bucket.delete(key)

    def failed_collections(self) -> list[tuple[str, str]]:
        names = {e["collection"] for e in self.state["failures"].values()} - {GLOBAL}
        return sorted((ns, name) for ns, _, name in (n.partition(".") for n in names))

    # When Galaxy serves different contents for a mirrored version, ours is
    # kept and a conflict is opened for a person to inspect. It collects every
    # differing artifact Galaxy serves after that, stops being reported once a
    # person acknowledges it, and is resolved once Galaxy serves our contents
    # again. Nothing is ever removed: each collection's conflicts are
    # published in full as conflicts/<ns>/<name>.json in the index bucket.

    @staticmethod
    def conflict_key(sha256: str) -> str:
        return f"conflicts/{sha256[:2]}/{sha256}.tar.gz"

    def save_upstream_artifact(self, sha256: str, data: bytes) -> str:
        """Keep Galaxy's artifact privately, for whoever inspects the conflict."""
        key = self.conflict_key(sha256)
        if not self.state_bucket.exists(key):
            self.state_bucket.write(key, data)
        return key

    def open_conflicts(self) -> list[Conflict]:
        return [c for c in self.state["conflicts"] if "resolved" not in c]

    def conflicts_to_inspect(self) -> list[Conflict]:
        """Open conflicts no one has acknowledged yet."""
        return [c for c in self.open_conflicts() if "acknowledged" not in c]

    def _open_conflict(self, fqcn: str, vers: str) -> Conflict | None:
        return next((c for c in self.open_conflicts() if (c["collection"], c["version"]) == (fqcn, vers)), None)

    def record_conflict(self, fqcn: str, vers: str, ours: str, observation: Observation) -> bool:
        """Add an artifact Galaxy serves for a version to its open conflict, opening one if needed.

        True if the conflict is new; only that is alerted.
        """
        self._conflicts_changed.add(fqcn)
        conflict = self._open_conflict(fqcn, vers)
        if conflict is None:
            conflict = Conflict(collection=fqcn, version=vers, ours=ours, opened=observation["seen"], observed=[])
            self.state["conflicts"].append(conflict)
            self._new_conflicts.append(conflict)
        if all(o["sha256"] != observation["sha256"] for o in conflict["observed"]):
            conflict["observed"].append(observation)
        return conflict in self._new_conflicts

    def resolve_conflicts(self, fqcn: str, vers: str) -> bool:
        """Mark a version's open conflict resolved; True if there was one."""
        if (conflict := self._open_conflict(fqcn, vers)) is None:
            return False
        conflict["resolved"] = now()
        self._conflicts_changed.add(fqcn)
        return True

    def acknowledge_conflict(self, fqcn: str, vers: str, note: str) -> Conflict | None:
        """Acknowledge a version's open conflict, or replace its note; None if it has none."""
        if (conflict := self._open_conflict(fqcn, vers)) is None:
            return None
        conflict["acknowledged"] = {"at": now(), "note": note}
        self._acknowledged.append(conflict)
        self._conflicts_changed.add(fqcn)
        return conflict

    @property
    def acknowledged(self) -> list[Conflict]:
        """The conflicts acknowledged this run."""
        return self._acknowledged

    @staticmethod
    def conflict_record_key(namespace: str, name: str) -> str:
        return f"conflicts/{namespace}/{name}.json"

    def write_conflict_records(self, fqcns: Collection[str] | None = None) -> int:
        """Write the public conflict record of each collection whose conflicts changed, or of `fqcns`.

        Returns how many were written. Written after the index files, as they
        name versions in them.
        """
        names = self._conflicts_changed if fqcns is None else fqcns
        written = 0
        for fqcn in sorted(names):
            if conflicts := [c for c in self.state["conflicts"] if c["collection"] == fqcn]:
                ns, _, name = fqcn.partition(".")
                written += self._write_index_file(
                    self.conflict_record_key(ns, name), report.conflict_record(fqcn, conflicts)
                )
        self._conflicts_changed.clear()
        return written

    def conflicted_collections(self) -> set[str]:
        return {c["collection"] for c in self.state["conflicts"]}

    def write_failure_report(self) -> report.Report:
        """Write failures.json and return the report, with the Markdown for the tracking issue."""
        failures = self.state["failures"]
        new = self._new_failures & {k for k, e in failures.items() if e["attempts"] == 1}
        generated = now()
        cutoff = datetime.datetime.fromisoformat(generated) - PERSISTENT
        persistent: list[Failure] = []
        for entry in failures.values():
            if "alerted" not in entry and datetime.datetime.fromisoformat(entry["first_seen"]) <= cutoff:
                entry["alerted"] = generated
                persistent.append(entry)
        result = report.render(
            failures,
            self.open_conflicts(),
            new,
            self._resolved_failures - set(failures),
            generated,
            alerts=report.Alerts(self._new_conflicts, persistent, self._acknowledged),
        )
        _ = _write(self.state_bucket, "failures.json", result.json)
        return result

    @staticmethod
    def artifact_key(cksum: str) -> str:
        return f"sha256/{cksum[:2]}/{cksum}.tar.gz"

    def write_artifact(self, cksum: str, data: bytes) -> None:
        key = self.artifact_key(cksum)
        if not self.dl.exists(key):  # content-addressed: same key means same bytes
            self.dl.write(key, data)

    def read_artifact(self, cksum: str) -> bytes | None:
        return self.dl.read(self.artifact_key(cksum))

    @staticmethod
    def _extractions_key(namespace: str, name: str) -> str:
        return f"extract/{namespace}/{name}.json.zst"

    def load_extractions(self, namespace: str, name: str) -> dict[str, ExtractionData]:
        """A collection's stored extractions, by artifact sha256: one object for the whole collection."""
        data = self.state_bucket.read(self._extractions_key(namespace, name))
        if data is None:
            return {}
        self._saved_extractions[f"{namespace}.{name}"] = data
        return json.loads(zstd.decompress(data))

    def save_extractions(self, namespace: str, name: str, extractions: dict[str, ExtractionData]) -> None:
        """Save a collection's extractions, unless they're what `load_extractions` read."""
        fqcn = f"{namespace}.{name}"
        if not extractions and fqcn not in self._saved_extractions:
            return  # nothing extracted yet, e.g. every version failed
        data = zstd.compress((_dump(dict(sorted(extractions.items()))) + "\n").encode(), EXTRACTIONS_LEVEL)
        if data != self._saved_extractions.get(fqcn):
            self.state_bucket.write(self._extractions_key(namespace, name), data)
            self._saved_extractions[fqcn] = data

    def save_builtin(self, lines: list[PluginLine]) -> None:
        """Plugin lines for ansible.builtin, merged into every plugins.jsonl."""
        _ = _write(self.state_bucket, "builtin.jsonl", _jsonl(lines))

    def load_builtin(self) -> list[PluginLine]:
        return cast(list[PluginLine], _read_jsonl(self.state_bucket.read("builtin.jsonl")))

    @staticmethod
    def index_key(namespace: str, name: str) -> str:
        return f"index/{namespace}/{name}.jsonl"

    def index_collections(self) -> list[tuple[str, str]]:
        """(namespace, name) of every collection with an index file."""
        result: list[tuple[str, str]] = []
        for key in self.index.list("index/"):
            parts = key.split("/")
            if len(parts) == 3 and parts[2].endswith(".jsonl"):
                result.append((parts[1], parts[2].removesuffix(".jsonl")))
        return result

    def read_index(self, namespace: str, name: str) -> list[IndexLine]:
        return cast(list[IndexLine], _read_jsonl(self.index.read(self.index_key(namespace, name))))

    def write_index(
        self,
        namespace: str,
        name: str,
        lines: list[IndexLine],
        added: Collection[str] = (),
        *,
        view: bool = False,
        partial: bool = False,
    ) -> bool:
        """Write an index file and, if it changed or with `view`, the collection's Galaxy view.

        `added` names versions new to the index, whose Galaxy pages are
        written without reading them first. `partial` writes only those
        pages and the listings, for progress saved part way through.
        """
        text = "".join(_dump(ordered_line(line)) + "\n" for line in lines)
        changed = self._write_index_file(self.index_key(namespace, name), text)
        state = self.collection_state(f"{namespace}.{name}")
        state["sha256"] = _sha256(text.encode())  # what's stored now, changed or not
        if changed:
            state["updated"] = now()
        if changed or view:
            _ = self.write_galaxy_collection(namespace, name, lines, added, partial=partial)
        return changed

    def _write_galaxy_page(self, path: str, body: dict[str, object], *, new: bool = False) -> bool:
        """Write one page, returning False if it was already the same."""
        key = galaxy_api.page_key(path)
        text = json.dumps(body, ensure_ascii=False) + "\n"
        if new:
            self.galaxy.write(key, text.encode())
        elif not _write(self.galaxy, key, text):
            return False
        log.debug("wrote %s", key)
        return True

    def write_galaxy_collection(
        self, namespace: str, name: str, lines: list[IndexLine], added: Collection[str] = (), *, partial: bool = False
    ) -> int:
        """Write one collection's Galaxy v3 pages from its index lines; returns how many changed.

        Version pages come first, so the listing never names a page that isn't
        written yet. Yanked versions keep their page but leave the listing.
        """
        written = 0
        for line in (line for line in lines if line["vers"] in added) if partial else lines:
            written += self._write_galaxy_page(
                galaxy_api.version_path(namespace, name, line["vers"]),
                galaxy_api.version(line, self.dl_url),
                new=line["vers"] in added,
            )
        written += self._write_galaxy_page(
            galaxy_api.versions_path(namespace, name), galaxy_api.versions(namespace, name, lines)
        )
        updated = self.collection_state(f"{namespace}.{name}").get("updated") or now()
        written += self._write_galaxy_page(
            galaxy_api.collection_path(namespace, name), galaxy_api.collection(namespace, name, lines, updated)
        )
        return written

    def write_galaxy_root(self) -> None:
        for path in (galaxy_api.SERVER_ROOT, galaxy_api.API_ROOT):
            _ = self._write_galaxy_page(path, galaxy_api.root(path))

    def rebuild_collection(self, namespace: str, name: str, *, view: bool = True) -> Rebuilt:
        """Rewrite one collection's Galaxy view and plugin lines from its index file and extractions.

        `rebuild` runs this for every collection, then `merge_plugins`.
        Without `view` the Galaxy view is left alone: it reads every version
        page, so it's most of a rebuild's time.
        """
        data = self.index.read(self.index_key(namespace, name))
        self.collection_state(f"{namespace}.{name}")["sha256"] = _sha256(data or b"")
        lines = cast(list[IndexLine], _read_jsonl(data))
        pages = self.write_galaxy_collection(namespace, name, lines) if view else 0
        extractions = self.load_extractions(namespace, name)
        plugins = self.set_collection_plugins(
            namespace,
            name,
            {line["vers"]: extractions[line["cksum"]] for line in lines if line["cksum"] in extractions},
        )
        return Rebuilt(len(lines), pages, plugins)

    def write_config(self) -> None:
        config = {
            "formats": {"1": {"index": "index/", "collections": "collections.json", "plugins": "plugins.jsonl"}},
            "dl": self.dl_url,
            "moved-to": None,
        }
        _ = self._write_index_file("config.json", json.dumps(config, indent=2) + "\n")

    def write_collections(self) -> None:
        """Write collections.json, after every other index bucket file.

        Each collection's `sha256` comes from state, so writing it reads no
        index files, except to fill in one state doesn't have yet. The index
        files this run changed are purged before it's written and it's
        purged after, so a client that sees a new `sha256` gets the file it
        describes (docs/index-spec/README.md, "Publishing order").
        """
        namespaces: dict[str, dict[str, dict[str, str]]] = {}
        for ns, name in self.index_collections():
            state = self.collection_state(f"{ns}.{name}")
            if "sha256" not in state:
                state["sha256"] = _sha256(self.index.read(self.index_key(ns, name)) or b"")
            namespaces.setdefault(ns, {})[name] = {"updated": state.get("updated") or now(), "sha256": state["sha256"]}
        self.purge()
        _ = self._write_index_file("collections.json", json.dumps({"v": 1, "namespaces": namespaces}, indent=1) + "\n")
        self.purge()

    def _write_index_file(self, key: str, text: str) -> bool:
        """Write one index bucket file, noting it for the next purge if it changed."""
        changed = _write(self.index, key, text)
        if changed:
            self._unpurged[key] = None
        return changed

    def purge(self) -> None:
        """Purge the index bucket files changed since the last purge from the CDN.

        A failure is recorded and the run carries on: the CDN serves the old
        files only until their max-age runs out, so they aren't purged again.
        """
        keys, self._unpurged = list(self._unpurged), {}
        if self.purger is None or not keys:
            return
        try:
            self.purger.purge(keys)
        except PurgeError as e:
            self.record_failure(GLOBAL, "purge", "cdn_purge", f"purging {len(keys)} files: {e}")
            return
        log.info("purged %d files from the CDN", len(keys))
        self.clear_failures(GLOBAL, "purge")

    def set_collection_plugins(self, namespace: str, name: str, extractions: dict[str, ExtractionData]) -> int:
        """Work out one collection's plugin lines, from each version's extraction, for `write_plugins` to merge.

        Returns how many there are. Every reconcile marks its collection for
        merging, changed or not: a run that stopped before saving state
        reconciles it again.
        """
        fqcn = f"{namespace}.{name}"
        lines = self._plugin_lines[fqcn] = plugin_lines(fqcn, extractions)
        pending = self.state.setdefault("plugins_pending", [])
        if fqcn not in pending:
            pending.append(fqcn)
        return len(lines)

    def _collection_plugins(self, fqcn: str) -> list[PluginLine]:
        """A collection's plugin lines: from this run, or worked out from its index file and extractions."""
        if (lines := self._plugin_lines.get(fqcn)) is not None:
            return lines
        namespace, _, name = fqcn.partition(".")
        extractions = self.load_extractions(namespace, name)
        return plugin_lines(
            fqcn,
            {
                line["vers"]: extractions[line["cksum"]]
                for line in self.read_index(namespace, name)
                if line["cksum"] in extractions
            },
        )

    def write_plugins(self) -> int:
        """Merge ansible.builtin and the collections reconciled since the last merge into plugins.jsonl.

        Returns its line count. Reads only plugins.jsonl, builtin.jsonl, and
        for a collection reconciled by a run that stopped before merging, its
        index file and extractions. A store without plugins.jsonl gets it
        built from every collection's. ansible.builtin's lines are always
        taken from builtin.jsonl, so a `builtin` run that overlapped a sync
        can't be undone by it.
        """
        if (merged := self.index.read("plugins.jsonl")) is None:
            return self.merge_plugins()
        replaced = set(self.state.get("plugins_pending", [])) | {"ansible.builtin"}
        lines = self.load_builtin()
        lines.extend(line for line in cast(list[PluginLine], _read_jsonl(merged)) if line["collection"] not in replaced)
        for fqcn in replaced - {"ansible.builtin"}:
            lines.extend(self._collection_plugins(fqcn))
        return self._write_plugins(lines)

    def merge_plugins(self) -> int:
        """Build plugins.jsonl from ansible.builtin's and every collection's plugin lines."""
        lines = self.load_builtin()
        for ns, name in self.index_collections():
            lines.extend(self._collection_plugins(f"{ns}.{name}"))
        return self._write_plugins(lines)

    def _write_plugins(self, lines: list[PluginLine]) -> int:
        lines.sort(key=lambda line: (line["type"], line["name"], line["collection"]))
        _ = self._write_index_file("plugins.jsonl", _jsonl(lines))
        _ = self.state.pop("plugins_pending", None)
        return len(lines)
