"""The sync's own state and files, on top of one storage backend per bucket.

    index bucket   conflicts/<ns>/<name>.json, and pruning index/
    state bucket   state.json, conflicts/, requests/

The index itself is alai-index's, through tree.py. See docs/storage.md.
"""

import datetime
import json
import logging
import secrets
from collections.abc import Callable, Collection, Sequence
from typing import cast

from . import report
from .models import CollectionState, Conflict, Failure, Observation, Request, SyncState
from .storage import Buckets, PreconditionFailed, StorageError
from .times import now

log = logging.getLogger(__name__)

# Ledger "collection" for failures of the Galaxy-wide checks (feed, listing,
# repo metadata, count, sweep); their "version" names the check.
GLOBAL = "*"

# What a `request` can ask the next sync for.
ACTIONS = ("mirror", "sweep", "generate-plugins", "generate-indexes")

# Why a collection is queued, in the order the queue is worked: what a person
# asked for, then new uploads, then the listing's and the sweep's findings
# (which include the initial import's backlog).
SOURCES = ("requested", "uploaded", "listed", "swept")

# State is saved after this much work: each version mirrored or rechecked
# counts one, and so does each collection reconciled.
SAVE_EVERY = 100

# Failure kinds reconciling a collection again doesn't clear.
NOT_RETRIED = frozenset({"extract_error"})

# A failure outstanding this long is reported once as needing a person.
PERSISTENT = datetime.timedelta(days=1)

# A published index file no collections.json names is kept this long, for
# clients and caches still holding an older listing, then deleted. Far longer
# than any copy of collections.json lives (its max-age, 10 minutes);
# docs/index-spec/README.md asks for at least 7 days.
RETIRED_GRACE = datetime.timedelta(days=7)

# How often retired index files are looked for, as listing the index bucket
# costs a request per thousand files.
PRUNE_EVERY = datetime.timedelta(days=1)


class StateChanged(StorageError):
    """Another run saved state.json since this one read it."""


class Store:
    def __init__(self, buckets: Buckets) -> None:
        self.buckets = buckets
        self.index = buckets.index
        self.state_bucket = buckets.state
        self.state: SyncState = {"collections": {}, "conflicts": [], "failures": {}, "pending": {}}
        self._saved = self.state_bucket.read("state.json")
        if self._saved is not None:
            self.state.update(json.loads(self._saved))
        # Ledger keys that appeared or cleared during this run, for the report.
        self._new_failures: set[str] = set()
        self._resolved_failures: set[str] = set()
        self._new_conflicts: list[Conflict] = []
        self._unsaved_work = 0
        self.before_save: Callable[[], object] | None = None
        """Run before state.json is saved. The run publishes the index's aggregate files there, so state never says a
        collection is done while the published collections.json still names its old index file."""

    def save_state(self) -> None:
        """Save state.json, refusing if another run saved it since this one read it.

        Scheduling keeps runs from overlapping; this catches one started some
        other way, e.g. by hand. On R2 the check is part of the write, so it
        can't race.
        """
        self._unsaved_work = 0
        if self.before_save is not None:
            _ = self.before_save()
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
        return self.state["collections"].setdefault(fqcn, {})

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

    # A `request` writes a request object instead of touching state.json, so
    # it can run alongside a sync; the next sync takes it and deletes it.

    def add_request(
        self, action: str, targets: list[str], *, revalidate: bool = False, from_plugin_cache: bool = False
    ) -> str:
        requested = now()
        key = f"requests/{requested.replace(':', '')}-{secrets.token_hex(4)}.json"
        request: Request = {"requested": requested, "action": action, "targets": targets}
        if revalidate:
            request["revalidate"] = True
        if from_plugin_cache:
            request["from_plugin_cache"] = True
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
        """Collections whose failures reconciling them again can clear. An extraction that failed isn't tried again
        until alai-index's extraction changes, or a generate-plugins request asks for it."""
        names = {e["collection"] for e in self.state["failures"].values() if e["kind"] not in NOT_RETRIED} - {GLOBAL}
        return sorted((ns, name) for ns, _, name in (n.partition(".") for n in names))

    # When Galaxy serves another artifact for a mirrored version, ours is
    # kept and a conflict is opened for a person to inspect. It collects every
    # artifact Galaxy serves after that, and is reported until it's
    # resolved, once Galaxy serves our artifact again. Nothing is ever removed
    # from state.json, so it's the history of every conflict.

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

    def _open_conflict(self, fqcn: str, vers: str) -> Conflict | None:
        return next((c for c in self.open_conflicts() if (c["collection"], c["version"]) == (fqcn, vers)), None)

    def record_conflict(self, fqcn: str, vers: str, ours: str, observation: Observation) -> bool:
        """Add an artifact Galaxy serves for a version to its open conflict, opening one if needed.

        True if the conflict is new; only that is alerted.
        """
        conflict = self._open_conflict(fqcn, vers)
        if conflict is None:
            conflict = Conflict(collection=fqcn, version=vers, ours=ours, opened=observation["seen"], observed=[])
            self.state["conflicts"].append(conflict)
            self._new_conflicts.append(conflict)
        if all(o["sha256"] != observation["sha256"] for o in conflict["observed"]):
            conflict["observed"].append(observation)
        return conflict in self._new_conflicts

    def checked(self, fqcn: str) -> dict[str, str]:
        """The latest upload time of the artifacts Galaxy served for each version of a collection with an open
        conflict, or "" when none is known. Galaxy normally serves the one it uploaded last."""
        return {
            c["version"]: max(o["uploaded"] or "" for o in c["observed"])
            for c in self.open_conflicts()
            if c["collection"] == fqcn
        }

    def known_artifact(self, fqcn: str, vers: str, sha256: str, uploaded: str | None) -> bool:
        """Whether a version's open conflict has Galaxy's artifact already, adding its upload time if it was missing."""
        if (conflict := self._open_conflict(fqcn, vers)) is None:
            return False
        if (seen := next((o for o in conflict["observed"] if o["sha256"] == sha256), None)) is None:
            return False
        if uploaded and seen["uploaded"] is None:
            seen["uploaded"] = uploaded
        return True

    def resolve_conflicts(self, fqcn: str, vers: str) -> bool:
        """Mark a version's open conflict resolved; True if there was one."""
        if (conflict := self._open_conflict(fqcn, vers)) is None:
            return False
        conflict["resolved"] = now()
        return True

    def failure_report(self) -> report.Report:
        """The report, with the Markdown for the tracking issue. Marks the failures alerted this run."""
        failures = self.state["failures"]
        new = self._new_failures & {k for k, e in failures.items() if e["attempts"] == 1}
        generated = now()
        cutoff = datetime.datetime.fromisoformat(generated) - PERSISTENT
        persistent: list[Failure] = []
        for entry in failures.values():
            if "alerted" not in entry and datetime.datetime.fromisoformat(entry["first_seen"]) <= cutoff:
                entry["alerted"] = generated
                persistent.append(entry)
        return report.render(
            failures,
            self.open_conflicts(),
            new,
            self._resolved_failures - set(failures),
            alerts=report.Alerts(self._new_conflicts, persistent),
        )

    def prune_due(self) -> bool:
        last = self.state.get("last_prune")
        return (
            last is None
            or datetime.datetime.fromisoformat(now()) - datetime.datetime.fromisoformat(last) >= PRUNE_EVERY
        )

    def prune_index_files(self, listed: Collection[str]) -> int:
        """Delete published index files `listed`, the keys collections.json names now, doesn't have and none has
        for RETIRED_GRACE, returning how many.

        A file is retired from when a prune first finds it unnamed: one a
        collection replaced, one left by a run that stopped between
        publishing and saving state, and the old files by name in the index
        bucket. One named again is no longer retired.
        """
        live = set(listed)
        retired = self.state.setdefault("retired_index_files", {})
        stamp = now()
        cutoff = datetime.datetime.fromisoformat(stamp) - RETIRED_GRACE
        keys = list(self.index.list("index/"))
        deleted = 0
        for key in keys:
            if key in live:
                _ = retired.pop(key, None)
            elif datetime.datetime.fromisoformat(retired.setdefault(key, stamp)) <= cutoff:
                self.index.delete(key)
                del retired[key]
                deleted += 1
        for key in set(retired) - set(keys):
            del retired[key]  # deleted some other way
        if not retired:
            del self.state["retired_index_files"]
        self.state["last_prune"] = stamp
        if deleted:
            log.info("deleted %d retired index files", deleted)
        return deleted
