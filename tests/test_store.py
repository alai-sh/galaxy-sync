"""Store: the reconcile queue and guarding state.json."""

import hashlib
import json

import pytest

from galaxy_sync.cdn import PurgeError
from galaxy_sync.storage import Buckets
from galaxy_sync.store import StateChanged, Store


def test_queue_order(tmp_path):
    store = Store(Buckets.local(tmp_path))
    store.enqueue("ns.swept", "swept")
    store.enqueue("ns.listed", "listed")
    store.enqueue("ns.uploaded", "uploaded")
    store.enqueue("ns.requested", "requested")
    store.enqueue("ns.later", "requested")
    store.state["pending"]["ns.later"]["queued"] = "2999-01-01T00:00:00Z"
    assert store.queued() == [
        ("ns", "requested"),
        ("ns", "later"),
        ("ns", "uploaded"),
        ("ns", "listed"),
        ("ns", "swept"),
    ]

    # Queuing again only ever moves a collection forward, and keeps a revalidate.
    store.enqueue("ns.swept", "uploaded", revalidate=True)
    store.enqueue("ns.swept", "listed")
    assert store.state["pending"]["ns.swept"]["source"] == "uploaded"
    assert store.state["pending"]["ns.swept"]["revalidate"] is True


def test_state_changed_by_another_run(tmp_path):
    first = Store(Buckets.local(tmp_path))
    first.state["feed_cursor"] = "a"
    first.save_state()
    first.save_state()  # its own save doesn't count as a change

    second = Store(Buckets.local(tmp_path))
    second.state["feed_cursor"] = "b"
    second.save_state()
    first.state["feed_cursor"] = "c"
    with pytest.raises(StateChanged):
        first.save_state()
    assert Store(Buckets.local(tmp_path)).state["feed_cursor"] == "b"


def test_requests(tmp_path):
    store = Store(Buckets.local(tmp_path))
    first = store.add_request(["ns.coll"], True)
    second = store.add_request([], False)
    taken = store.requests()
    assert [key for key, _ in taken] == sorted([first, second])
    assert {tuple(r["targets"]): r["revalidate"] for _, r in taken} == {("ns.coll",): True, (): False}
    store.delete_request(first)
    assert [key for key, _ in store.requests()] == [second]


def test_conflicts_saved_one_per_artifact_are_folded(tmp_path):
    def legacy(upstream, seen, **extra):
        return {
            "collection": "ns.coll",
            "version": "1.0.0",
            "ours": "a" * 64,
            "upstream": upstream,
            "seen": seen,
            "changes": f"changes in {upstream}",
            "saved": f"conflicts/{upstream[:2]}/{upstream}.tar.gz",
            **extra,
        }

    old = Store(Buckets.local(tmp_path))
    old.state["conflicts"] = [
        legacy("c" * 64, "2026-10-19T00:06:49Z"),
        legacy("b" * 64, "2026-10-18T23:20:18Z"),
        legacy("d" * 64, "2026-10-01T00:00:00Z", resolved="2026-10-02T00:00:00Z"),
    ]
    old.save_state()

    store = Store(Buckets.local(tmp_path))
    resolved, current = store.state["conflicts"]
    assert resolved["resolved"] == "2026-10-02T00:00:00Z"
    assert [o["sha256"] for o in resolved["observed"]] == ["d" * 64]
    assert current == {
        "collection": "ns.coll",
        "version": "1.0.0",
        "ours": "a" * 64,
        "opened": "2026-10-18T23:20:18Z",
        "observed": [
            {
                "sha256": sha,
                "uploaded": None,
                "seen": seen,
                "changes": f"changes in {sha}",
                "saved": f"conflicts/{sha[:2]}/{sha}.tar.gz",
            }
            for sha, seen in (("b" * 64, "2026-10-18T23:20:18Z"), ("c" * 64, "2026-10-19T00:06:49Z"))
        ],
    }

    # Their public records are written, as the conflicts changed shape.
    assert store.write_conflict_records() == 1
    assert json.loads(store.index.read("conflicts/ns/coll.json") or b"")["conflicts"][1]["opened"] == current["opened"]

    # Folded state stays as it is.
    store.save_state()
    assert Store(Buckets.local(tmp_path)).state["conflicts"] == [resolved, current]


LINE = {
    "v": 1,
    "name": "ns.coll",
    "vers": "1.0.0",
    "deps": [],
    "cksum": "a" * 64,
    "size": 1,
    "yanked": False,
    "published": "2026-01-01T00:00:00Z",
}


def _listed(store, name="coll"):
    return json.loads(store.index.read("collections.json"))["namespaces"]["ns"][name]


def test_collections_sha256(tmp_path):
    store = Store(Buckets.local(tmp_path))
    _ = store.write_index("ns", "coll", [LINE])
    store.write_collections()
    data = store.index.read("index/ns/coll.jsonl")
    assert _listed(store)["sha256"] == hashlib.sha256(data).hexdigest()

    # A collection state has no hash for yet is read once to fill it in.
    store.index.write("index/ns/other.jsonl", b"{}\n")
    del store.state["collections"]["ns.coll"]["sha256"]
    store.write_collections()
    assert _listed(store)["sha256"] == hashlib.sha256(data).hexdigest()
    assert _listed(store, "other")["sha256"] == hashlib.sha256(b"{}\n").hexdigest()

    # Rebuild takes the hash from the file, whatever state says.
    store.state["collections"]["ns.coll"]["sha256"] = "stale"
    _ = store.rebuild_collection("ns", "coll")
    store.write_collections()
    assert _listed(store)["sha256"] == hashlib.sha256(data).hexdigest()


class FakePurger:
    def __init__(self, store, error=None):
        self.store = store
        self.error = error
        self.calls = []

    def purge(self, keys):
        listing = self.store.index.read("collections.json")
        self.calls.append((list(keys), listing and json.loads(listing)))
        if self.error:
            raise PurgeError(self.error)


def test_purge_order(tmp_path):
    store = Store(Buckets.local(tmp_path))
    store.purger = purger = FakePurger(store)
    _ = store.write_index("ns", "coll", [LINE])
    store.write_config()
    store.write_collections()
    # The index files first, while collections.json doesn't name them yet, then collections.json.
    assert purger.calls == [(["index/ns/coll.jsonl", "config.json"], None), (["collections.json"], purger.calls[1][1])]

    # Unchanged files aren't purged, and nothing at all is when nothing changed.
    purger.calls.clear()
    _ = store.write_index("ns", "coll", [LINE])
    store.write_config()
    store.write_collections()
    assert purger.calls == []

    _ = store.write_index("ns", "coll", [{**LINE, "yanked": True}])
    store.write_collections()
    assert [keys for keys, _ in purger.calls] == [["index/ns/coll.jsonl"], ["collections.json"]]
    assert purger.calls[0][1]["namespaces"]["ns"]["coll"]["sha256"] != _listed(store)["sha256"]


def test_purge_failure_is_recorded(tmp_path):
    store = Store(Buckets.local(tmp_path))
    store.purger = FakePurger(store, error="HTTP 403: Authentication error")
    _ = store.write_index("ns", "coll", [LINE])
    store.write_collections()  # carries on, and writes collections.json anyway
    assert store.index.read("collections.json") is not None
    [failure] = store.state["failures"].values()
    assert (failure["collection"], failure["version"], failure["kind"]) == ("*", "purge", "cdn_purge")
    assert failure["message"] == "purging 1 files: HTTP 403: Authentication error"

    store.purger = FakePurger(store)
    _ = store.write_index("ns", "coll", [{**LINE, "yanked": True}])
    store.write_collections()
    assert store.state["failures"] == {}
