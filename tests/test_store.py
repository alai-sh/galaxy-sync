"""Store: the reconcile queue and guarding state.json."""

import datetime
import hashlib
import json

import pytest

from galaxy_sync import store as store_mod
from galaxy_sync.storage import Buckets, StorageError
from galaxy_sync.store import RETIRED_GRACE, StateChanged, Store


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
    sha256 = _listed(store)["sha256"]
    data = store.index.read(f"index/{sha256[:2]}/{sha256}.jsonl")
    assert data is not None
    assert hashlib.sha256(data).hexdigest() == sha256
    # Nothing by name, in either bucket.
    assert store.index.read("index/ns/coll.jsonl") is None
    assert list(store.state_bucket.list("index/")) == []
    assert store.read_index("ns", "coll") == [LINE]


def test_read_index_missing_file(tmp_path):
    store = Store(Buckets.local(tmp_path))
    assert store.read_index("ns", "never") == []
    _ = store.write_index("ns", "coll", [LINE])
    for key in list(store.index.list("index/")):
        store.index.delete(key)
    # Never read as empty, which would mirror the whole collection again.
    with pytest.raises(StorageError, match=r"ns\.coll's index file .* is missing"):
        _ = store.read_index("ns", "coll")


def test_read_index_drops_removed_fields(tmp_path):
    store = Store(Buckets.local(tmp_path))
    _ = store.write_index("ns", "coll", [{**LINE, "origin": "galaxy", "upstream_conflict": True}])  # pyright: ignore[reportArgumentType]
    old = store.state["collections"]["ns.coll"]["sha256"]
    assert store.read_index("ns", "coll") == [LINE]
    assert store.migrate_collection("ns", "coll")
    assert store.state["collections"]["ns.coll"]["sha256"] != old
    assert not store.migrate_collection("ns", "coll")  # already without them


def _published(store, data):
    sha256 = hashlib.sha256(data).hexdigest()
    return store.index.read(f"index/{sha256[:2]}/{sha256}.jsonl")


def test_index_file_published_once(tmp_path):
    store = Store(Buckets.local(tmp_path))
    assert store.write_index("ns", "coll", [LINE])
    assert not store.write_index("ns", "coll", [LINE])
    assert store.write_index("ns", "coll", [{**LINE, "yanked": True}])

    # One file per content, each kept: the old one until pruning removes it.
    assert len(list(store.index.list("index/"))) == 2
    assert store.index_collections() == [("ns", "coll")]
    assert store.read_index("ns", "coll") == [{**LINE, "yanked": True}]


def test_unchanged_index_file_writes_nothing(tmp_path):
    store = Store(Buckets.local(tmp_path))
    _ = store.write_index("ns", "coll", [LINE])
    written = []
    store.index.write = lambda key, data: written.append(key)  # pyright: ignore[reportAttributeAccessIssue]
    assert not store.write_index("ns", "coll", [LINE])
    assert written == []


def test_stopped_run_keeps_the_old_file(tmp_path):
    store = Store(Buckets.local(tmp_path))
    _ = store.write_index("ns", "coll", [LINE])
    store.save_state()
    _ = store.write_index("ns", "coll", [{**LINE, "yanked": True}])

    # The run stops before saving state: the next one reads the file it replaced, and reconciles from there.
    assert Store(Buckets.local(tmp_path)).read_index("ns", "coll") == [LINE]


def test_prune(tmp_path, monkeypatch):
    store = Store(Buckets.local(tmp_path))
    _ = store.write_index("ns", "coll", [LINE])
    old = next(iter(store.index.list("index/")))
    _ = store.write_index("ns", "coll", [{**LINE, "yanked": True}])
    store.index.write("index/ns/coll.jsonl", b"an old file by name\n")
    store.write_collections()
    assert store.prune_due()

    start = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
    monkeypatch.setattr(store_mod, "now", lambda: start.isoformat())
    assert store.prune_index_files() == 0
    assert set(store.state["retired_index_files"]) == {old, "index/ns/coll.jsonl"}
    assert not store.prune_due()

    # Kept until no listing has named them for RETIRED_GRACE.
    monkeypatch.setattr(store_mod, "now", lambda: (start + RETIRED_GRACE - datetime.timedelta(minutes=1)).isoformat())
    assert store.prune_index_files() == 0
    monkeypatch.setattr(store_mod, "now", lambda: (start + RETIRED_GRACE).isoformat())
    assert store.prune_index_files() == 2
    assert "retired_index_files" not in store.state
    [current] = store.index.list("index/")
    assert current == store.index_key(_listed(store)["sha256"])


def test_prune_forgets_a_file_named_again(tmp_path):
    store = Store(Buckets.local(tmp_path))
    _ = store.write_index("ns", "coll", [LINE])
    first = next(iter(store.index.list("index/")))
    _ = store.write_index("ns", "coll", [{**LINE, "yanked": True}])
    store.write_collections()
    _ = store.prune_index_files()
    assert set(store.state["retired_index_files"]) == {first}

    # Unyanked: the first file is named again, so it's no longer retired.
    _ = store.write_index("ns", "coll", [LINE])
    store.write_collections()
    _ = store.prune_index_files()
    assert first not in store.state.get("retired_index_files", {})


def test_prune_needs_this_runs_listing(tmp_path):
    store = Store(Buckets.local(tmp_path))
    store.index.write("index/ns/coll.jsonl", b"x\n")
    assert store.prune_index_files() == 0
    assert "retired_index_files" not in store.state
