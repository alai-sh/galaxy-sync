"""Store: the reconcile queue and guarding state.json."""

import json

import pytest

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
