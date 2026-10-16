"""Store: the reconcile queue and guarding state.json."""

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
