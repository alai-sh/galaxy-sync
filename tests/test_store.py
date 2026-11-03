"""Store: the reconcile queue and guarding state.json."""

import datetime
import json

import pytest

from galaxy_sync import store as store_mod
from galaxy_sync.storage import Buckets
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
    first = store.add_request("mirror", ["ns.coll"], revalidate=True)
    second = store.add_request("generate-plugins", [], from_plugin_cache=True)
    taken = store.requests()
    assert [key for key, _ in taken] == sorted([first, second])
    assert sorted(({k: v for k, v in r.items() if k != "requested"} for _, r in taken), key=lambda r: r["action"]) == [
        {"action": "generate-plugins", "targets": [], "from_plugin_cache": True},
        {"action": "mirror", "targets": ["ns.coll"], "revalidate": True},
    ]
    store.delete_request(first)
    assert [key for key, _ in store.requests()] == [second]


def test_prune(tmp_path, monkeypatch):
    store = Store(Buckets.local(tmp_path))
    for key in ("index/aa/old.jsonl", "index/bb/current.jsonl", "index/ns/coll.jsonl"):
        store.index.write(key, b"x\n")
    assert store.prune_due()

    start = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
    monkeypatch.setattr(store_mod, "now", lambda: start.isoformat())
    assert store.prune_index_files({"index/bb/current.jsonl"}) == 0
    assert set(store.state["retired_index_files"]) == {"index/aa/old.jsonl", "index/ns/coll.jsonl"}
    assert not store.prune_due()

    # Kept until no listing has named them for RETIRED_GRACE.
    monkeypatch.setattr(store_mod, "now", lambda: (start + RETIRED_GRACE - datetime.timedelta(minutes=1)).isoformat())
    assert store.prune_index_files({"index/bb/current.jsonl"}) == 0
    monkeypatch.setattr(store_mod, "now", lambda: (start + RETIRED_GRACE).isoformat())
    assert store.prune_index_files({"index/bb/current.jsonl"}) == 2
    assert "retired_index_files" not in store.state
    assert list(store.index.list("index/")) == ["index/bb/current.jsonl"]


def test_prune_forgets_a_file_named_again(tmp_path):
    store = Store(Buckets.local(tmp_path))
    store.index.write("index/aa/first.jsonl", b"x\n")
    store.index.write("index/bb/second.jsonl", b"y\n")
    _ = store.prune_index_files({"index/bb/second.jsonl"})
    assert set(store.state["retired_index_files"]) == {"index/aa/first.jsonl"}

    # Unyanked: the first file is named again, so it's no longer retired.
    _ = store.prune_index_files({"index/aa/first.jsonl"})
    assert "index/aa/first.jsonl" not in store.state.get("retired_index_files", {})


def test_failed_collections_skip_extraction_failures(tmp_path):
    store = Store(Buckets.local(tmp_path))
    store.record_failure("ns.extract", "1.0.0", "extract_error", "bad manifest")
    store.record_failure("ns.both", "1.0.0", "extract_error", "bad manifest")
    store.record_failure("ns.both", "1.1.0", "galaxy_unavailable", "503")
    store.record_failure(store_mod.GLOBAL, "feed", "galaxy_unavailable", "503")
    # Reconciling again can't clear an extraction failure, so only the other collection is retried.
    assert store.failed_collections() == [("ns", "both")]


def test_keys_from_older_migrations_are_dropped(tmp_path):
    old = {
        "collections": {},
        "conflicts": [],
        "failures": {},
        "pending": {},
        "feed_cursor": "a",
        "migration": {"done": True},
    }
    Store(Buckets.local(tmp_path)).state_bucket.write("state.json", json.dumps(old).encode())
    assert Store(Buckets.local(tmp_path)).state == {
        "collections": {},
        "conflicts": [],
        "failures": {},
        "pending": {},
        "feed_cursor": "a",
    }
