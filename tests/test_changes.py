"""Finding what changed: discover, sweep, the version count and targets."""

import urllib.parse

import pytest
from helpers import COUNT, FEED, LISTING, REPO_METADATA, UNAVAILABLE, artifact, feed_row

from galaxy_sync.changes import check_count, discover, resolve_targets, sweep
from galaxy_sync.galaxy import GalaxyError
from galaxy_sync.reconcile import reconcile


def test_resolve_targets(galaxy, client):
    query = urllib.parse.urlencode({"limit": "100", "namespace": "ns"})
    galaxy.json(
        f"/api/v3/plugin/ansible/content/published/collections/index/?{query}",
        {"data": [{"namespace": "ns", "name": "a"}], "links": {"next": "/page2"}},
    )
    galaxy.json("/page2", {"data": [{"namespace": "ns", "name": "b"}], "links": {"next": None}})
    assert resolve_targets(client, ["ns", "other.coll"]) == [("ns", "a"), ("ns", "b"), ("other", "coll")]
    with pytest.raises(ValueError, match="invalid collection name"):
        resolve_targets(client, ["Bad.name"])


def test_discover_first_run_lists_everything(galaxy, client, store):
    galaxy.add_collection("ns", "a", ["1.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0"])
    res = discover(client, store)
    assert res.collections == [("ns", "a"), ("ns", "b")]
    assert (res.feed, res.listed, res.ok) == (True, True, True)
    assert res.cursor == galaxy.uploaded[("ns", "b", "1.0.0")]


def mirror(galaxy, client, store, *names):
    """Reconcile collections and record the feed cursor, as an applied discover does."""
    for name in names:
        reconcile(client, store, "ns", name)
    store.state["feed_cursor"] = max(galaxy.uploaded.values())


def test_discover_feed_finds_releases_below_the_highest(galaxy, client, store):
    galaxy.add_collection("ns", "a", ["2.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0"])
    mirror(galaxy, client, store, "a", "b")
    res = discover(client, store)
    assert (res.collections, res.listed) == ([("ns", "b")], False)  # the cursor's own entry again

    # A backport leaves updated_at (the highest version's timestamp) alone,
    # so only the feed sees it.
    galaxy.add_collection("ns", "a", ["2.0.0", "1.5.0"])
    galaxy.requests.clear()
    res = discover(client, store)
    assert res.collections == [("ns", "a"), ("ns", "b")]  # back to the cursor, inclusive
    assert res.cursor == galaxy.uploaded[("ns", "a", "1.5.0")]
    assert not any("collections/index/" in r for r in galaxy.requests)


def test_discover_feed_stops_at_the_cursor(galaxy, client, store):
    galaxy.json(
        FEED,
        {
            "meta": {"count": 3},
            "data": [
                feed_row("ns", "new", "1.0.0", "2026-03-02T00:00:00Z"),
                feed_row("NS", "bad", "1.0.0", "2026-03-01T12:00:00Z"),
            ],
            "links": {"next": "/feed2"},
        },
    )
    galaxy.json(
        "/feed2",
        {
            "meta": {"count": 3},
            "data": [feed_row("ns", "old", "1.0.0", "2026-02-01T00:00:00Z")],
            "links": {"next": "/feed3"},
        },
    )
    # The overlap re-reads uploads up to an hour before the cursor.
    store.state["feed_cursor"] = "2026-03-01T12:30:00Z"
    res = discover(client, store)
    assert (res.collections, res.cursor) == ([("ns", "new")], "2026-03-02T00:00:00Z")
    store.state["feed_cursor"] = "2026-03-01T00:00:00Z"
    assert "/feed3" not in galaxy.requests
    assert "/feed3" not in galaxy.requests

    # Nothing newer, and the cursor's own version is gone: the cursor stays.
    store.state["feed_cursor"] = "2026-04-01T00:00:00Z"
    res = discover(client, store)
    assert (res.collections, res.cursor) == ([], "2026-04-01T00:00:00Z")


def test_discover_listing_finds_what_the_feed_cannot(galaxy, client, store):
    galaxy.add_collection("ns", "a", ["1.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0"])
    galaxy.add_collection("ns", "c", ["1.0.0", "2.0.0"])
    gated(galaxy, client, store, "a", "b", "c")

    # Deprecation, a deleted collection and a deleted highest version don't
    # reach the feed. The repo changed without an upload, so the listing runs.
    galaxy.add_collection("ns", "a", ["1.0.0"], deprecated=True)
    galaxy.remove_collection("ns", "b")
    galaxy.add_collection("ns", "c", ["1.0.0"], updated="2026-02-01T00:00:00Z")
    res = discover(client, store)
    assert res.collections == [("ns", "a"), ("ns", "b"), ("ns", "c")]
    assert (res.feed, res.listed, res.hinted) == (True, True, True)

    # Once the 404 has yanked it, a deleted collection isn't checked again.
    reconcile(client, store, "ns", "b")
    del store.state["feed_cursor"]  # the first run always lists
    assert ("ns", "b") not in discover(client, store).collections


def test_discover_falls_back_to_the_listing(galaxy, client, store):
    galaxy.add_collection("ns", "a", ["1.0.0"])
    mirror(galaxy, client, store, "a")
    galaxy.add_collection("ns", "new", ["1.0.0"])
    galaxy.routes[FEED] = UNAVAILABLE
    res = discover(client, store)
    assert (res.collections, res.feed, res.listed, res.cursor) == ([("ns", "new")], False, True, None)
    assert [(f["collection"], f["version"]) for f in store.state["failures"].values()] == [("*", "feed")]
    assert store.failed_collections() == []

    galaxy.routes[LISTING] = UNAVAILABLE
    res = discover(client, store)
    assert not res.ok
    assert sorted(f["version"] for f in store.state["failures"].values()) == ["feed", "listing"]

    galaxy.add_collection("ns", "new", ["1.0.0"])  # serves both again
    del store.state["feed_cursor"]  # the first run always lists
    assert discover(client, store).ok
    assert store.state["failures"] == {}


def gated(galaxy, client, store, *names):
    """Mirror, and record the repo metadata as an applied discover does."""
    mirror(galaxy, client, store, *names)
    store.state["repo_published"] = client.repo_published()


def test_discover_skips_an_unchanged_repo(galaxy, client, store):
    galaxy.add_collection("ns", "a", ["1.0.0"])
    gated(galaxy, client, store, "a")
    galaxy.requests.clear()
    res = discover(client, store)
    assert (res.unchanged, res.ok, res.feed, res.listed, res.collections) == (True, True, False, False, [])
    assert res.cursor == store.state["feed_cursor"]
    assert galaxy.requests == [REPO_METADATA]

    # A missing cursor always looks.
    del store.state["feed_cursor"]
    assert not discover(client, store).unchanged


def test_discover_lists_when_the_repo_changed_without_an_upload(galaxy, client, store):
    galaxy.add_collection("ns", "a", ["1.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0"])
    gated(galaxy, client, store, "a", "b")

    galaxy.add_collection("ns", "a", ["1.0.0"], deprecated=True)
    res = discover(client, store)
    assert (res.hinted, res.feed, res.listed) == (True, True, True)
    assert ("ns", "a") in res.collections
    assert res.published == client.repo_published()

    # An upload explains the change, so no listing.
    galaxy.add_collection("ns", "b", ["1.0.0", "1.1.0"])
    res = discover(client, store)
    assert (res.hinted, res.listed) == (False, False)


def test_discover_repo_metadata_failures(galaxy, client, store):
    galaxy.add_collection("ns", "a", ["1.0.0"])
    gated(galaxy, client, store, "a")

    # Without the metadata, look anyway.
    galaxy.routes[REPO_METADATA] = UNAVAILABLE
    res = discover(client, store)
    assert (res.unchanged, res.feed, res.published) == (False, True, None)
    assert [(f["version"], f["kind"]) for f in store.state["failures"].values()] == [
        ("published", "galaxy_unavailable")
    ]

    # A failed feed or required listing means the next run mustn't skip.
    galaxy.touch()
    galaxy.routes[FEED] = UNAVAILABLE
    res = discover(client, store)
    assert (res.listed, res.published) == (True, None)
    galaxy.add_collection("ns", "a", ["1.0.0"], deprecated=True)  # serves the feed again
    galaxy.routes[LISTING] = UNAVAILABLE
    res = discover(client, store)
    assert (res.hinted, res.listed, res.published) == (True, False, None)
    assert {f["version"] for f in store.state["failures"].values()} == {"listing"}


def test_discover_skips_invalid_names(galaxy, client, store):
    galaxy.json(LISTING, {"data": [{"namespace": "NS", "name": "x"}], "links": {"next": None}})
    galaxy.json(FEED, {"meta": {"count": 1}, "data": [feed_row("NS", "x", "1.0.0", "2026-03-01T00:00:00Z")]})
    assert discover(client, store).collections == []


def test_check_count(galaxy, client, store):
    galaxy.add_collection("ns", "a", ["1.0.0", "1.1.0", "2.0.0"])
    galaxy.add_collection("ns", "gone", ["1.0.0"])
    mirror(galaxy, client, store, "a", "gone")
    assert check_count(client, store) == 0

    # Deleting a version below the highest changes nothing tier 2 can see.
    galaxy.add_collection("ns", "a", ["1.0.0", "2.0.0"])
    galaxy.remove_collection("ns", "gone")
    assert check_count(client, store) == 2
    (failure,) = store.state["failures"].values()
    assert (failure["collection"], failure["version"], failure["kind"]) == ("*", "count", "deletions_suspected")
    assert check_count(client, store) == 2
    assert next(iter(store.state["failures"].values()))["attempts"] == 2

    galaxy.routes[COUNT] = UNAVAILABLE
    assert check_count(client, store) is None
    assert {f["kind"] for f in store.state["failures"].values()} == {"deletions_suspected", "galaxy_unavailable"}

    # A refresh yanks them and the counts match again.
    galaxy.add_collection("ns", "a", ["1.0.0", "2.0.0"])
    galaxy.remove_collection("ns", "gone")
    for name in ("a", "gone"):
        reconcile(client, store, "ns", name)
    assert check_count(client, store) == 0
    assert store.state["failures"] == {}


def test_sweep(galaxy, client, store):
    galaxy.add_collection("ns", "a", ["1.0.0", "1.1.0", "2.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0"])
    galaxy.add_collection("ns", "c", ["1.0.0"])
    for name in ("a", "b", "c"):
        reconcile(client, store, "ns", name)
    res = sweep(client, store)
    assert (res.gone, res.unmirrored, res.published, res.collections) == ({}, {}, 5, [])

    # A deletion below the highest, a deleted collection, a missed upload and
    # a collection never seen.
    galaxy.add_collection("ns", "a", ["1.0.0", "2.0.0"])
    galaxy.remove_collection("ns", "c")
    galaxy.add_collection("ns", "b", ["1.0.0", "1.1.0"])
    galaxy.add_collection("ns", "d", ["1.0.0"])
    res = sweep(client, store)
    assert res.gone == {"ns.a": ["1.1.0"], "ns.c": ["1.0.0"]}
    assert res.unmirrored == {"ns.b": ["1.1.0"], "ns.d": ["1.0.0"]}
    assert res.collections == [("ns", "a"), ("ns", "b"), ("ns", "c"), ("ns", "d")]

    for ns, name in res.collections:
        reconcile(client, store, ns, name)
    assert sweep(client, store).collections == []

    # A yanked version back on Galaxy needs a reconcile to un-yank it.
    galaxy.add_collection("ns", "a", ["1.0.0", "1.1.0", "2.0.0"])
    assert sweep(client, store).unmirrored == {"ns.a": ["1.1.0"]}


def test_sweep_needs_every_page(galaxy, client, store):
    galaxy.json(
        FEED,
        {
            "meta": {"count": 2},
            "data": [feed_row("NS", "bad", "1.0.0", "2026-03-01T00:00:00Z")],
            "links": {"next": "/p2"},
        },
    )
    galaxy.routes["/p2"] = UNAVAILABLE
    with pytest.raises(GalaxyError):
        sweep(client, store)


def test_sweep_finds_replaced_versions(galaxy, client, store):
    galaxy.add_collection("ns", "a", ["1.0.0", "2.0.0"])
    reconcile(client, store, "ns", "a")
    assert sweep(client, store).replaced == {}

    # Replacing a version below the highest doesn't change the collection's
    # updated_at, but its upload time no longer matches.
    original_upload = galaxy.uploaded[("ns", "a", "1.0.0")]
    original = galaxy.blobs[("ns", "a", "1.0.0")]
    galaxy.replace("ns", "a", "1.0.0", artifact("ns", "a", "1.0.0", mtime=1_800_000_000))
    res = sweep(client, store)
    assert (res.replaced, res.collections) == ({"ns.a": ["1.0.0"]}, [("ns", "a")])
    assert reconcile(client, store, "ns", "a").repacked == ["1.0.0"]
    assert sweep(client, store).replaced == {}

    # Restoring the original brings back its old upload time: still a change.
    galaxy.replace("ns", "a", "1.0.0", original, uploaded=original_upload)
    assert sweep(client, store).replaced == {"ns.a": ["1.0.0"]}
