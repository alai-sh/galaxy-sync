"""reconcile(): mirroring through alai-index, yanking, conflicts and failure handling for one collection."""

import hashlib
import json
import logging
from compression import zstd

import pytest
from helpers import DETAIL, artifact

from galaxy_sync import reconcile as reconcile_mod
from galaxy_sync import report
from galaxy_sync.reconcile import ReconcileFailed, reconcile
from galaxy_sync.report import Summary
from galaxy_sync.storage import StorageError
from galaxy_sync.store import Store
from galaxy_sync.times import rfc3339
from galaxy_sync.tree import Tree

# A status alai-index doesn't retry, as its backoff can't be shortened from its command line.
FORBIDDEN = (403, {"detail": "Forbidden"}, {})


def later(store, tree, tmp_path):
    """The store and tree a later run would open, once this one published what it changed."""
    _ = tree.publish_aggregates()
    store.save_state()
    n = len(list(tmp_path.glob("later*")))
    return Store(store.buckets), Tree(store.buckets, tmp_path / f"later{n}")


def run(client, store, tree, name="coll", **kwargs):
    return reconcile(client, store, tree, "ns", name, **kwargs)


def test_new_versions_are_mirrored(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])

    res = run(client, store, tree)
    assert res.added == ["1.0.0", "1.1.0"]
    assert res.downloaded == sum(len(galaxy.blobs[("ns", "coll", v)]) for v in ("1.0.0", "1.1.0"))
    first, _ = tree.lines("ns.coll")
    blob = galaxy.blobs[("ns", "coll", "1.0.0")]
    # From the artifact, not from Galaxy's API.
    assert json.loads(first.to_json()) == {
        "v": 1,
        "name": "ns.coll",
        "vers": "1.0.0",
        "deps": [],
        "sha256": hashlib.sha256(blob).hexdigest(),
        "size": len(blob),
        "yanked": False,
        "published": rfc3339(galaxy.uploaded[("ns", "coll", "1.0.0")]),
    }
    assert tree.read_artifact(first) == blob
    # Published as it was written: the index file, and the private state compressed.
    assert store.index.read(tree.index_file("ns.coll")) is not None
    plugins = zstd.decompress(store.state_bucket.read("plugin-cache/ns/coll.jsonl.zst") or b"")
    assert next(json.loads(line)["plugins"] for line in plugins.splitlines()) == [
        {"type": "module", "name": "thing", "short_description": "A thing"}
    ]
    # plugins.jsonl and collections.json wait for the run to publish them.
    assert "coll" not in (store.index.read("collections.json") or b"").decode()
    assert tree.publish_aggregates()
    assert "ns.coll" in (store.index.read("plugins.jsonl") or b"").decode()
    assert store.collection_state("ns.coll") | {"upstream_updated": None} == {
        "upstream_updated": None,
        "upstream_deprecated": False,
        "upstream_count": 2,
        "mirrored": 2,
    }

    # A no-op re-run downloads nothing, and writes nothing.
    galaxy.requests.clear()
    listing = dict(tree.listing)
    assert run(client, store, tree).added == []
    assert tree.listing == listing
    assert not any("/pulp/content/" in r for r in galaxy.requests)


def test_alai_index_progress_is_logged(galaxy, client, store, tree, caplog):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    caplog.set_level(logging.INFO, logger="galaxy_sync")
    run(client, store, tree)
    progress = [r.getMessage() for r in caplog.records if r.getMessage().startswith("alai-index: ")]
    assert "alai-index: downloading ns.coll 1.0.0, 1 of 1" in progress


def test_a_later_run_carries_on_from_the_buckets(galaxy, client, store, tree, tmp_path):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    run(client, store, tree)
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])

    store, tree = later(store, tree, tmp_path)
    assert run(client, store, tree).added == ["1.1.0"]
    assert [line.version.text for line in tree.lines("ns.coll")] == ["1.0.0", "1.1.0"]


def test_failed_version_is_recorded_skipped_and_retried(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    good = galaxy.routes[DETAIL.format("1.1.0")]
    galaxy.routes[DETAIL.format("1.1.0")] = FORBIDDEN

    res = run(client, store, tree)
    assert res.added == ["1.0.0"]
    assert res.upstream == 2  # still listed by Galaxy
    assert [line.version.text for line in tree.lines("ns.coll")] == ["1.0.0"]
    (failure,) = store.state["failures"].values()
    assert (failure["version"], failure["kind"], failure["attempts"]) == ("1.1.0", "galaxy_http_403", 1)
    assert store.failed_collections() == [("ns", "coll")]

    galaxy.routes[DETAIL.format("1.1.0")] = good
    res = run(client, store, tree)
    assert res.added == ["1.1.0"]
    assert store.state["failures"] == {}


def test_extraction_failure_is_recorded_but_still_mirrors(galaxy, client, store, tree, monkeypatch):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    real = tree.run

    def failing_extraction(command, *args):
        out = real(command, *args)
        for change in out["versions"]:
            if change["result"] == "added":
                change["details"] = {**(change["details"] or {}), "error": "ValueError: parser bug"}
        return out

    monkeypatch.setattr(tree, "run", failing_extraction)
    res = run(client, store, tree)
    assert res.added == ["1.0.0"]
    (failure,) = store.state["failures"].values()
    assert (failure["version"], failure["kind"], failure["message"]) == (
        "1.0.0",
        "extract_error",
        "ValueError: parser bug",
    )

    # A later reconcile doesn't clear it, as only a new extractor heals one.
    monkeypatch.undo()
    run(client, store, tree)
    assert [f["kind"] for f in store.state["failures"].values()] == ["extract_error"]


def test_failure_report_counts(galaxy, client, store, tree, tmp_path):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    good = galaxy.routes[DETAIL.format("1.0.0")]
    galaxy.routes[DETAIL.format("1.0.0")] = FORBIDDEN
    run(client, store, tree)
    report = store.failure_report()
    assert report.summary == Summary(outstanding=1, new=1, resolved=0, conflicts=0)
    assert "| ns.coll | 1.0.0 | galaxy_http_403 | 20" in report.markdown

    store, tree = later(store, tree, tmp_path)
    galaxy.routes[DETAIL.format("1.0.0")] = good
    run(client, store, tree)
    assert store.failure_report().summary == Summary(outstanding=0, new=0, resolved=1, conflicts=0)


@pytest.mark.parametrize(
    ("kind", "breaks"),
    [
        ("galaxy_http_404", lambda g: g.routes.pop("/pulp/content/ns-coll-1.0.0.tar.gz?sig=x")),
        ("bad_artifact", lambda g: g.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": b"not a tarball"})),
        (
            "manifest_mismatch",
            lambda g: g.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": artifact("ns", "other", "1.0.0")}),
        ),
        ("sha256_mismatch", lambda g: g.routes[DETAIL.format("1.0.0")][1]["artifact"].update(sha256="0" * 64)),
    ],
)
def test_a_version_galaxy_serves_badly_is_recorded_not_mirrored(galaxy, client, store, tree, kind, breaks):
    # Each is a kind report.SECTIONS lists, so a kind alai-index renames fails here.
    assert report._section(kind) != "Other failures"
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    breaks(galaxy)
    assert run(client, store, tree).added == []
    (failure,) = store.state["failures"].values()
    assert (failure["version"], failure["kind"]) == ("1.0.0", kind)
    assert not list(store.buckets.dl.list(""))


def test_deleted_versions_are_yanked_and_restored(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    run(client, store, tree)

    galaxy.add_collection("ns", "coll", ["1.0.0"])
    res = run(client, store, tree)
    assert (res.yanked, res.upstream) == (["1.1.0"], 1)
    assert [line.yanked for line in tree.lines("ns.coll")] == [False, True]

    # Back with the same bytes: un-yanked, not downloaded again.
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    galaxy.requests.clear()
    res = run(client, store, tree)
    assert (res.unyanked, res.added) == (["1.1.0"], [])
    assert not any("/pulp/content/" in r for r in galaxy.requests)
    assert [line.yanked for line in tree.lines("ns.coll")] == [False, False]


def test_a_version_missing_from_the_listing_is_checked_before_yanking(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    run(client, store, tree)
    listing = "/api/v3/collections/ns/coll/versions/?limit=100"
    rows = galaxy.routes[listing][1]["data"]

    # Paging shifted 1.1.0 past us, but its own page is still there: kept.
    galaxy.json(listing, {"data": [r for r in rows if r["version"] == "1.0.0"], "links": {"next": None}})
    assert run(client, store, tree).yanked == []

    # Galaxy can't say: kept, and the check is retried.
    galaxy.routes[DETAIL.format("1.1.0")] = FORBIDDEN
    res = run(client, store, tree)
    assert (res.yanked, len(res.errors)) == ([], 1)
    assert [e["version"] for e in store.state["failures"].values()] == ["1.1.0"]

    # Deleted: its page is a 404.
    del galaxy.routes[DETAIL.format("1.1.0")]
    assert run(client, store, tree).yanked == ["1.1.0"]
    assert not store.state["failures"]


def test_deleted_collection_is_yanked(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    run(client, store, tree)
    store.record_failure("ns.coll", None, "galaxy_unavailable", "earlier")

    galaxy.remove_collection("ns", "coll")
    res = run(client, store, tree)
    assert (res.yanked, res.upstream) == (["1.0.0"], 0)
    assert tree.lines("ns.coll")[0].yanked
    assert store.state["failures"] == {}
    assert "upstream_updated" not in store.collection_state("ns.coll")

    # Never mirrored: nothing to write.
    assert run(client, store, tree, "never").added == []
    assert "ns.never" not in tree.listing


def test_a_collection_that_cant_be_listed_fails_whole(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    galaxy.routes["/api/v3/collections/ns/coll/versions/?limit=100"] = FORBIDDEN
    with pytest.raises(ReconcileFailed) as e:
        run(client, store, tree)
    assert e.value.kind == "galaxy_http_403"


def test_repacked_artifact_is_a_conflict(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    run(client, store, tree)
    (original,) = tree.lines("ns.coll")

    # The same files rebuilt, with new bytes and a new upload time. Ours is kept and Galaxy's is recorded.
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", mtime=1_800_000_000))
    res = run(client, store, tree)
    assert res.conflicts == ["1.0.0"]
    assert tree.lines("ns.coll") == (original,)
    (conflict,) = store.open_conflicts()
    assert conflict["observed"][0]["changes"] == "none, the same files packed again"
    assert store.state["failures"] == {}

    # Downloaded once, even when revalidating.
    galaxy.requests.clear()
    assert run(client, store, tree, revalidate=True).conflicts == []
    assert not any("/pulp/content/" in r for r in galaxy.requests)


def test_changed_contents_are_a_conflict(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    run(client, store, tree)
    (original,) = tree.lines("ns.coll")
    first_upload = galaxy.uploaded[("ns", "coll", "1.0.0")]
    first_blob = galaxy.blobs[("ns", "coll", "1.0.0")]

    changed = artifact("ns", "coll", "1.0.0", modules=("other",))
    galaxy.replace("ns", "coll", "1.0.0", changed)
    res = run(client, store, tree)
    assert res.conflicts == ["1.0.0"]
    assert tree.lines("ns.coll") == (original,)  # ours is kept, and the index doesn't change
    (conflict,) = store.open_conflicts()
    upstream = hashlib.sha256(changed).hexdigest()
    (observed,) = conflict["observed"]
    assert observed["sha256"] == upstream
    assert observed["changes"] == (
        "added plugins/modules/other.py; removed plugins/modules/thing.py; changed FILES.json, MANIFEST.json"
    )
    assert store.state_bucket.read(observed["saved"]) == changed
    assert conflict["opened"] == observed["seen"]
    assert store.state["failures"] == {}  # reported as a conflict, not retried as a failure
    report = store.failure_report()
    assert "### Artifact conflicts" in report.markdown
    assert f"| ns.coll | 1.0.0 | {original.sha256[:12]} | {upstream[:12]} |" in report.markdown
    assert observed["saved"] not in report.markdown  # the state bucket key stays private
    assert report.summary.alerts.conflicts == [conflict]

    # Seen again: still one open conflict, and no new download.
    galaxy.requests.clear()
    assert run(client, store, tree, revalidate=True).conflicts == []
    assert len(store.state["conflicts"]) == 1
    assert not any("/pulp/content/" in r for r in galaxy.requests)

    # Different bytes again before it's resolved: added to the open conflict.
    again = artifact("ns", "coll", "1.0.0", modules=("third",))
    galaxy.replace("ns", "coll", "1.0.0", again)
    assert run(client, store, tree).conflicts == ["1.0.0"]
    (updated,) = store.state["conflicts"]
    first, latest = updated["observed"]
    assert first == observed
    assert latest["sha256"] == hashlib.sha256(again).hexdigest()
    assert store.state_bucket.read(latest["saved"]) == again
    assert updated["opened"] == conflict["opened"]

    # Back to a build already observed: not repeated.
    galaxy.replace("ns", "coll", "1.0.0", changed)
    assert run(client, store, tree).conflicts == []
    assert [o["sha256"] for o in store.state["conflicts"][0]["observed"]] == [upstream, latest["sha256"]]

    # Galaxy serves our artifact again, with its old upload time, so galaxy-mirror has no reason to check it.
    galaxy.replace("ns", "coll", "1.0.0", first_blob, uploaded=first_upload)
    res = run(client, store, tree)
    assert (res.resolved, res.conflicts) == (["1.0.0"], [])
    assert store.open_conflicts() == []
    assert "resolved" in store.state["conflicts"][0]


def test_conflict_comparison_failures(galaxy, client, store, tree, monkeypatch):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    run(client, store, tree)
    (original,) = tree.lines("ns.coll")
    filename = "ns-coll-1.0.0.tar.gz"

    # A failed download is retried on the next run.
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", modules=("other",)))
    good = galaxy.routes.pop(f"/pulp/content/{filename}?sig=x")
    run(client, store, tree)
    (failure,) = store.state["failures"].values()
    assert failure["kind"] == "galaxy_http_404"
    assert failure["message"].startswith("download for the conflict: ")
    galaxy.routes[f"/pulp/content/{filename}?sig=x"] = good
    assert run(client, store, tree).conflicts == ["1.0.0"]

    # Bytes that don't match Galaxy's sha256.
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", modules=("third",)))
    galaxy.routes[f"/pulp/content/{filename}?sig=x"] = (200, b"tampered", {})
    run(client, store, tree)
    assert {f["kind"] for f in store.state["failures"].values()} == {"sha256_mismatch"}

    # Unreadable on Galaxy's side, or our artifact missing: still an open conflict.
    galaxy.replace("ns", "coll", "1.0.0", b"not a tarball")
    run(client, store, tree)
    assert store.open_conflicts()[-1]["observed"][-1]["changes"].startswith("Galaxy's artifact can't be read")
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", modules=("fourth",)))
    monkeypatch.setattr(tree, "read_artifact", lambda line: None)
    run(client, store, tree)
    assert (
        store.open_conflicts()[-1]["observed"][-1]["changes"] == f"the mirror's artifact {original.sha256} is missing"
    )
    monkeypatch.setattr(tree, "read_artifact", lambda line: b"corrupt")
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", modules=("fifth",)))
    run(client, store, tree)
    assert store.open_conflicts()[-1]["observed"][-1]["changes"].startswith("the mirror's artifact can't be read")


def test_old_version_failures_are_cleared(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    run(client, store, tree)
    store.record_failure("ns.coll", "1.0.0", "galaxy_unavailable", "checking it earlier")
    store.record_failure("ns.coll", "1.0.0", "extract_error", "only heals with a new extractor")
    run(client, store, tree, revalidate=True)
    assert [f["kind"] for f in store.state["failures"].values()] == ["extract_error"]


def test_revalidate_with_unchanged_bytes(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    run(client, store, tree)
    res = run(client, store, tree, revalidate=True)
    assert (res.conflicts, res.added) == ([], [])


def test_deprecation_follows_galaxy(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0"], deprecated=True)
    assert run(client, store, tree).deprecated is True
    assert tree.lines("ns.coll")[0].deprecated is True

    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert run(client, store, tree).deprecated is False
    assert not tree.lines("ns.coll")[0].deprecated


def test_failures_for_versions_gone_from_galaxy_are_cleared(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    galaxy.routes[DETAIL.format("1.1.0")] = FORBIDDEN
    run(client, store, tree)
    assert len(store.state["failures"]) == 1

    galaxy.add_collection("ns", "coll", ["1.0.0"])
    run(client, store, tree)
    assert store.state["failures"] == {}


@pytest.mark.parametrize("name", [("ns", "../x"), ("NS", "coll"), ("ns", "a/b"), ("", "coll")])
def test_invalid_names_are_rejected(client, store, tree, name):
    with pytest.raises(ValueError, match="invalid collection name"):
        reconcile(client, store, tree, *name)


def test_deadline_defers_new_versions(galaxy, client, store, tree, monkeypatch):
    galaxy.add_collection("ns", "coll", ["2.0.0", "1.0.0", "1.1.0"])
    monkeypatch.setattr(reconcile_mod, "CHUNK", 1)

    # Out of time after the first chunk: what's mirrored is published, oldest upload first.
    res = run(client, store, tree, deadline=0)
    assert (res.added, res.deferred) == (["2.0.0"], ["1.0.0", "1.1.0"])
    assert [line.version.text for line in tree.lines("ns.coll")] == ["2.0.0"]
    assert res.upstream == 3
    assert "upstream_updated" not in store.collection_state("ns.coll")

    res = run(client, store, tree)
    assert (res.added, res.deferred) == (["1.0.0", "1.1.0"], [])
    assert [line.version.text for line in tree.lines("ns.coll")] == ["2.0.0", "1.0.0", "1.1.0"]
    assert store.collection_state("ns.coll")["upstream_updated"]


def test_a_run_that_stops_mirrors_again(galaxy, client, store, tree, tmp_path, monkeypatch):
    versions = ["1.0.0", "1.1.0", "1.2.0", "1.3.0", "1.4.0"]
    galaxy.add_collection("ns", "coll", versions)
    monkeypatch.setattr(reconcile_mod, "CHUNK", 2)
    publish = tree.publish
    calls = []

    def stopping(out):
        calls.append(out)
        if len(calls) == 2:
            raise KeyboardInterrupt  # the run stops
        publish(out)

    monkeypatch.setattr(tree, "publish", stopping)
    with pytest.raises(KeyboardInterrupt):
        run(client, store, tree)
    monkeypatch.undo()

    # collections.json wasn't published, so the next run mirrors the collection again, from its first version.
    store, tree = Store(store.buckets), Tree(store.buckets, tmp_path / "later")
    assert run(client, store, tree).added == versions
    assert [line.version.text for line in tree.lines("ns.coll")] == versions


def test_out_of_date_plugins_are_extracted_again(galaxy, client, store, tree, tmp_path):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    run(client, store, tree)
    store, tree = later(store, tree, tmp_path)
    store.state_bucket.delete("plugin-cache/ns/coll.jsonl.zst")

    # From the artifacts in the dl bucket, as the collection is reconciled.
    res = run(client, store, tree)
    assert (res.added, res.reextracted) == ([], ["1.0.0", "1.1.0"])
    assert store.state_bucket.read("plugin-cache/ns/coll.jsonl.zst") is not None
    assert not (tree.dl).exists()

    # One whose artifact is gone from the dl bucket is a failure.
    store, tree = later(store, tree, tmp_path)
    store.state_bucket.delete("plugin-cache/ns/coll.jsonl.zst")
    (line, _) = tree.lines("ns.coll")
    store.buckets.dl.delete(tree.artifact_key(line))
    res = run(client, store, tree)
    assert res.reextracted == ["1.1.0"]
    assert [(f["version"], f["kind"]) for f in store.state["failures"].values()] == [("1.0.0", "artifact_missing")]

    # The next reconcile downloads it from Galaxy again, then extracts it.
    res = run(client, store, tree)
    assert (res.restored, res.reextracted) == (["1.0.0"], ["1.0.0"])
    assert store.state["failures"] == {}


def test_missing_artifacts_are_restored(galaxy, client, store, tree, tmp_path):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    run(client, store, tree)
    store, tree = later(store, tree, tmp_path)
    line, _ = tree.lines("ns.coll")
    key = tree.artifact_key(line)
    line_blob = galaxy.blobs[("ns", "coll", "1.0.0")]

    def missing():
        store.buckets.dl.delete(key)
        assert reconcile_mod.generate_indexes(store, tree, "ns.coll") == []
        return [(f["version"], f["kind"]) for f in store.state["failures"].values()]

    # Galaxy still serves the same bytes: downloaded again, as the next reconcile finds it recorded.
    assert missing() == [("1.0.0", "artifact_missing")]
    assert run(client, store, tree).restored == ["1.0.0"]
    assert store.buckets.dl.read(key) == galaxy.blobs[("ns", "coll", "1.0.0")]
    assert store.state["failures"] == {}

    # Other bytes are never written for it: lost, which reconciling again doesn't clear, and isn't retried on its own.
    assert missing() == [("1.0.0", "artifact_missing")]
    first_upload = galaxy.uploaded[("ns", "coll", "1.0.0")]
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", modules=("other",)))
    for _ in range(2):
        assert run(client, store, tree).restored == []
        assert not store.buckets.dl.exists(key)
        (failure,) = store.state["failures"].values()
        assert failure["kind"] == "artifact_lost"
        assert "Galaxy now serves other bytes" in failure["message"]
    assert store.failed_collections() == []

    # A reconcile for something else still tries it, and puts it back once Galaxy serves ours again.
    galaxy.replace("ns", "coll", "1.0.0", line_blob, uploaded=first_upload)
    assert run(client, store, tree).restored == ["1.0.0"]
    assert store.state["failures"] == {}

    # Gone from Galaxy: nothing to download it from.
    assert missing() == [("1.0.0", "artifact_missing")]
    del galaxy.routes[DETAIL.format("1.0.0")]
    run(client, store, tree)
    (failure,) = store.state["failures"].values()
    assert (failure["kind"], failure["message"]) == (
        "artifact_lost",
        f"{key} isn't in the dl bucket, and Galaxy no longer has it",
    )


def test_generate_plugins(galaxy, client, store, tree, tmp_path):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    run(client, store, tree)
    store, tree = later(store, tree, tmp_path)

    # By default every version, whatever the cache says. From the cache, only what's out of date.
    assert reconcile_mod.generate_plugins(store, tree, "ns.coll") == ["1.0.0", "1.1.0"]
    assert reconcile_mod.generate_plugins(store, tree, "ns.coll", from_plugin_cache=True) == []
    store.state_bucket.delete("plugin-cache/ns/coll.jsonl.zst")
    store, tree = later(store, tree, tmp_path)
    assert reconcile_mod.generate_plugins(store, tree, "ns.coll", from_plugin_cache=True) == ["1.0.0", "1.1.0"]
    assert not tree.dl.exists()


def test_generate_plugins_without_every_artifact(galaxy, client, store, tree, tmp_path):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    run(client, store, tree)
    store, tree = later(store, tree, tmp_path)
    (line, _) = tree.lines("ns.coll")
    store.buckets.dl.delete(tree.artifact_key(line))

    # The rest are extracted again, named a version each in a file, and the missing one is a failure.
    assert reconcile_mod.generate_plugins(store, tree, "ns.coll") == ["1.1.0"]
    assert (tree.root.parent / "targets.txt").read_text() == "ns.coll:1.1.0\n"
    assert [(f["version"], f["kind"]) for f in store.state["failures"].values()] == [("1.0.0", "artifact_missing")]


def test_generate_plugins_in_batches(galaxy, client, store, tree, tmp_path, monkeypatch):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0", "1.2.0"])
    run(client, store, tree)
    store, tree = later(store, tree, tmp_path)
    monkeypatch.setattr(reconcile_mod, "BATCH", 2)
    fetched = []
    fetch_artifacts = tree.fetch_artifacts

    def fetch(wanted):
        assert not tree.dl.exists(), "the batch before wasn't deleted"
        fetched.append([entry["version"] for entry in wanted])
        return fetch_artifacts(wanted)

    monkeypatch.setattr(tree, "fetch_artifacts", fetch)
    assert reconcile_mod.generate_plugins(store, tree, "ns.coll") == ["1.0.0", "1.1.0", "1.2.0"]
    assert reconcile_mod.generate_indexes(store, tree, "ns.coll") == []
    assert fetched == [["1.0.0", "1.1.0"], ["1.2.0"]] * 2
    assert not tree.dl.exists()


def test_generate_indexes(galaxy, client, store, tree, tmp_path):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    run(client, store, tree)
    store, tree = later(store, tree, tmp_path)

    # Lines already as the artifacts give them don't change, but are read from the artifacts all the same.
    assert reconcile_mod.generate_indexes(store, tree, "ns.coll") == []
    (line,) = tree.lines("ns.coll")
    store.buckets.dl.delete(tree.artifact_key(line))
    assert reconcile_mod.generate_indexes(store, tree, "ns.coll") == []
    assert [(f["version"], f["kind"]) for f in store.state["failures"].values()] == [("1.0.0", "artifact_missing")]


def test_an_unreadable_index_line_is_never_left_out(galaxy, client, store, tree):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    run(client, store, tree)
    path = tree.root / tree.index_file("ns.coll")
    _ = path.write_bytes(path.read_bytes() + b"not json\n")
    with pytest.raises(StorageError, match="line 2"):
        tree.lines("ns.coll")
