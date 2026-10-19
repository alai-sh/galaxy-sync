"""reconcile(): mirroring, yanking, conflicts and failure handling for one collection."""

import hashlib
import json

import pytest
from helpers import DETAIL, UNAVAILABLE, artifact, extraction

from galaxy_sync import reconcile as reconcile_mod
from galaxy_sync import store as store_mod
from galaxy_sync.plugins import EXTRACTION_FORMAT
from galaxy_sync.reconcile import reconcile
from galaxy_sync.report import Summary
from galaxy_sync.store import Store
from galaxy_sync.times import rfc3339


def reload(store):
    """The store a later run would open."""
    store.save_state()
    return Store(store.buckets, store.dl_url)


def test_new_versions_are_mirrored(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])

    res = reconcile(client, store, "ns", "coll")
    assert res.added == ["1.0.0", "1.1.0"]
    assert res.changed
    first, _ = store.read_index("ns", "coll")
    blob = galaxy.blobs[("ns", "coll", "1.0.0")]
    assert first == {
        "v": 1,
        "name": "ns.coll",
        "vers": "1.0.0",
        "deps": [{"name": "other.coll", "req": ">=1.0.0"}],
        "cksum": hashlib.sha256(blob).hexdigest(),
        "size": len(blob),
        "requires_ansible": ">=2.16",
        "yanked": False,
        "origin": "galaxy",
        "published": rfc3339(galaxy.uploaded[("ns", "coll", "1.0.0")]),
    }
    assert store.read_artifact(first["cksum"]) == blob
    assert extraction(store, first["cksum"])["plugins"] == [
        {"type": "module", "name": "thing", "short_description": "A thing"}
    ]

    # A no-op re-run only lists the collection and its versions.
    galaxy.requests.clear()
    res = reconcile(client, store, "ns", "coll")
    assert (res.added, res.changed) == ([], False)
    assert galaxy.requests == ["/api/v3/collections/ns/coll/", "/api/v3/collections/ns/coll/versions/?limit=100"]


def test_failed_version_is_recorded_skipped_and_retried(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    good = galaxy.routes[DETAIL.format("1.1.0")]
    galaxy.routes[DETAIL.format("1.1.0")] = UNAVAILABLE

    res = reconcile(client, store, "ns", "coll")
    assert res.added == ["1.0.0"]
    assert [line["vers"] for line in store.read_index("ns", "coll")] == ["1.0.0"]
    (failure,) = store.state["failures"].values()
    assert (failure["version"], failure["kind"], failure["attempts"]) == ("1.1.0", "galaxy_unavailable", 1)
    assert store.failed_collections() == [("ns", "coll")]

    galaxy.routes[DETAIL.format("1.1.0")] = good
    res = reconcile(client, store, "ns", "coll")
    assert res.added == ["1.1.0"]
    assert store.state["failures"] == {}


def test_download_errors_are_recorded(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    galaxy.routes.pop("/pulp/content/ns-coll-1.0.0.tar.gz?sig=x")
    res = reconcile(client, store, "ns", "coll")
    assert res.added == []
    (failure,) = store.state["failures"].values()
    assert failure["kind"] == "galaxy_http_404"
    assert failure["message"].startswith("download: ")


def test_extraction_failure_still_mirrors_and_heals(galaxy, client, store, monkeypatch):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    real_extract = reconcile_mod.ext.extract_bytes

    def broken(data):
        raise RuntimeError("parser bug")

    monkeypatch.setattr(reconcile_mod.ext, "extract_bytes", broken)
    res = reconcile(client, store, "ns", "coll")
    assert res.added == ["1.0.0"]  # the artifact is mirrored regardless
    (line,) = store.read_index("ns", "coll")
    assert store.read_artifact(line["cksum"]) is not None
    assert extraction(store, line["cksum"]) is None
    (failure,) = store.state["failures"].values()
    assert failure["kind"] == "extract_error"

    monkeypatch.setattr(reconcile_mod.ext, "extract_bytes", real_extract)
    res = reconcile(client, store, "ns", "coll")
    assert res.reextracted == ["1.0.0"]
    assert extraction(store, line["cksum"])["plugins"] == [
        {"type": "module", "name": "thing", "short_description": "A thing"}
    ]
    assert store.state["failures"] == {}


def test_failure_report_counts(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    good = galaxy.routes[DETAIL.format("1.0.0")]
    galaxy.routes[DETAIL.format("1.0.0")] = UNAVAILABLE
    reconcile(client, store, "ns", "coll")
    report = store.write_failure_report()
    assert report.summary == Summary(outstanding=1, new=1, resolved=0, conflicts=0)
    assert "| ns.coll | 1.0.0 | galaxy_unavailable | 20" in report.markdown
    assert json.loads(store.state_bucket.read("failures.json"))["outstanding"][0]["attempts"] == 1

    later = reload(store)
    galaxy.routes[DETAIL.format("1.0.0")] = good
    reconcile(client, later, "ns", "coll")
    assert later.write_failure_report().summary == Summary(outstanding=0, new=0, resolved=1, conflicts=0)


def test_outdated_extractions_are_rerun(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    reconcile(client, store, "ns", "coll")
    (line,) = store.read_index("ns", "coll")
    current = extraction(store, line["cksum"])
    assert current["format"] == EXTRACTION_FORMAT

    # Up to date: left alone.
    assert reconcile(client, store, "ns", "coll").reextracted == []

    # An older format, or from before results had one.
    for stale in ({**current, "format": 0}, {"extractor": 2, "plugins": [], "routing": {}}):
        store.save_extractions("ns", "coll", {line["cksum"]: stale})
        assert reconcile(client, store, "ns", "coll").reextracted == ["1.0.0"]
        assert extraction(store, line["cksum"]) == current


def test_bad_and_mismatched_artifacts_are_not_mirrored(galaxy, client, store):
    galaxy.add_collection(
        "ns",
        "coll",
        ["1.0.0", "2.0.0"],
        blobs={"1.0.0": b"not a tarball", "2.0.0": artifact("ns", "other", "2.0.0")},
    )
    res = reconcile(client, store, "ns", "coll")
    assert res.added == []
    kinds = sorted(f["kind"] for f in store.state["failures"].values())
    assert kinds == ["bad_artifact", "manifest_mismatch"]


def test_sha256_mismatch_is_not_mirrored(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    galaxy.routes[DETAIL.format("1.0.0")][1]["artifact"]["sha256"] = "0" * 64
    assert reconcile(client, store, "ns", "coll").added == []
    (failure,) = store.state["failures"].values()
    assert failure["kind"] == "sha256_mismatch"
    assert not list(store.dl.list(""))


def test_deleted_versions_are_yanked_and_restored(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    reconcile(client, store, "ns", "coll")

    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert reconcile(client, store, "ns", "coll").yanked == ["1.1.0"]
    assert [line["yanked"] for line in store.read_index("ns", "coll")] == [False, True]

    # Back with the same bytes: un-yanked, not downloaded again.
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    galaxy.requests.clear()
    res = reconcile(client, store, "ns", "coll")
    assert (res.unyanked, res.added) == (["1.1.0"], [])
    assert not any("/pulp/content/" in r for r in galaxy.requests)
    assert [line["yanked"] for line in store.read_index("ns", "coll")] == [False, False]


def test_a_version_missing_from_the_listing_is_checked_before_yanking(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    reconcile(client, store, "ns", "coll")
    listing = "/api/v3/collections/ns/coll/versions/?limit=100"
    rows = galaxy.routes[listing][1]["data"]

    # Paging shifted 1.1.0 past us, but its own page is still there: kept.
    galaxy.json(listing, {"data": [r for r in rows if r["version"] == "1.0.0"], "links": {"next": None}})
    assert reconcile(client, store, "ns", "coll").yanked == []

    # Galaxy can't say: kept, and the check is retried.
    galaxy.routes[DETAIL.format("1.1.0")] = UNAVAILABLE
    res = reconcile(client, store, "ns", "coll")
    assert (res.yanked, len(res.errors)) == ([], 1)
    assert [e["version"] for e in store.state["failures"].values()] == ["1.1.0"]

    # Deleted: its page is a 404.
    del galaxy.routes[DETAIL.format("1.1.0")]
    assert reconcile(client, store, "ns", "coll").yanked == ["1.1.0"]
    assert not store.state["failures"]


def test_deleted_collection_is_yanked(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    reconcile(client, store, "ns", "coll")
    store.record_failure("ns.coll", None, "galaxy_unavailable", "earlier")

    galaxy.remove_collection("ns", "coll")
    res = reconcile(client, store, "ns", "coll")
    assert res.yanked == ["1.0.0"]
    assert store.read_index("ns", "coll")[0]["yanked"]
    assert store.state["failures"] == {}

    # Never mirrored: nothing to write.
    assert not reconcile(client, store, "ns", "never").changed
    assert store.read_index("ns", "never") == []


def test_repacked_artifact_keeps_ours_silently(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    reconcile(client, store, "ns", "coll")
    (original,) = store.read_index("ns", "coll")

    # The same files rebuilt: new bytes and a new upload time.
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", mtime=1_800_000_000))
    res = reconcile(client, store, "ns", "coll")
    assert (res.repacked, res.conflicts) == (["1.0.0"], [])
    (line,) = store.read_index("ns", "coll")
    assert line == original  # same bytes, no flag
    assert store.state["conflicts"] == []
    assert store.state["failures"] == {}

    # Compared once: --revalidate reuses the verdict without downloading again.
    galaxy.requests.clear()
    res = reconcile(client, store, "ns", "coll", revalidate=True)
    assert res.repacked == ["1.0.0"]
    assert not any("/pulp/content/" in r for r in galaxy.requests)


def test_changed_contents_are_a_conflict(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    reconcile(client, store, "ns", "coll")
    (original,) = store.read_index("ns", "coll")
    first_upload = galaxy.uploaded[("ns", "coll", "1.0.0")]
    first_blob = galaxy.blobs[("ns", "coll", "1.0.0")]

    changed = artifact("ns", "coll", "1.0.0", modules=("other",))
    galaxy.replace("ns", "coll", "1.0.0", changed)
    res = reconcile(client, store, "ns", "coll")
    assert res.conflicts == ["1.0.0"]
    (line,) = store.read_index("ns", "coll")
    assert line["cksum"] == original["cksum"]
    assert line["upstream_conflict"] is True
    (conflict,) = store.open_conflicts()
    upstream = hashlib.sha256(changed).hexdigest()
    assert conflict["upstream"] == upstream
    assert conflict["changes"] == "added plugins/modules/other.py; removed plugins/modules/thing.py"
    assert store.state_bucket.read(conflict["saved"]) == changed
    assert store.state["failures"] == {}  # reported as a conflict, not retried as a failure
    report = store.write_failure_report()
    assert "### Conflicts to inspect" in report.markdown
    assert f"| ns.coll | 1.0.0 | {original['cksum'][:12]} | {upstream[:12]} |" in report.markdown
    assert conflict["saved"] not in report.markdown  # the state bucket key stays private
    assert report.summary.alerts.conflicts == [conflict]

    # Seen again: still one open conflict, and no new download.
    galaxy.requests.clear()
    assert reconcile(client, store, "ns", "coll", revalidate=True).conflicts == ["1.0.0"]
    assert len(store.state["conflicts"]) == 1
    assert not any("/pulp/content/" in r for r in galaxy.requests)

    # Different bytes again before it's resolved: the open conflict takes them.
    again = artifact("ns", "coll", "1.0.0", modules=("third",))
    galaxy.replace("ns", "coll", "1.0.0", again)
    assert reconcile(client, store, "ns", "coll").conflicts == ["1.0.0"]
    (updated,) = store.state["conflicts"]
    assert updated["upstream"] == hashlib.sha256(again).hexdigest()
    assert updated["changes"] == "added plugins/modules/third.py; removed plugins/modules/thing.py"
    assert store.state_bucket.read(updated["saved"]) == again
    assert updated["seen"] == conflict["seen"]

    # Galaxy serves our artifact again (re-uploading it brings back its old upload time).
    galaxy.replace("ns", "coll", "1.0.0", first_blob, uploaded=first_upload)
    res = reconcile(client, store, "ns", "coll")
    assert (res.resolved, res.conflicts) == (["1.0.0"], [])
    (line,) = store.read_index("ns", "coll")
    assert "upstream_conflict" not in line
    assert store.open_conflicts() == []
    assert "resolved" in store.state["conflicts"][0]


def test_conflict_comparison_failures(galaxy, client, store, monkeypatch):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    reconcile(client, store, "ns", "coll")
    (original,) = store.read_index("ns", "coll")
    filename = "ns-coll-1.0.0.tar.gz"

    # A failed download is retried on the next run.
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", modules=("other",)))
    good = galaxy.routes.pop(f"/pulp/content/{filename}?sig=x")
    reconcile(client, store, "ns", "coll")
    (failure,) = store.state["failures"].values()
    assert failure["kind"] == "galaxy_http_404"
    assert failure["message"].startswith("download for comparison: ")
    galaxy.routes[f"/pulp/content/{filename}?sig=x"] = good
    assert reconcile(client, store, "ns", "coll").conflicts == ["1.0.0"]

    # Bytes that don't match Galaxy's sha256.
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", modules=("third",)))
    galaxy.routes[f"/pulp/content/{filename}?sig=x"] = (200, b"tampered", {})
    reconcile(client, store, "ns", "coll")
    assert {f["kind"] for f in store.state["failures"].values()} == {"sha256_mismatch"}

    # Unreadable on Galaxy's side, or our artifact missing: still a conflict to inspect.
    unreadable = b"not a tarball"
    galaxy.replace("ns", "coll", "1.0.0", unreadable)
    reconcile(client, store, "ns", "coll")
    assert store.open_conflicts()[-1]["changes"].startswith("Galaxy's artifact can't be read")
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", modules=("fourth",)))
    monkeypatch.setattr(store, "read_artifact", lambda cksum: None)
    reconcile(client, store, "ns", "coll")
    assert store.open_conflicts()[-1]["changes"] == f"the mirror's artifact {original['cksum']} is missing"
    monkeypatch.setattr(store, "read_artifact", lambda cksum: b"corrupt")
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", modules=("fifth",)))
    reconcile(client, store, "ns", "coll")
    assert store.open_conflicts()[-1]["changes"].startswith("the mirror's artifact can't be read")


def test_old_conflict_failures_are_cleared(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    reconcile(client, store, "ns", "coll")
    store.record_failure("ns.coll", "1.0.0", "upstream_conflict", "from before conflicts were reported separately")
    store.record_failure("ns.coll", "1.0.0", "extract_error", "heals on its own")
    reconcile(client, store, "ns", "coll", revalidate=True)
    assert [f["kind"] for f in store.state["failures"].values()] == ["extract_error"]


def test_revalidate_with_unchanged_bytes(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    reconcile(client, store, "ns", "coll")
    res = reconcile(client, store, "ns", "coll", revalidate=True)
    assert (res.conflicts, res.added, res.changed) == ([], [], False)


def test_deprecation_follows_galaxy(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"], deprecated=True)
    reconcile(client, store, "ns", "coll")
    assert store.read_index("ns", "coll")[0]["deprecated"] is True

    galaxy.add_collection("ns", "coll", ["1.0.0"])
    reconcile(client, store, "ns", "coll")
    assert "deprecated" not in store.read_index("ns", "coll")[0]


def test_failures_for_versions_gone_from_galaxy_are_cleared(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    galaxy.routes[DETAIL.format("1.1.0")] = UNAVAILABLE
    reconcile(client, store, "ns", "coll")
    assert len(store.state["failures"]) == 1

    galaxy.add_collection("ns", "coll", ["1.0.0"])
    reconcile(client, store, "ns", "coll")
    assert store.state["failures"] == {}


@pytest.mark.parametrize("name", [("ns", "../x"), ("NS", "coll"), ("ns", "a/b"), ("", "coll")])
def test_invalid_names_are_rejected(client, store, name):
    with pytest.raises(ValueError, match="invalid collection name"):
        reconcile(client, store, *name)


def test_deadline_defers_new_versions(galaxy, client, store, monkeypatch):
    galaxy.add_collection("ns", "coll", ["2.0.0", "1.0.0", "1.1.0"])

    # Out of time before anything was mirrored: no empty index file.
    res = reconcile(client, store, "ns", "coll", deadline=0)
    assert res.deferred == ["2.0.0", "1.0.0", "1.1.0"]  # oldest upload first
    assert not store.index_collections()
    assert "upstream_updated" not in store.collection_state("ns.coll")

    # Out of time part way: what's mirrored is written, in publication order.
    # Each mirror takes a minute; the deadline passes after the second.
    clock = [0.0]
    mirror = reconcile_mod._Reconcile._mirror

    def slow_mirror(self, v):
        clock[0] += 60
        return mirror(self, v)

    monkeypatch.setattr(reconcile_mod.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(reconcile_mod._Reconcile, "_mirror", slow_mirror)
    res = reconcile(client, store, "ns", "coll", deadline=120)
    monkeypatch.undo()
    assert res.added == ["2.0.0", "1.0.0"]
    assert res.deferred == ["1.1.0"]
    assert [line["vers"] for line in store.read_index("ns", "coll")] == ["2.0.0", "1.0.0"]

    res = reconcile(client, store, "ns", "coll")
    assert (res.added, res.deferred) == (["1.1.0"], [])
    assert [line["vers"] for line in store.read_index("ns", "coll")] == ["2.0.0", "1.0.0", "1.1.0"]
    assert store.collection_state("ns.coll")["upstream_updated"]


def test_new_objects_are_written_without_reading_them(galaxy, client, store, monkeypatch):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert reconcile(client, store, "ns", "coll").added == ["1.0.0"]
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])

    reads: list[str] = []
    for backend in (store.galaxy, store.state_bucket):
        monkeypatch.setattr(backend, "read", _recording(backend.read, reads))
    assert reconcile(client, store, "ns", "coll").added == ["1.1.0"]
    # The existing version's page is compared; the new one's isn't.
    assert "api/v3/collections/ns/coll/versions/1.0.0/index.html" in reads
    assert "api/v3/collections/ns/coll/versions/1.1.0/index.html" not in reads
    assert reads.count("extract/ns/coll.json.zst") == 1  # every extraction in one read


def _recording(read, reads):
    def recorded(key):
        reads.append(key)
        return read(key)

    return recorded


def test_progress_is_saved_part_way(galaxy, client, store, monkeypatch):
    versions = ["1.0.0", "1.1.0", "1.2.0", "1.3.0", "1.4.0"]
    galaxy.add_collection("ns", "coll", versions)
    monkeypatch.setattr(store_mod, "SAVE_EVERY", 2)
    mirror = reconcile_mod._Reconcile._mirror

    def stopping_mirror(self, v):
        if v["version"] == "1.4.0":
            raise KeyboardInterrupt  # the run stops
        return mirror(self, v)

    monkeypatch.setattr(reconcile_mod._Reconcile, "_mirror", stopping_mirror)
    with pytest.raises(KeyboardInterrupt):
        reconcile(client, store, "ns", "coll")
    monkeypatch.setattr(reconcile_mod._Reconcile, "_mirror", mirror)

    # What the next run sees: four versions written, with their pages and state.
    later = Store(store.buckets, store.dl_url)
    assert [line["vers"] for line in later.read_index("ns", "coll")] == versions[:4]
    assert sorted(later.collection_state("ns.coll")["versions"]) == versions[:4]
    listing = json.loads(later.galaxy.read("api/v3/collections/ns/coll/versions/index.html"))
    assert listing["meta"]["count"] == 4
    assert later.galaxy.exists("api/v3/collections/ns/coll/versions/1.3.0/index.html")

    res = reconcile(client, later, "ns", "coll")
    assert res.added == ["1.4.0"]
    listing = json.loads(later.galaxy.read("api/v3/collections/ns/coll/versions/index.html"))
    assert listing["meta"]["count"] == 5


def test_lost_version_state_is_recorded_again(galaxy, client, store):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    _ = reconcile(client, store, "ns", "coll")
    created = store.collection_state("ns.coll")["versions"].pop("1.0.0")["created_at"]
    res = reconcile(client, store, "ns", "coll")
    assert not res.changed
    assert store.collection_state("ns.coll")["versions"]["1.0.0"] == {"created_at": created}
