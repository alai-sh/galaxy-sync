import hashlib
import json
import logging
import re

import pytest
from helpers import DETAIL, FEED, LISTING, REPO_METADATA, UNAVAILABLE, artifact, tarball

from galaxy_sync import cli
from galaxy_sync import store as store_mod
from galaxy_sync.storage import Buckets, LocalBackend
from galaxy_sync.store import Store
from galaxy_sync.tree import DEFAULT_DL

# A status alai-index doesn't retry, as its backoff can't be shortened from its command line.
FORBIDDEN = (403, {"detail": "Forbidden"}, {})


@pytest.fixture
def run(galaxy, tmp_path, monkeypatch):
    # Retries in the CLI's client back off for real; the tests don't wait.
    monkeypatch.setattr(cli.GalaxyClient, "__init__", _fast_client(cli.GalaxyClient.__init__))

    def run(command, *args):
        options = ["--store", str(tmp_path)]
        if command == "sync":
            options += ["--galaxy-url", galaxy.url]
        if command == "request":
            # The storage options go after the action.
            return cli.main([command, args[0], *options, *args[1:]])
        return cli.main([command, *options, *args])

    return run


@pytest.fixture
def ask(run):
    """`request`, then the `sync` that carries it out."""

    def ask(*args, sync=()):
        assert run("request", *args) == 0
        return run("sync", *sync)

    return ask


def _fast_client(init):
    def wrapped(self, base_url, **kwargs):
        init(self, base_url, **{**kwargs, "backoff": 0, "retries": 2})

    return wrapped


def index_file(tmp_path, name):
    return (tmp_path / "index-root" / name).read_text()


def index_lines(tmp_path, fqcn):
    """A collection's lines, through the published collections.json."""
    ns, _, name = fqcn.partition(".")
    sha256 = json.loads(index_file(tmp_path, "collections.json"))["namespaces"][ns][name]["sha256"]
    return [json.loads(line) for line in index_file(tmp_path, f"index/{sha256[:2]}/{sha256}.jsonl").splitlines()]


def test_sync_writes_the_index(galaxy, run, tmp_path, capsys, ask):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert ask("mirror", "ns.coll") == 0
    out = capsys.readouterr().out
    assert "ns.coll: +1" in out
    assert "failures: 0 outstanding" in out

    config = json.loads(index_file(tmp_path, "config.json"))
    assert config["dl"] == DEFAULT_DL
    assert config["formats"]["1"]["plugins"] == "plugins.jsonl"
    assert config["formats"]["1"]["index"] == "index/{sha256-prefix}/{sha256-checksum}.jsonl"
    collections = json.loads(index_file(tmp_path, "collections.json"))
    assert list(collections["namespaces"]) == ["ns"]
    assert "updated" in collections["namespaces"]["ns"]["coll"]
    # Published by its sha256, the only copy.
    sha256 = collections["namespaces"]["ns"]["coll"]["sha256"]
    index = (tmp_path / "index-root" / "index" / sha256[:2] / f"{sha256}.jsonl").read_bytes()
    assert sha256 == hashlib.sha256(index).hexdigest()
    assert json.loads(index_file(tmp_path, "plugins.jsonl")) == {
        "v": 1,
        "type": "module",
        "name": "thing",
        "collection": "ns.coll",
        "since": "1.0.0",
        "short_description": "A thing",
    }
    assert len(list((tmp_path / "dl-root" / "sha256").rglob("*.tar.gz"))) == 1


def _plugin_collections(tmp_path):
    return sorted(json.loads(line)["collection"] for line in index_file(tmp_path, "plugins.jsonl").splitlines())


def test_plugins_jsonl_is_patched(galaxy, tmp_path, run, ask):
    galaxy.add_collection("ns", "one", ["1.0.0"])
    galaxy.add_collection("ns", "two", ["1.0.0"])
    assert run("sync") == 0
    assert _plugin_collections(tmp_path) == ["ns.one", "ns.two"]

    # The next run merges only what it reconciled, keeping the rest as is,
    # even with ns.one's plugin cache gone.
    one = tmp_path / "state" / "plugin-cache" / "ns" / "one.jsonl.zst"
    saved = one.read_bytes()
    one.unlink()
    galaxy.add_collection("ns", "two", ["1.0.0", "1.1.0"])
    assert run("request", "mirror", "ns.two") == 0
    assert run("sync") == 0
    assert _plugin_collections(tmp_path) == ["ns.one", "ns.two"]

    # generate-plugins works every collection's plugin lines out again from its plugin cache.
    _ = one.write_bytes(saved)
    (tmp_path / "index-root" / "plugins.jsonl").unlink()
    assert ask("generate-plugins", "--from-plugin-cache") == 0
    assert _plugin_collections(tmp_path) == ["ns.one", "ns.two"]


def test_sync_reports_each_kind_of_change(galaxy, run, tmp_path, capsys, ask):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    assert ask("mirror", "ns.coll") == 0

    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "{}"})})
    capsys.readouterr()
    assert ask("mirror", "--revalidate", "ns.coll") == 0
    out = capsys.readouterr().out
    assert "ns.coll: +0 yanked 1.1.0 CONFLICT 1.0.0\n" in out
    assert "1 open conflicts" in out

    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    assert ask("mirror", "ns.coll", sync=["--strict"]) == 1
    assert "unyanked 1.1.0" in capsys.readouterr().out


def test_failures_are_recorded_and_retried(galaxy, run, tmp_path, capsys, ask, monkeypatch):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    good = galaxy.routes[DETAIL.format("1.0.0")]
    galaxy.routes[DETAIL.format("1.0.0")] = FORBIDDEN
    galaxy.routes["/api/v3/collections/ns/broken/"] = (403, b"", {})

    assert ask("mirror", "ns.coll", "ns.broken") == 0
    captured = capsys.readouterr()
    assert "ns.coll: +0 1 errors" in captured.out
    assert "ns.broken: FAILED" in captured.err
    assert "failures: 2 outstanding, 2 new" in captured.out
    assert {(f["collection"], f["kind"]) for f in state(tmp_path)["failures"].values()} == {
        ("ns.coll", "galaxy_http_403"),
        ("ns.broken", "galaxy_http_403"),
    }
    # The scheduled sync retries outstanding failures.
    galaxy.routes[DETAIL.format("1.0.0")] = good
    galaxy.add_collection("ns", "broken", [])
    capsys.readouterr()
    monkeypatch.setattr(cli, "SHOW_FAILURES", 1)
    assert run("sync", "--strict") == 0
    out = capsys.readouterr().out
    assert "\noutstanding failures: 2 in 2 collections\n  queued, retried in their place in the queue\n" in out
    assert re.search(r"^    ns\.broken  \*  +galaxy_http_403 +1 attempts since \S+ \S+: ", out, re.MULTILINE)
    assert "\n  and 1 more; see state.json in the state bucket\n" in out
    assert out.index("outstanding failures") < out.index("[1/2] ns.coll: +1")
    assert "failures: 0 outstanding, 0 new, 2 resolved" in out
    # Nothing outstanding: nothing listed.
    assert run("sync") == 0
    assert "outstanding failures" not in capsys.readouterr().out


def test_unexpected_errors_are_recorded(galaxy, run, capsys, monkeypatch, ask):
    galaxy.add_collection("ns", "coll", ["1.0.0"])

    def boom(*args, **kwargs):
        raise KeyError("surprise")

    monkeypatch.setattr(cli, "reconcile", boom)
    assert ask("mirror", "ns.coll") == 0
    assert "FAILED unexpectedly (KeyError: 'surprise')" in capsys.readouterr().err
    assert run("sync", "--strict") == 1


def test_sync_namespace(galaxy, run, capsys, ask):
    galaxy.json(
        "/api/v3/plugin/ansible/content/published/collections/index/?limit=100&namespace=ns",
        {"data": [{"namespace": "ns", "name": "coll"}], "links": {"next": None}},
    )
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert ask("mirror", "ns") == 0
    assert "ns.coll: +1" in capsys.readouterr().out


def test_sync_retries_failures_and_reads_nothing_on_failure(galaxy, run, tmp_path, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert run("sync") == 0  # sets the cursor and timestamp

    # Nothing changed and no failures: one ask, no files rewritten.
    collections = tmp_path / "index-root" / "collections.json"
    before = collections.stat().st_mtime_ns
    galaxy.requests.clear()
    assert run("sync") == 0
    assert galaxy.requests == [REPO_METADATA]
    assert collections.stat().st_mtime_ns == before

    # Outstanding failures are retried even when the repo hasn't changed.
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    good = galaxy.routes[DETAIL.format("1.1.0")]
    galaxy.routes[DETAIL.format("1.1.0")] = FORBIDDEN
    assert run("sync") == 0
    galaxy.routes[DETAIL.format("1.1.0")] = good
    capsys.readouterr()
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert "galaxy: published repo is unchanged, no feed scan required\n" in out
    assert "  retry: 1 collections with outstanding failures, if there's time left\n" in out
    assert "retrying 1 collections with outstanding failures" in out
    assert "failures: 0 outstanding" in out

    # If neither the feed nor the listing can be read, nothing is reconciled
    # and the cursor doesn't move, so the next run looks again.
    galaxy.add_collection("ns", "new", ["1.0.0"])
    galaxy.routes[FEED] = UNAVAILABLE
    galaxy.routes[LISTING] = UNAVAILABLE
    state = json.loads((tmp_path / "state" / "state.json").read_text())
    assert run("sync") == 0
    assert "sync failed" in capsys.readouterr().err
    after = json.loads((tmp_path / "state" / "state.json").read_text())
    assert (after["feed_cursor"], after["repo_published"]) == (state["feed_cursor"], state["repo_published"])
    assert sorted((f["collection"], f["version"]) for f in after["failures"].values()) == [
        ("*", "feed"),
        ("*", "listing"),
    ]


def test_usage_errors(run, tmp_path, capsys, monkeypatch):
    with pytest.raises(SystemExit) as e:
        run("request", "sweep", "--revalidate")
    assert e.value.code == 2
    assert "unrecognized arguments: --revalidate" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        run("request", "mirror", "--revalidate")
    assert "the following arguments are required: NS[.NAME]" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        run("request", "bogus")
    assert "invalid choice: 'bogus'" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        run("request", "mirror", "Bad.Name")
    assert "invalid collection name Bad.Name" in capsys.readouterr().err

    for var in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit):
        cli.main(["sync", "--r2"])
    assert "R2 storage needs these environment variables set: R2_ACCOUNT_ID" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        cli.main(["sync", "--r2", "--store", "x"])
    assert "not allowed with argument" in capsys.readouterr().err


def test_verbose_logging(run):
    assert run("request", "sweep") == 0
    assert logging.getLogger("galaxy_sync").level == logging.INFO
    assert run("request", "sweep", "-v") == 0
    assert logging.getLogger("galaxy_sync").level == logging.DEBUG


def test_sync(galaxy, run, tmp_path, capsys, ask):
    galaxy.add_collection("ns", "coll", ["1.0.0", "2.0.0"])
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert (
        "galaxy: no feed cursor yet, so this is the initial import\n  current:  2026-04-01 00:00:01Z\n"
        + "  listing: 1 collections new or changed\n"
        in out
    )
    assert "ns.coll: +2" in out
    assert [line["vers"] for line in index_lines(tmp_path, "ns.coll")] == ["1.0.0", "2.0.0"]
    state = json.loads((tmp_path / "state" / "state.json").read_text())
    assert state["feed_cursor"] == galaxy.uploaded[("ns", "coll", "2.0.0")]
    assert state["repo_published"] == "2026-04-01T00:00:01.000000Z"

    # Nothing changed: one ask, and no count check.
    galaxy.requests.clear()
    assert run("sync") == 0
    assert (
        "galaxy: published repo is unchanged, no feed scan required\n"
        + "  last run: 2026-04-01 00:00:01Z\n  current:  2026-04-01 00:00:01Z\nsweep: next due "
    ) in capsys.readouterr().out
    assert galaxy.requests == [REPO_METADATA]

    # A backport comes from the feed alone.
    galaxy.add_collection("ns", "coll", ["1.0.0", "2.0.0", "1.1.0"])
    galaxy.requests.clear()
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert "  upload feed: 1 collections with new uploads\n" in out
    assert "listing" not in out
    assert "ns.coll: +1" in out
    assert not any("collections/index/" in r for r in galaxy.requests)

    # Deleting a version below the highest only shows in the count.
    galaxy.add_collection("ns", "coll", ["1.0.0", "2.0.0"])
    assert run("sync") == 0
    assert "1 versions are gone from Galaxy but not yet yanked; the next sweep finds them" in capsys.readouterr().out
    state = json.loads((tmp_path / "state" / "state.json").read_text())
    assert [f["kind"] for f in state["failures"].values()] == ["deletions_suspected"]

    assert ask("sweep") == 0
    out = capsys.readouterr().out
    assert "reconciling 1 collections" in out
    assert "ns.coll: +0 yanked 1.1.0" in out
    assert "failures: 0 outstanding" in out


def test_sweep_unindexed_versions(galaxy, run, tmp_path, capsys, ask):
    galaxy.add_collection("ns", "a", ["1.0.0", "2.0.0"])
    assert run("sync") == 0
    galaxy.unindex(("ns", "a", "1.0.0"))
    capsys.readouterr()

    # The first sweep can't tell it from a deletion; reconcile keeps it, and
    # the count stops expecting it.
    assert ask("sweep") == 0
    out = capsys.readouterr().out
    assert "  gone: ns.a 1.0.0\n" in out
    assert "ns.a: +0, up to date (2/2 mirrored)" in out
    assert "Galaxy's search index lost it" in out
    assert "failures: 0 outstanding" in out
    assert "mirror: 2 of 2 versions Galaxy publishes (100.0%), 1 of them missing from its search index" in out

    # Later sweeps recheck it without calling it gone.
    assert ask("sweep") == 0
    out = capsys.readouterr().out
    assert "gone:" not in out
    assert "  1 versions in 1 collections are still missing from Galaxy's search index" in out
    assert "Galaxy's search index still leaves out 1.0.0, but Galaxy still serves it" in out
    assert "failures: 0 outstanding" in out
    assert state(tmp_path)["collections"]["ns.a"]["unindexed"] == ["1.0.0"]


def test_generate_plugins(galaxy, run, tmp_path, capsys, ask):
    galaxy.add_collection("ns", "a", ["1.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0", "1.1.0"])
    assert run("sync") == 0
    capsys.readouterr()

    # By default, every version's artifact is fetched and its plugins extracted again.
    assert ask("generate-plugins", "ns.b") == 0
    out = capsys.readouterr().out
    assert "generate the plugins of ns.b, starting this run" in out
    assert "job: generate the plugins of ns.b, 1 collections to go\n" in out
    assert "job: [1/1] ns.b, 2 versions extracted again\n" in out
    assert "job: done, 2 versions extracted again" in out
    assert "jobs" not in state(tmp_path)

    # From the plugin cache, only what's missing, and a lost plugins.jsonl is made again.
    (tmp_path / "state" / "plugin-cache" / "ns" / "a.jsonl.zst").unlink()
    (tmp_path / "index-root" / "plugins.jsonl").unlink()
    assert ask("generate-plugins", "--from-plugin-cache") == 0
    assert "job: done, 1 versions extracted again" in capsys.readouterr().out
    assert (tmp_path / "state" / "plugin-cache" / "ns" / "a.jsonl.zst").exists()
    assert _plugin_collections(tmp_path) == ["ns.a", "ns.b"]

    # A collection alai-index refuses is a failure, and the job carries on.
    sha256 = json.loads(index_file(tmp_path, "collections.json"))["namespaces"]["ns"]["a"]["sha256"]
    (tmp_path / "index-root" / "index" / sha256[:2] / f"{sha256}.jsonl").write_text("not json\n")
    assert ask("generate-plugins", "--from-plugin-cache") == 0
    assert "job: [1/2] ns.a: FAILED" in capsys.readouterr().err
    assert "jobs" not in state(tmp_path)
    assert ("ns.a", "alai_index") in [(f["collection"], f["kind"]) for f in state(tmp_path)["failures"].values()]


def test_jobs_carry_on_in_the_next_run(galaxy, run, tmp_path, capsys):
    galaxy.add_collection("ns", "a", ["1.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0"])
    assert run("sync") == 0
    assert run("request", "generate-indexes") == 0
    assert run("request", "generate-plugins", "ns") == 0
    capsys.readouterr()

    # Out of time: both wait. Requests made in the same second are taken in either order.
    assert run("sync", "--budget", "0") == 0
    jobs = [job["action"] for job in state(tmp_path)["jobs"]]
    assert sorted(jobs) == ["generate-indexes", "generate-plugins"]

    # The next run's plan says so, and works them in that order.
    capsys.readouterr()
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert "  job: generate the index lines of every collection, requested " in out
    done = {
        "generate-indexes": "job: done, 0 lines changed",
        "generate-plugins": "job: done, 2 versions extracted again",
    }
    assert out.index(done[jobs[0]]) < out.index(done[jobs[1]])
    assert "jobs" not in state(tmp_path)


def test_generate_indexes(galaxy, run, tmp_path, capsys, ask):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert run("sync") == 0
    # As a line published before a fix to how artifacts are read.
    collections = json.loads(index_file(tmp_path, "collections.json"))
    sha256 = collections["namespaces"]["ns"]["coll"]["sha256"]
    path = tmp_path / "index-root" / "index" / sha256[:2] / f"{sha256}.jsonl"
    data = path.read_bytes().replace(b'"deps":[]', b'"deps":[{"name":"ns.other","req":"*"}]')
    new = hashlib.sha256(data).hexdigest()
    (tmp_path / "index-root" / "index" / new[:2]).mkdir(parents=True, exist_ok=True)
    (tmp_path / "index-root" / "index" / new[:2] / f"{new}.jsonl").write_bytes(data)
    collections["namespaces"]["ns"]["coll"]["sha256"] = new
    (tmp_path / "index-root" / "collections.json").write_text(json.dumps(collections))
    capsys.readouterr()

    assert ask("generate-indexes", "ns.coll") == 0
    out = capsys.readouterr().out
    assert "  ns.coll 1.0.0: line changed\n" in out
    assert "job: done, 1 lines changed" in out
    assert index_lines(tmp_path, "ns.coll")[0]["deps"] == []


def test_an_index_without_its_listing_is_refused(galaxy, run, tmp_path, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert run("sync") == 0
    (tmp_path / "index-root" / "collections.json").unlink()
    with pytest.raises(SystemExit) as e:
        run("sync")
    assert e.value.code == 2
    assert "has config.json but no collections.json" in capsys.readouterr().err


def test_aggregates_are_published_before_state_is_saved(galaxy, run, tmp_path, monkeypatch):
    galaxy.add_collection("ns", "a", ["1.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0"])
    monkeypatch.setattr(store_mod, "SAVE_EVERY", 1)
    real = cli.reconcile

    def stopping(client, store, tree, ns, name, **kwargs):
        if name == "b":
            raise KeyboardInterrupt  # the run stops
        return real(client, store, tree, ns, name, **kwargs)

    monkeypatch.setattr(cli, "reconcile", stopping)
    with pytest.raises(KeyboardInterrupt):
        run("sync")

    # State says ns.a is done, so the published listing has it too.
    assert "ns.a" not in state(tmp_path)["pending"]
    assert [line["vers"] for line in index_lines(tmp_path, "ns.a")] == ["1.0.0"]
    assert "ns.a" in _plugin_collections(tmp_path)


def state(tmp_path):
    return json.loads((tmp_path / "state" / "state.json").read_text())


def test_sweep_and_mirror_requests(galaxy, run, tmp_path, capsys, ask):
    galaxy.add_collection("ns", "a", ["1.0.0", "1.1.0", "2.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0"])
    assert run("sync") == 0
    galaxy.add_collection("ns", "a", ["1.0.0", "2.0.0"])
    capsys.readouterr()

    # A sweep request asks the next sync to sweep, which queues what differs.
    assert ask("sweep") == 0
    out = capsys.readouterr().out
    assert "sweep: Galaxy publishes 3 versions; 1 mirrored versions are gone from it and 0 aren't mirrored" in out
    assert "  gone: ns.a 1.1.0\n" in out
    assert "ns.a: +0 yanked 1.1.0" in out
    assert not list((tmp_path / "state" / "requests").iterdir())

    # The sweep runs once a day.
    assert run("sync") == 0
    assert "sweep: Galaxy publishes" not in capsys.readouterr().out
    current = state(tmp_path)
    current["last_sweep"] = "2026-01-01T00:00:00Z"
    (tmp_path / "state" / "state.json").write_text(json.dumps(current))
    assert run("sync") == 0
    assert "sweep: Galaxy publishes 3 versions" in capsys.readouterr().out

    # With targets, the next sync reconciles them first, without a sweep.
    assert ask("mirror", "ns.b", "ns.a") == 0
    out = capsys.readouterr().out
    assert "[1/2] ns.a" in out
    assert "[2/2] ns.b" in out
    assert "sweep: Galaxy publishes" not in out

    # A failed listing still applies the sweep.
    galaxy.add_collection("ns", "a", ["1.0.0"])
    galaxy.routes[LISTING] = UNAVAILABLE
    assert ask("sweep") == 0
    captured = capsys.readouterr()
    assert "listing collections failed; deprecation changes wait for the next sweep" in captured.err
    assert "ns.a: +0 yanked 2.0.0" in captured.out
    galaxy.add_collection("ns", "a", ["1.0.0"])

    # A failed walk queues nothing, and the next sync sweeps again.
    galaxy.routes[FEED] = UNAVAILABLE
    assert ask("sweep") == 0
    assert "the sweep couldn't read the upload feed" in capsys.readouterr().err
    after = state(tmp_path)
    assert "last_sweep" not in after
    assert {f["version"] for f in after["failures"].values()} >= {"feed", "sweep"}


def test_mirror_requests(galaxy, run, tmp_path, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert run("sync") == 0
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "{}"})})

    # A namespace is expanded by the sync; a failed expansion keeps the request.
    namespace = "/api/v3/plugin/ansible/content/published/collections/index/?limit=100&namespace=ns"
    galaxy.routes[namespace] = UNAVAILABLE
    assert run("request", "mirror", "--revalidate", "ns") == 0
    capsys.readouterr()
    assert run("sync") == 0
    assert re.search(r"^request requests/\S+\.json: kept for the next run", capsys.readouterr().err, re.MULTILINE)
    assert "galaxy_unavailable" in json.dumps(state(tmp_path)["failures"])
    assert len(list((tmp_path / "state" / "requests").iterdir())) == 1

    galaxy.json(namespace, {"data": [{"namespace": "ns", "name": "coll"}], "links": {"next": None}})
    capsys.readouterr()
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert re.search(r"^    requests/\S+\.json, from .+\n      mirror ns \(revalidating\)$", out, re.MULTILINE)
    assert re.search(r"^request requests/\S+\.json: queued 1 collections ahead", out, re.MULTILINE)
    assert "ns.coll: +0 CONFLICT 1.0.0" in out
    assert not list((tmp_path / "state" / "requests").iterdir())
    assert not state(tmp_path)["failures"]

    # A request written by hand with a bad name is dropped.
    store = Store(Buckets.local(tmp_path))
    _ = store.add_request("mirror", ["Bad"])
    assert run("sync") == 0
    assert ".json: dropped: invalid" in capsys.readouterr().err
    assert not list((tmp_path / "state" / "requests").iterdir())


def test_budget(galaxy, run, tmp_path, capsys):
    galaxy.add_collection("ns", "a", ["1.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0"])

    # Out of time: what was found stays queued, and the cursor still moves.
    assert run("sync", "--budget", "0") == 0
    assert "out of time: 2 collections left for the next run" in capsys.readouterr().out
    saved = state(tmp_path)
    assert sorted(saved["pending"]) == ["ns.a", "ns.b"]
    assert {p["source"] for p in saved["pending"].values()} == {"listed"}  # the initial import
    assert saved["feed_cursor"]

    # A requested collection keeps to the budget too, staying first in the queue.
    assert run("request", "mirror", "ns.b") == 0
    assert run("sync", "--budget", "0") == 0
    assert "out of time: 1 collections left for the next run" in capsys.readouterr().out
    assert state(tmp_path)["pending"]["ns.b"]["source"] == "requested"

    # The next run works the queue though nothing changed on Galaxy, what
    # was requested first and ahead of the sweep.
    store = Store(Buckets.local(tmp_path))
    del store.state["last_sweep"]
    store.save_state()
    # A sweep that's due isn't started once the budget has run out.
    assert run("sync", "--budget", "0") == 0
    assert "sweep: Galaxy publishes" not in capsys.readouterr().out
    assert "last_sweep" not in state(tmp_path)
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert "galaxy: published repo is unchanged, no feed scan required\n" in out
    assert out.index("[1/1] ns.b: +1") < out.index("sweep: Galaxy publishes") < out.index("[1/1] ns.a: +1")
    assert not state(tmp_path)["pending"]


def comments(directory):
    return [f.read_text() for f in sorted(directory.glob("*.md"))]


def test_alerts(galaxy, run, tmp_path, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    alerts = tmp_path / "alerts"
    assert run("sync", "--alerts", str(alerts)) == 0
    assert comments(alerts) == []

    # A new conflict is only in the report, not a comment.
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "{}"})})
    assert run("request", "mirror", "--revalidate", "ns.coll") == 0
    capsys.readouterr()
    assert run("sync", "--alerts", str(alerts)) == 0
    assert comments(alerts) == []
    assert "galaxy-sync: 1 new conflict\n" in capsys.readouterr().out

    # Nor is Galaxy serving different bytes again for the version.
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "[]"})})
    assert run("request", "mirror", "--revalidate", "ns.coll") == 0
    assert run("sync", "--alerts", str(alerts)) == 0
    assert comments(alerts) == []
    (conflict,) = state(tmp_path)["conflicts"]
    assert len(conflict["observed"]) == 2

    # A failure does, once it has been outstanding for a day.
    galaxy.add_collection("ns", "other", ["1.0.0"])
    galaxy.routes[DETAIL.replace("ns/coll", "ns/other").format("1.0.0")] = FORBIDDEN
    assert run("sync", "--alerts", str(alerts)) == 0
    assert comments(alerts) == []
    current = state(tmp_path)
    for failure in current["failures"].values():
        failure["first_seen"] = "2026-01-01T00:00:00Z"
    (tmp_path / "state" / "state.json").write_text(json.dumps(current))
    assert run("sync", "--alerts", str(alerts)) == 0
    (text,) = comments(alerts)
    assert text.startswith("**galaxy-sync: 1 failure outstanding for over a day**\n\nEach failure is reported once")
    assert "\n| Collection | Version | Kind | Attempts | First seen | Message |\n|---|---|---|---|---|---|\n" in text
    assert "\n| ns.other | 1.0.0 | galaxy_http_403 | 2 | 2026-01-01T00:00:00Z | " in text
    assert text.endswith("\nThe full current report is in this issue's description.\n")
    assert run("sync", "--alerts", str(alerts)) == 0
    assert comments(alerts) == []


def test_state_is_saved_after_enough_work(galaxy, run, monkeypatch):
    for name in ("a", "b", "c"):
        galaxy.add_collection("ns", name, ["1.0.0"])
    saves: list[bytes] = []
    replace = LocalBackend.replace

    def recorded(self, key, data, previous):
        saves.append(data)
        replace(self, key, data, previous)

    monkeypatch.setattr(LocalBackend, "replace", recorded)
    assert run("sync") == 0
    assert len(saves) == 2  # once the changes are queued, and at the end; not per collection

    saves.clear()
    for name in ("a", "b", "c"):
        galaxy.add_collection("ns", name, ["1.0.0", "1.1.0"])
    monkeypatch.setattr(store_mod, "SAVE_EVERY", 2)
    assert run("request", "mirror", "ns") == 0
    assert run("sync") == 0
    assert len(saves) > 3


def test_sync_summary(galaxy, run, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    galaxy.add_collection("ns", "broken", ["1.0.0"])
    galaxy.routes["/api/v3/collections/ns/broken/"] = (403, b"", {})
    assert run("sync") == 0
    out = capsys.readouterr().out
    summary = out[out.index("reconciled 2 collections in ") :]
    assert re.match(r"reconciled 2 collections in \d+s: 2 versions mirrored \(\d+\.\d MB\), 1 failed\n", summary)
    assert re.search(r"^  ns\.broken +FAILED +galaxy_http_403$", summary, re.MULTILINE)
    assert re.search(r"^  ns\.coll +\+2 +2/2$", summary, re.MULTILINE)
    assert "queue: 0 collections left (0 requested, 0 uploaded, 0 listed, 0 swept)" in summary
    assert "mirror: 2 of 3 versions Galaxy publishes (66.7%)" in summary


def test_report(galaxy, run, tmp_path):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    report = tmp_path / "report.md"
    assert run("sync", "--report", str(report)) == 0
    assert report.read_text() == (
        "<!-- galaxy-sync:report -->\n## galaxy-sync report\n\n"
        + "No outstanding failures or open conflicts.\n<!-- /galaxy-sync:report -->\n"
    )

    # A failure and a conflict appear, without attempts or a timestamp, so
    # the block only changes when what's outstanding does.
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "{}"})})
    galaxy.add_collection("ns", "other", ["1.0.0"])
    galaxy.routes[DETAIL.replace("ns/coll", "ns/other").format("1.0.0")] = FORBIDDEN
    assert run("request", "mirror", "--revalidate", "ns.coll") == 0
    assert run("sync", "--report", str(report)) == 0
    text = report.read_text()
    assert "| Error | Count |\n|---|---|\n| [Artifact conflicts](#artifact-conflicts) | 1 |\n" in text
    assert "| [Galaxy HTTP errors](#galaxy-http-errors) | 1 |\n" in text
    assert "| 0 |" not in text
    assert "\n### Galaxy HTTP errors\n\nGalaxy answered with another HTTP error, such as a 403.\n\n<details>\n" in text
    assert (
        "<summary>1 failure, click to expand</summary>\n\n| Collection | Version | Kind | First seen | Message |\n|---|---|---|---|---|\n"
        in text
    )
    assert "\n| ns.other | 1.0.0 | galaxy_http_403 | 20" in text
    assert "\n### Artifact conflicts\n" in text
    assert "<summary>1 conflict, click to expand</summary>" in text
    assert "\n| ns.coll | 1.0.0 | " in text
    assert "conflicts/" not in text
    report.unlink()
    assert run("sync", "--report", str(report)) == 0
    assert report.read_text() == text

    # Resolved: back to nothing outstanding.
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": artifact("ns", "coll", "1.0.0")})
    galaxy.add_collection("ns", "other", ["1.0.0"])
    assert run("request", "mirror", "--revalidate", "ns.coll") == 0
    assert run("sync", "--report", str(report)) == 0
    assert "No outstanding failures or open conflicts." in report.read_text()


def test_unexpected_failure_messages_lose_urls(galaxy, run, tmp_path, monkeypatch):
    galaxy.add_collection("ns", "coll", ["1.0.0"])

    def boom(*args, **kwargs):
        raise RuntimeError("Could not connect to the endpoint URL: https://abc123.r2.cloudflarestorage.com/state")

    monkeypatch.setattr(cli, "reconcile", boom)
    report = tmp_path / "report.md"
    assert run("sync", "--report", str(report)) == 0
    text = report.read_text()
    assert "| ns.coll | * | unexpected | " in text
    assert "RuntimeError: Could not connect to the endpoint URL: <url> |" in text
    assert "cloudflarestorage" not in text
    (failure,) = state(tmp_path)["failures"].values()
    assert "https://abc123.r2.cloudflarestorage.com/state" in failure["message"]  # kept in private state


def test_run_output_explains_itself(galaxy, run, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    galaxy.add_collection("ns", "other", ["1.0.0"])
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert ("plan (budget 45m):\n  queue: 2 collections (0 requested, 0 uploaded, 2 listed, 0 swept)\n") in out
    assert "\n    ns.coll, ns.other\n" in out
    assert re.search(r"^sweep: next due \S+ \S+Z \(in 24h\)$", out, re.MULTILINE)

    # A reconcile that changes nothing says why the collection was queued:
    # here the feed's overlap read the newest upload again.
    galaxy.touch()
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert re.search(
        r"^galaxy: published repo has changed\n  last run: \S+ \S+Z\n  current:  \S+ \S+Z$", out, re.MULTILINE
    )
    assert "  upload feed: no new uploads, 1 collections seen again in the 1h overlap\n" in out
    assert (
        "[1/1] ns.other: +0, up to date (1/1 mirrored)\n"
        + "        the upload feed saw 1.0.0: already mirrored, so seen again in the feed's 1h overlap\n"
    ) in out

    # New bytes with the same contents: a conflict.
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", mtime=1_800_000_000))
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert "  upload feed: 1 collections with new uploads, 1 collections seen again in the 1h overlap\n" in out
    assert "ns.coll: +0 CONFLICT 1.0.0" in out
    assert "galaxy-sync: 1 new conflict\n" in out

    # The plan lists each request; a deprecation is a change of its own.
    galaxy.add_collection("ns", "other", ["1.0.0"], deprecated=True)
    assert run("request", "sweep") == 0
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert "sweep: due now (requested)\nplan (budget 45m):\n  requests: 1 waiting\n" in out
    assert re.search(r"^      sweep\n  sweep: queues everything that differs from Galaxy$", out, re.MULTILINE)
    assert "ns.other: +0 deprecated\n" in out

    # Nothing to do.
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert re.match(
        r"galaxy: published repo is unchanged, no feed scan required\n  last run: (\S+ \S+Z)\n  current:  \1\n"
        + r"sweep: next due .+\nplan: nothing to do\n$",
        out,
    )


def test_explain_unchanged_reconciles():
    from galaxy_sync.reconcile import Result

    res = Result(listed={"1.0.0"}, upstream=1)

    def explain(**entry):
        return cli._explain({"source": "listed", "queued": "", **entry}, res)

    assert cli._explain(None, res) == ["queued for its outstanding failures"]
    assert explain() == ["found by the listing"]
    assert explain(source="requested") == ["requested"]
    assert explain(source="uploaded")[0].startswith("the upload feed saw an upload: probably")
    assert explain(uploads=["2.0.0"]) == [
        "the upload feed saw 2.0.0: not in Galaxy's version list, so awaiting approval or deleted since"
    ]
    reasons = ["new", "updated_at changed", "deprecation changed", "gone from the listing", "gone: 1.1.0"]
    reasons += ["not mirrored: 2.0.0", "re-uploaded: 1.0.0", "unindexed: 1.0.0", "something else"]
    assert [line.split(":")[0] for line in explain(reasons=reasons)] == [
        "new in the listing",
        "the listing's updated_at changed but no version did",
        "the listing's deprecation flag changed but the collection's page didn't",
        "missing from the listing, but Galaxy still serves it",
        "the sweep didn't see 1.1.0 on Galaxy, but Galaxy still serves it",
        "the sweep saw 2.0.0 unmirrored, but Galaxy's version list doesn't have it",
        "the sweep saw 1.0.0 re-uploaded, but Galaxy's sha256 still matches ours",
        "Galaxy's search index still leaves out 1.0.0, but Galaxy still serves it",
        "something else",
    ]
    res.upstream = 0
    assert explain(reasons=["new"]) == ["new in the listing; Galaxy lists no versions for it yet"]
