import hashlib
import json
import re

import pytest
from helpers import DETAIL, FEED, LISTING, REPO_METADATA, UNAVAILABLE, artifact, tarball

from galaxy_sync import cli
from galaxy_sync import store as store_mod
from galaxy_sync.storage import Buckets, LocalBackend
from galaxy_sync.store import DEFAULT_DL, Store


@pytest.fixture
def run(galaxy, tmp_path, monkeypatch):
    # Retries in the CLI's client back off for real; the tests don't wait.
    monkeypatch.setattr(cli.GalaxyClient, "__init__", _fast_client(cli.GalaxyClient.__init__))

    def run(command, *args):
        options = ["--store", str(tmp_path)]
        if command == "sync":
            options += ["--galaxy-url", galaxy.url]
        return cli.main([command, *options, *args])

    return run


@pytest.fixture
def refresh(run):
    """`refresh`, then the `sync` that carries it out."""

    def refresh(*args, sync=()):
        assert run("refresh", *args) == 0
        return run("sync", *sync)

    return refresh


def _fast_client(init):
    def wrapped(self, base_url, **kwargs):
        init(self, base_url, **{**kwargs, "backoff": 0, "retries": 2})

    return wrapped


def index_file(tmp_path, name):
    return (tmp_path / "index-root" / name).read_text()


def test_sync_writes_the_index(galaxy, run, tmp_path, capsys, refresh):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert refresh("ns.coll") == 0
    out = capsys.readouterr().out
    assert "ns.coll: +1" in out
    assert "failures: 0 outstanding" in out

    config = json.loads(index_file(tmp_path, "config.json"))
    assert config["dl"] == DEFAULT_DL
    assert config["formats"]["1"]["plugins"] == "plugins.jsonl"
    collections = json.loads(index_file(tmp_path, "collections.json"))
    assert list(collections["namespaces"]) == ["ns"]
    assert "updated" in collections["namespaces"]["ns"]["coll"]
    index = (tmp_path / "index-root" / "index" / "ns" / "coll.jsonl").read_bytes()
    assert collections["namespaces"]["ns"]["coll"]["sha256"] == hashlib.sha256(index).hexdigest()
    assert json.loads(index_file(tmp_path, "plugins.jsonl")) == {
        "v": 1,
        "type": "module",
        "name": "thing",
        "collection": "ns.coll",
        "since": "1.0.0",
        "short_description": "A thing",
    }
    assert (tmp_path / "index-root" / "index" / "ns" / "coll.jsonl").is_file()
    assert len(list((tmp_path / "dl-root" / "sha256").rglob("*.tar.gz"))) == 1


def _plugin_collections(tmp_path):
    return sorted(json.loads(line)["collection"] for line in index_file(tmp_path, "plugins.jsonl").splitlines())


def test_plugins_jsonl_is_patched(galaxy, tmp_path, run):
    galaxy.add_collection("ns", "one", ["1.0.0"])
    galaxy.add_collection("ns", "two", ["1.0.0"])
    assert run("sync") == 0
    assert _plugin_collections(tmp_path) == ["ns.one", "ns.two"]

    # The next run merges only what it reconciled, keeping the rest as is,
    # even with ns.one's extractions gone.
    one = tmp_path / "state" / "extract" / "ns" / "one.json.zst"
    saved = one.read_bytes()
    one.unlink()
    galaxy.add_collection("ns", "two", ["1.0.0", "1.1.0"])
    assert run("refresh", "ns.two") == 0
    assert run("sync") == 0
    assert _plugin_collections(tmp_path) == ["ns.one", "ns.two"]
    assert "plugins_pending" not in Store(Buckets.local(tmp_path)).state

    # rebuild works every collection's plugin lines out again from its extractions.
    _ = one.write_bytes(saved)
    (tmp_path / "index-root" / "plugins.jsonl").unlink()
    assert run("rebuild") == 0
    assert _plugin_collections(tmp_path) == ["ns.one", "ns.two"]


def test_plugins_merge_after_a_stopped_run(galaxy, tmp_path, run):
    galaxy.add_collection("ns", "one", ["1.0.0"])
    assert run("sync") == 0
    # As if a run had stopped after reconciling ns.one, before merging it.
    store = Store(Buckets.local(tmp_path))
    store.state["plugins_pending"] = ["ns.one"]
    store.save_state()
    (tmp_path / "index-root" / "plugins.jsonl").write_text("")
    galaxy.add_collection("ns", "two", ["1.0.0"])
    assert run("refresh", "ns.two") == 0
    assert run("sync") == 0
    assert _plugin_collections(tmp_path) == ["ns.one", "ns.two"]


def test_sync_reports_each_kind_of_change(galaxy, run, tmp_path, capsys, refresh):
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    assert refresh("ns.coll") == 0

    store = Store(Buckets.local(tmp_path), "")
    lines = store.read_index("ns", "coll")
    extractions = store.load_extractions("ns", "coll")
    extractions[lines[0]["cksum"]] = {"format": 0, "plugins": [], "routing": {}}
    store.save_extractions("ns", "coll", extractions)
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "{}"})})
    capsys.readouterr()
    assert refresh("--revalidate", "ns.coll") == 0
    out = capsys.readouterr().out
    assert "ns.coll: +0 yanked 1.1.0 CONFLICT 1.0.0 re-extracted 1.0.0\n" in out
    assert "1 conflicts to inspect" in out

    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    assert refresh("ns.coll", sync=["--strict"]) == 1
    assert "unyanked 1.1.0" in capsys.readouterr().out


def test_failures_are_recorded_and_retried(galaxy, run, tmp_path, capsys, refresh, monkeypatch):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    good = galaxy.routes[DETAIL.format("1.0.0")]
    galaxy.routes[DETAIL.format("1.0.0")] = UNAVAILABLE
    galaxy.routes["/api/v3/collections/ns/broken/"] = (403, b"", {})

    assert refresh("ns.coll", "ns.broken") == 0
    captured = capsys.readouterr()
    assert "ns.coll: +0 1 errors" in captured.out
    assert "ns.broken: FAILED" in captured.err
    assert "failures: 2 outstanding, 2 new" in captured.out
    failures = json.loads((tmp_path / "state" / "failures.json").read_text())
    assert {(f["collection"], f["kind"]) for f in failures["outstanding"]} == {
        ("ns.coll", "galaxy_unavailable"),
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
    assert "\n  and 1 more; see failures.json in the state bucket\n" in out
    assert out.index("outstanding failures") < out.index("[1/2] ns.coll: +1")
    assert "failures: 0 outstanding, 0 new, 2 resolved" in out
    # Nothing outstanding: nothing listed.
    assert run("sync") == 0
    assert "outstanding failures" not in capsys.readouterr().out


def test_unexpected_errors_are_recorded(galaxy, run, capsys, monkeypatch, refresh):
    galaxy.add_collection("ns", "coll", ["1.0.0"])

    def boom(*args, **kwargs):
        raise KeyError("surprise")

    monkeypatch.setattr(cli, "reconcile", boom)
    assert refresh("ns.coll") == 0
    assert "FAILED unexpectedly (KeyError: 'surprise')" in capsys.readouterr().err
    assert run("sync", "--strict") == 1


def test_sync_namespace(galaxy, run, capsys, refresh):
    galaxy.json(
        "/api/v3/plugin/ansible/content/published/collections/index/?limit=100&namespace=ns",
        {"data": [{"namespace": "ns", "name": "coll"}], "links": {"next": None}},
    )
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert refresh("ns") == 0
    assert "ns.coll: +1" in capsys.readouterr().out


def test_sync_retries_failures_and_reads_nothing_on_failure(galaxy, run, tmp_path, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert run("sync") == 0  # sets the cursor and timestamp

    # Nothing changed and no failures: one request, no files rewritten.
    collections = tmp_path / "index-root" / "collections.json"
    before = collections.stat().st_mtime_ns
    galaxy.requests.clear()
    assert run("sync") == 0
    assert galaxy.requests == [REPO_METADATA]
    assert collections.stat().st_mtime_ns == before

    # Outstanding failures are retried even when the repo hasn't changed.
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    good = galaxy.routes[DETAIL.format("1.1.0")]
    galaxy.routes[DETAIL.format("1.1.0")] = UNAVAILABLE
    assert run("sync") == 0
    galaxy.routes[DETAIL.format("1.1.0")] = good
    capsys.readouterr()
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert "galaxy: unchanged since " in out
    assert "  retry: 1 collections with outstanding failures, if there's time left\n" in out
    assert "retrying 1 collections with outstanding failures" in out
    assert "failures: 0 outstanding" in out

    # If neither the feed nor the listing can be read, nothing is reconciled
    # and nothing is saved, so the next run looks again.
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


def _module(path, name, version_added):
    doc = f"module: {name}\\nshort_description: The {name} module\\n"
    if version_added is not None:
        doc += f"version_added: {version_added}\\n"
    (path / f"{name}.py").write_text(f'DOCUMENTATION = "{doc}"\n')


def test_rebuild_builtin(run, tmp_path, capsys):
    src = tmp_path / "ansible"
    modules = src / "lib" / "ansible" / "modules"
    modules.mkdir(parents=True)
    _module(modules, "ping", "historical")
    _module(modules, "pong", '\\"2.15\\"')
    _module(modules, "unquoted", "2.18")  # YAML reads this as a number
    _module(modules, "undated", None)
    (src / "lib" / "ansible" / "plugins" / "filter").mkdir(parents=True)
    (src / "lib" / "ansible" / "plugins" / "filter" / "odd.py").write_text("x = 1\n")
    (src / "lib" / "ansible" / "config").mkdir(parents=True)
    (src / "lib" / "ansible" / "config" / "ansible_builtin_runtime.yml").write_text(
        "plugin_routing:\n  modules:\n    gone:\n      tombstone:\n        removal_version: '2.10'\n"
    )
    (src / "lib" / "ansible" / "release.py").write_text("__version__ = '2.20.0'\n")
    store_dir = tmp_path / "store"
    assert cli.main(["rebuild", "--store", str(store_dir), "--builtin", str(src)]) == 0
    out = capsys.readouterr().out
    assert "ansible.builtin 2.20.0: 4 entries {'module': 4}, 2 with version_added, 1 routing-only" in out
    assert "unresolved filter/test files: ['plugins/filter/odd.py']" in out
    assert "rebuilt 0 collections (5 plugin lines)" in out

    # `since` is each plugin's version_added, not the release extracted.
    Store(Buckets.local(store_dir), "").write_plugins()
    lines = {
        line["name"]: line
        for line in map(json.loads, (store_dir / "index-root" / "plugins.jsonl").read_text().splitlines())
    }
    assert {name: line.get("since") for name, line in lines.items()} == {
        "gone": None,
        "ping": None,
        "pong": "2.15",
        "undated": None,
        "unquoted": "2.18",
    }
    assert lines["pong"]["short_description"] == "The pong module"
    assert lines["gone"]["routing"] == {"tombstone": {"removal_version": "2.10"}}
    assert all(line["collection"] == "ansible.builtin" and "until" not in line for line in lines.values())


def test_usage_errors(run, tmp_path, capsys, monkeypatch):
    with pytest.raises(SystemExit) as e:
        run("rebuild", "--builtin", str(tmp_path / "missing.tar.gz"))
    assert e.value.code == 2
    assert "can't extract" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        run("refresh", "--revalidate")
    assert "--revalidate needs NS or NS.NAME targets" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        run("refresh", "Bad.Name")
    assert "invalid collection name Bad.Name" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        run("refresh", "--ack", "ns.coll:1.0.0")
    assert "--ack and --note go together" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        run("refresh", "--ack", "ns.coll:1.0.0", "--note", "x", "ns.coll")
    assert "--ack can't be combined with targets" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        run("refresh", "--ack", "ns.coll", "--note", "x")
    assert "expected NS.NAME:VERSION, got ns.coll" in capsys.readouterr().err

    for var in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit):
        cli.main(["sync", "--r2"])
    assert "R2 storage needs these environment variables set: R2_ACCOUNT_ID" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        cli.main(["sync", "--r2", "--store", "x"])
    assert "not allowed with argument" in capsys.readouterr().err


def test_verbose_logging(run):
    assert run("sync", "-v", "--dry-run") == 0


def test_sync(galaxy, run, tmp_path, capsys, refresh):
    galaxy.add_collection("ns", "coll", ["1.0.0", "2.0.0"])
    assert run("sync", "--dry-run") == 0
    assert (
        "galaxy: no feed cursor yet, so this is the initial import\n  listing: 1 collections new or changed\n  ns.coll\n"
        in capsys.readouterr().out
    )
    assert not (tmp_path / "index-root").exists()

    assert run("sync") == 0
    assert "ns.coll: +2" in capsys.readouterr().out
    assert (
        tmp_path / "galaxy-root" / "api" / "v3" / "collections" / "ns" / "coll" / "versions" / "index.html"
    ).is_file()
    assert json.loads((tmp_path / "galaxy-root" / "index.html").read_text())["available_versions"] == {"v3": "api/v3/"}
    state = json.loads((tmp_path / "state" / "state.json").read_text())
    assert state["feed_cursor"] == galaxy.uploaded[("ns", "coll", "2.0.0")]

    assert state["repo_published"] == "2026-04-01T00:00:01.000000Z"

    # Nothing changed: one request, and no count check.
    galaxy.requests.clear()
    assert run("sync") == 0
    assert "galaxy: unchanged since 2026-04-01 00:00:01Z\n" in capsys.readouterr().out
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

    assert refresh() == 0
    out = capsys.readouterr().out
    assert "reconciling 1 collections" in out
    assert "ns.coll: +0 yanked 1.1.0" in out
    assert "failures: 0 outstanding" in out


def test_sweep_unindexed_versions(galaxy, run, tmp_path, capsys, refresh):
    galaxy.add_collection("ns", "a", ["1.0.0", "2.0.0"])
    assert run("sync") == 0
    galaxy.unindex(("ns", "a", "1.0.0"))
    capsys.readouterr()

    # The first sweep can't tell it from a deletion; reconcile keeps it, and
    # the count stops expecting it.
    assert refresh() == 0
    out = capsys.readouterr().out
    assert "  gone: ns.a 1.0.0\n" in out
    assert "ns.a: +0, up to date (2/2 mirrored)" in out
    assert "Galaxy's search index lost it" in out
    assert "failures: 0 outstanding" in out
    assert "mirror: 2 of 2 versions Galaxy publishes (100.0%), 1 of them missing from its search index" in out

    # Later sweeps recheck it without calling it gone.
    assert refresh() == 0
    out = capsys.readouterr().out
    assert "gone:" not in out
    assert "  1 versions in 1 collections are still missing from Galaxy's search index" in out
    assert "Galaxy's search index still leaves out 1.0.0, but Galaxy still serves it" in out
    assert "failures: 0 outstanding" in out
    assert state(tmp_path)["collections"]["ns.a"]["unindexed"] == ["1.0.0"]


def test_rebuild(galaxy, run, tmp_path, capsys, refresh):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert refresh("ns.coll") == 0
    config_file = tmp_path / "index-root" / "config.json"
    pages = tmp_path / "galaxy-root" / "api" / "v3" / "collections" / "ns" / "coll" / "versions"

    def urls():
        """config.json's template and every Galaxy page's download URL."""
        return json.loads(config_file.read_text())["dl"], {
            json.loads(page.read_text())["download_url"].split("/")[2] for page in pages.glob("*/index.html")
        }

    assert urls() == (DEFAULT_DL, {"dl.alai.sh"})

    # rebuild --dl moves every URL at once, and later runs keep it.
    assert run("rebuild", "--dl", "https://moved.example/{sha256-checksum}") == 0
    out = capsys.readouterr().out
    assert "[1/1] ns.coll: 1 versions, Galaxy view: 1 pages written, 1 plugin lines\n" in out
    assert "rebuilt 1 collections (1 plugin lines)\n" in out
    assert urls() == ("https://moved.example/{sha256-checksum}", {"moved.example"})
    galaxy.add_collection("ns", "coll", ["1.0.0", "1.1.0"])
    assert refresh("ns.coll") == 0
    assert run("sync") == 0
    assert urls() == ("https://moved.example/{sha256-checksum}", {"moved.example"})

    # A plain rebuild resets it.
    assert run("rebuild") == 0
    assert urls() == (DEFAULT_DL, {"dl.alai.sh"})


def state(tmp_path):
    return json.loads((tmp_path / "state" / "state.json").read_text())


def test_refresh(galaxy, run, tmp_path, capsys, refresh):
    galaxy.add_collection("ns", "a", ["1.0.0", "1.1.0", "2.0.0"])
    galaxy.add_collection("ns", "b", ["1.0.0"])
    assert run("sync") == 0
    galaxy.add_collection("ns", "a", ["1.0.0", "2.0.0"])
    capsys.readouterr()

    # A refresh without targets asks the next sync to sweep, which queues what differs.
    assert refresh() == 0
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
    assert refresh("ns.b", "ns.a") == 0
    out = capsys.readouterr().out
    assert "[1/2] ns.a" in out
    assert "[2/2] ns.b" in out
    assert "sweep: Galaxy publishes" not in out

    # A failed listing still applies the sweep.
    galaxy.add_collection("ns", "a", ["1.0.0"])
    galaxy.routes[LISTING] = UNAVAILABLE
    assert refresh() == 0
    captured = capsys.readouterr()
    assert "listing collections failed; deprecation changes wait for the next sweep" in captured.err
    assert "ns.a: +0 yanked 2.0.0" in captured.out
    galaxy.add_collection("ns", "a", ["1.0.0"])

    # A failed walk queues nothing, and the next sync sweeps again.
    galaxy.routes[FEED] = UNAVAILABLE
    assert refresh() == 0
    assert "the sweep couldn't read the upload feed" in capsys.readouterr().err
    after = state(tmp_path)
    assert "last_sweep" not in after
    assert {f["version"] for f in after["failures"].values()} >= {"feed", "sweep"}


def test_refresh_requests(galaxy, run, tmp_path, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert run("sync") == 0
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "{}"})})

    # A namespace is expanded by the sync; a failed expansion keeps the request.
    namespace = "/api/v3/plugin/ansible/content/published/collections/index/?limit=100&namespace=ns"
    galaxy.routes[namespace] = UNAVAILABLE
    assert run("refresh", "--revalidate", "ns") == 0
    capsys.readouterr()
    assert run("sync") == 0
    assert re.search(r"^request requests/\S+\.json: kept for the next run", capsys.readouterr().err, re.MULTILINE)
    assert "galaxy_unavailable" in json.dumps(state(tmp_path)["failures"])
    assert len(list((tmp_path / "state" / "requests").iterdir())) == 1

    galaxy.json(namespace, {"data": [{"namespace": "ns", "name": "coll"}], "links": {"next": None}})
    capsys.readouterr()
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert re.search(
        r"^    requests/\S+\.json, from .+\n      reconcile namespace ns \(revalidating\)$", out, re.MULTILINE
    )
    assert re.search(r"^request requests/\S+\.json: queued 1 collections ahead", out, re.MULTILINE)
    assert "ns.coll: +0 CONFLICT 1.0.0" in out
    assert not list((tmp_path / "state" / "requests").iterdir())
    assert not state(tmp_path)["failures"]

    # A request written by hand with a bad name is dropped.
    store = Store(Buckets.local(tmp_path))
    _ = store.add_request(["Bad"], False)
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
    assert run("refresh", "ns.b") == 0
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
    assert "galaxy: unchanged since" in out
    assert out.index("[1/1] ns.b: +1") < out.index("sweep: Galaxy publishes") < out.index("[1/1] ns.a: +1")
    assert not state(tmp_path)["pending"]


def comments(directory):
    return [f.read_text() for f in sorted(directory.glob("*.md"))]


def test_alerts(galaxy, run, tmp_path, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    alerts = tmp_path / "alerts"
    assert run("sync", "--alerts", str(alerts)) == 0
    assert comments(alerts) == []

    # A new conflict alerts once, in a comment of its own.
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "{}"})})
    assert run("refresh", "--revalidate", "ns.coll") == 0
    assert run("sync", "--alerts", str(alerts)) == 0
    (text,) = comments(alerts)
    opened = state(tmp_path)["conflicts"][0]["opened"]
    assert text.startswith(
        f"<!-- galaxy-sync:conflict ns.coll 1.0.0 {opened} -->\n**galaxy-sync: new conflict in ns.coll 1.0.0**\n"
    )
    assert "`acknowledge: ns.coll:1.0.0`" in text
    assert "\n| Ours | Galaxy | Uploaded to Galaxy | Seen |\n" in text
    assert text.endswith("\n\nChanges: removed plugins/modules/thing.py; changed MANIFEST.json\n")
    assert "conflicts/" not in text  # the state bucket key stays private
    assert run("refresh", "--revalidate", "ns.coll") == 0
    assert run("sync", "--alerts", str(alerts)) == 0
    assert comments(alerts) == []

    # Galaxy serving different bytes again for the version doesn't alert again.
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "[]"})})
    assert run("refresh", "--revalidate", "ns.coll") == 0
    assert run("sync", "--alerts", str(alerts)) == 0
    assert comments(alerts) == []
    (conflict,) = state(tmp_path)["conflicts"]
    assert len(conflict["observed"]) == 2

    # A failure does, once it has been outstanding for a day.
    galaxy.add_collection("ns", "other", ["1.0.0"])
    galaxy.routes[DETAIL.replace("ns/coll", "ns/other").format("1.0.0")] = UNAVAILABLE
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
    assert "\n| ns.other | 1.0.0 | galaxy_unavailable | 2 | 2026-01-01T00:00:00Z | " in text
    assert text.endswith("\nThe full current report is in this issue's description.\n")
    assert run("sync", "--alerts", str(alerts)) == 0
    assert comments(alerts) == []


def test_acknowledge(galaxy, run, tmp_path, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    assert run("refresh", "ns.coll") == 0
    assert run("sync") == 0
    record = tmp_path / "index-root" / "conflicts" / "ns" / "coll.json"
    assert not record.exists()

    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "{}"})})
    assert run("refresh", "--revalidate", "ns.coll") == 0
    report, acknowledged = tmp_path / "report.md", tmp_path / "acknowledged.jsonl"
    assert run("sync", "--strict", "--report", str(report), "--acknowledged", str(acknowledged)) == 1
    assert "\n| ns.coll | 1.0.0 | " in report.read_text()
    assert acknowledged.read_text() == ""
    (conflict,) = state(tmp_path)["conflicts"]
    (observed,) = conflict["observed"]
    public = json.loads(record.read_text())
    assert public == {
        "collection": "ns.coll",
        "conflicts": [
            {
                "version": "1.0.0",
                "ours": conflict["ours"],
                "opened": conflict["opened"],
                "observed": [{k: observed[k] for k in ("sha256", "uploaded", "seen", "changes")}],
            }
        ],
    }

    # A version without an open conflict is dropped.
    capsys.readouterr()
    assert run("refresh", "--ack", "ns.coll:2.0.0", "--note", "typo") == 0
    assert run("sync") == 0
    assert "acknowledge ns.coll:2.0.0 dropped: it has no open conflict" in capsys.readouterr().err

    # Acknowledged: off the report, and listed for the workflow to resolve its comment.
    assert run("refresh", "--ack", "ns.coll:1.0.0", "--note", "the author republished 1.0.0") == 0
    out = capsys.readouterr().out
    assert "queued acknowledging ns.coll:1.0.0 for the next sync" in out
    assert run("sync", "--strict", "--report", str(report), "--acknowledged", str(acknowledged)) == 0
    out = capsys.readouterr().out
    assert "      acknowledge ns.coll:1.0.0: the author republished 1.0.0\n" in out
    assert "acknowledge ns.coll:1.0.0: acknowledged\n" in out
    assert "No outstanding failures or conflicts to inspect." in report.read_text()
    marker = f"<!-- galaxy-sync:conflict ns.coll 1.0.0 {conflict['opened']} -->"
    assert json.loads(acknowledged.read_text()) == {"marker": marker, "note": "the author republished 1.0.0"}
    (entry,) = json.loads(record.read_text())["conflicts"]
    assert entry["acknowledged"]["note"] == "the author republished 1.0.0"
    acknowledged.unlink()
    assert run("refresh", "ns.coll") == 0
    assert run("sync", "--acknowledged", str(acknowledged)) == 0
    assert acknowledged.read_text() == ""  # once only

    # Later builds are still recorded, silently.
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "[]"})})
    assert run("refresh", "--revalidate", "ns.coll") == 0
    alerts = tmp_path / "alerts"
    assert run("sync", "--strict", "--alerts", str(alerts)) == 0
    assert comments(alerts) == []
    (entry,) = json.loads(record.read_text())["conflicts"]
    assert len(entry["observed"]) == 2

    # Resolved once Galaxy serves our contents again, and kept in the record.
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": artifact("ns", "coll", "1.0.0")})
    assert run("refresh", "--revalidate", "ns.coll") == 0
    assert run("sync") == 0
    (entry,) = json.loads(record.read_text())["conflicts"]
    assert "resolved" in entry
    assert entry["acknowledged"]["note"] == "the author republished 1.0.0"

    # rebuild writes the records again.
    record.unlink()
    assert run("rebuild") == 0
    assert json.loads(record.read_text())["conflicts"] == [entry]


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
    assert run("refresh", "ns") == 0
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
        + "No outstanding failures or conflicts to inspect.\n<!-- /galaxy-sync:report -->\n"
    )

    # A failure and a conflict appear, without attempts or a timestamp, so
    # the block only changes when what's outstanding does.
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": tarball({"MANIFEST.json": "{}"})})
    galaxy.add_collection("ns", "other", ["1.0.0"])
    galaxy.routes[DETAIL.replace("ns/coll", "ns/other").format("1.0.0")] = UNAVAILABLE
    assert run("refresh", "--revalidate", "ns.coll") == 0
    assert run("sync", "--report", str(report)) == 0
    text = report.read_text()
    assert "\n1 failure outstanding, 1 conflict to inspect.\n" in text
    assert "\n| Collection | Version | Kind | First seen | Message |\n|---|---|---|---|---|\n" in text
    assert "\n| ns.other | 1.0.0 | galaxy_unavailable | 20" in text
    assert "\n### Conflicts to inspect\n" in text
    assert "\n| ns.coll | 1.0.0 | " in text
    assert "conflicts/" not in text
    report.unlink()
    assert run("sync", "--report", str(report)) == 0
    assert report.read_text() == text

    # Resolved: back to nothing outstanding.
    galaxy.add_collection("ns", "coll", ["1.0.0"], blobs={"1.0.0": artifact("ns", "coll", "1.0.0")})
    galaxy.add_collection("ns", "other", ["1.0.0"])
    assert run("refresh", "--revalidate", "ns.coll") == 0
    assert run("sync", "--report", str(report)) == 0
    assert "No outstanding failures or conflicts to inspect." in report.read_text()


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
    failures = json.loads((tmp_path / "state" / "failures.json").read_text())
    assert "https://abc123.r2.cloudflarestorage.com/state" in failures["outstanding"][0]["message"]


def test_run_output_explains_itself(galaxy, run, capsys):
    galaxy.add_collection("ns", "coll", ["1.0.0"])
    galaxy.add_collection("ns", "other", ["1.0.0"])
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert "plan (budget 45m):\n  queue: 2 collections (0 requested, 0 uploaded, 2 listed, 0 swept)\n" in out
    assert "\n    ns.coll, ns.other\n" in out
    assert re.search(r"^sweep: next due \S+ \S+Z \(in 24h\)$", out, re.MULTILINE)

    # A reconcile that changes nothing says why the collection was queued:
    # here the feed's overlap read the newest upload again.
    galaxy.touch()
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert re.search(r"^galaxy: changed since the last run \(\S+ \S+Z → \S+ \S+Z\)$", out, re.MULTILINE)
    assert "  upload feed: no new uploads, 1 collections seen again in the 1h overlap\n" in out
    assert (
        "[1/1] ns.other: +0, up to date (1/1 mirrored)\n"
        + "        the upload feed saw 1.0.0: already mirrored, so seen again in the feed's 1h overlap\n"
    ) in out

    # New bytes with the same contents: a warning, and again at the end.
    galaxy.replace("ns", "coll", "1.0.0", artifact("ns", "coll", "1.0.0", mtime=1_800_000_000))
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert "  upload feed: 1 collections with new uploads, 1 collections seen again in the 1h overlap\n" in out
    assert "ns.coll: +0 repacked 1.0.0\nWARNING ns.coll 1.0.0: Galaxy now serves sha256 " in out
    assert "warnings: 1 repacks (Galaxy serves different bytes with the same contents; ours kept)\n" in out
    assert re.search(r"^  ns\.coll 1\.0\.0: ours [0-9a-f]{64}, Galaxy's [0-9a-f]{64}$", out, re.MULTILINE)

    # The plan lists each request; a deprecation is a change of its own.
    galaxy.add_collection("ns", "other", ["1.0.0"], deprecated=True)
    assert run("refresh") == 0
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert "sweep: due now (requested)\nplan (budget 45m):\n  requests: 1 waiting\n" in out
    assert re.search(r"^      sweep\n  sweep: queues everything that differs from Galaxy$", out, re.MULTILINE)
    assert "ns.other: +0 deprecated\n" in out
    assert "warnings:" not in out

    # Nothing to do.
    assert run("sync") == 0
    out = capsys.readouterr().out
    assert re.match(r"galaxy: unchanged since \S+ \S+Z\nsweep: next due .+\nplan: nothing to do\n$", out)


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
