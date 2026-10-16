"""galaxy-sync: mirror galaxy.ansible.com into the alai static index.

    sync       every 10 minutes: queue what changed on Galaxy (and, once a
               day, everything that differs), then work the queue for up
               to --budget minutes
    refresh    ask the next sync to reconcile NS[.NAME] first, or to sweep
    rebuild    regenerate the files derived from the index

Recorded failures and conflicts don't fail a run; they're reported in
failures.md in the state bucket. --strict exits 1 while any is outstanding.
"""

import argparse
import collections
import datetime
import io
import logging
import sys
import tarfile
import time
import traceback
from pathlib import Path

import alai_plugin_extractor as ext

from .changes import Collection, check_count, check_target, discover, resolve_targets, sweep
from .galaxy import BASE_URL, GalaxyClient, GalaxyError
from .plugins import builtin_lines
from .reconcile import Result, reconcile
from .storage import Buckets, R2Config, StorageError
from .store import DEFAULT_DL, GLOBAL, SOURCES, Store
from .times import now

# How often `sync` runs the full sweep.
SWEEP_EVERY = datetime.timedelta(days=1)


class UsageError(Exception):
    pass


def _open_store(args: argparse.Namespace) -> Store:
    return Store(Buckets.r2(R2Config.from_env()) if args.r2 else Buckets.local(args.store))


def _exit_code(store: Store, args: argparse.Namespace) -> int:
    return 1 if args.strict and (store.state["failures"] or store.open_conflicts()) else 0


def _changes(res: Result) -> list[str]:
    """What a reconcile did besides adding versions."""
    parts: list[str] = []
    for label, versions in (
        ("yanked", res.yanked),
        ("unyanked", res.unyanked),
        ("CONFLICT", res.conflicts),
        ("repacked", res.repacked),
        ("conflict resolved", res.resolved),
        ("re-extracted", res.reextracted),
    ):
        if versions:
            parts.append(f"{label} {','.join(versions)}")
    if res.deferred:
        parts.append(f"{len(res.deferred)} left for the next run")
    if res.errors:
        parts.append(f"{len(res.errors)} errors")
    return parts


def _describe(res: Result) -> str:
    return " ".join([f"+{len(res.added)}", *_changes(res)])


def _duration(seconds: float) -> str:
    minutes, seconds = divmod(round(seconds), 60)
    return f"{minutes}m {seconds}s" if minutes else f"{seconds}s"


def _print_summary(store: Store, worked: dict[str, Result | str], elapsed: float) -> None:
    """Recap what the run reconciled, what's still queued, and how far the mirror is."""
    results = [res for res in worked.values() if isinstance(res, Result)]
    failed = len(worked) - len(results)
    totals = [
        f"{sum(len(r.added) for r in results)} versions mirrored ({sum(r.downloaded for r in results) / 1e6:.1f} MB)"
    ]
    if yanked := sum(len(r.yanked) for r in results):
        totals.append(f"{yanked} yanked")
    if errors := sum(len(r.errors) for r in results):
        totals.append(f"{errors} version errors")
    if failed:
        totals.append(f"{failed} failed")
    print(f"reconciled {len(worked)} collections in {_duration(elapsed)}: {', '.join(totals)}")
    if worked:
        width = max(len("collection"), *map(len, worked))
        print(f"  {'collection':{width}}  this run   mirrored/on Galaxy")
        for fqcn, res in worked.items():
            if isinstance(res, Result):
                counts = ", ".join([f"{res.mirrored}/{res.upstream}", *_changes(res)])
                print(f"  {fqcn:{width}}  {'+' + str(len(res.added)):9}  {counts}")
            else:
                print(f"  {fqcn:{width}}  {'FAILED':9}  {res}")

    pending = store.state["pending"]
    sources = collections.Counter(entry["source"] for entry in pending.values())
    print(
        f"queue: {len(pending)} collections left ("
        + ", ".join(f"{sources[source]} {source}" for source in SOURCES)
        + ")"
    )
    if published := store.state.get("published_count"):
        mirrored = sum(len(c["versions"]) for c in store.state["collections"].values())
        print(f"mirror: {mirrored} of {published} versions Galaxy publishes ({mirrored / published:.1%})")


def _queue_requests(client: GalaxyClient, store: Store) -> int:
    """Queue every waiting `refresh` request and delete it; returns how many were taken."""
    taken = 0
    failed = False
    for key, request in store.requests():
        asked = ", ".join(request["targets"]) or "a sweep"
        flags = ", revalidating" if request["revalidate"] else ""
        what = f"request {key} from {request['requested']}: {asked}{flags}"
        if not request["targets"]:
            _ = store.state.pop("last_sweep", None)  # sweep this run
            print(f"{what}; sweeping this run")
        else:
            try:
                targets = resolve_targets(client, request["targets"])
            except GalaxyError as e:
                store.record_failure(GLOBAL, "requests", e.kind, f"expanding {', '.join(request['targets'])}: {e}")
                print(f"{what}; kept for the next run, as Galaxy couldn't expand it: {e}", file=sys.stderr)
                failed = True
                continue
            except ValueError as e:  # `refresh` checks names, so only a hand-written request
                print(f"{what}; dropped: {e}", file=sys.stderr)
                targets = []
            else:
                print(f"{what}; queued {len(targets)} collections ahead of everything else")
            for ns, name in targets:
                store.enqueue(f"{ns}.{name}", "requested", revalidate=request["revalidate"])
        store.save_state()  # before deleting, so a crash can only take a request twice
        store.delete_request(key)
        taken += 1
    if not failed:
        store.clear_failures(GLOBAL, "requests")
    return taken


def _sweep_due(store: Store) -> bool:
    last = store.state.get("last_sweep")
    return last is None or datetime.datetime.fromisoformat(now()) - datetime.datetime.fromisoformat(last) >= SWEEP_EVERY


def _sweep(client: GalaxyClient, store: Store) -> bool:
    """Queue every collection that differs from Galaxy; False if the feed couldn't be read."""
    try:
        res = sweep(client, store)
    except GalaxyError as e:
        store.record_failure(GLOBAL, "sweep", e.kind, f"upload feed: {e}")
        print(f"the sweep couldn't read the upload feed; it runs again next time: {e}", file=sys.stderr)
        return False
    store.clear_failures(GLOBAL, "sweep")
    store.state["last_sweep"] = now()
    print(
        f"sweep: Galaxy publishes {res.published} versions; "
        + f"{sum(map(len, res.gone.values()))} mirrored versions are gone from it "
        + f"and {sum(map(len, res.unmirrored.values()))} aren't mirrored; "
        + f"{sum(map(len, res.replaced.values()))} were re-uploaded since"
    )
    if res.listed is None:
        print("listing collections failed; deprecation changes wait for the next sweep", file=sys.stderr)
    for fqcn, versions in sorted(res.gone.items()):
        print(f"  gone: {fqcn} {', '.join(versions)}")
    for ns, name in res.collections:
        store.enqueue(f"{ns}.{name}", "swept")
    store.save_state()
    return True


def _reconcile_one(
    client: GalaxyClient, store: Store, ns: str, name: str, label: str, revalidate: bool, deadline: float
) -> Result | str:
    """Reconcile one collection, recording rather than raising anything that goes wrong.

    Returns the result, or the kind of failure that stopped it.
    """
    fqcn = f"{ns}.{name}"
    try:
        res = reconcile(client, store, ns, name, revalidate=revalidate, deadline=deadline)
    except GalaxyError as e:  # e.g. listing its versions failed
        store.record_failure(fqcn, None, e.kind, str(e))
        print(f"{label}: FAILED ({e})", file=sys.stderr)
        return e.kind
    except Exception as e:
        store.record_failure(fqcn, None, "unexpected", "".join(traceback.format_exception_only(e)).strip())
        print(f"{label}: FAILED unexpectedly ({type(e).__name__}: {e})", file=sys.stderr)
        return "unexpected"
    print(f"{label}: {_describe(res)}")  # failures were logged as they were recorded
    return res


def _work(
    client: GalaxyClient, store: Store, deadline: float, done: dict[str, Result | str], *, requested: bool = False
) -> None:
    """Reconcile queued collections, then ones with outstanding failures, until `deadline`.

    With `requested`, only what a person asked for, which is first in the
    queue anyway. Each collection's result is added to `done`, and it isn't
    retried this run. A collection leaves the queue once it's reconciled;
    one that ran out of time part way stays queued, keeping its place.
    """
    pending = store.state["pending"]
    queued = store.queued()
    if requested:
        targets: list[Collection] = [c for c in queued if pending[f"{c[0]}.{c[1]}"]["source"] == "requested"]
        retry: list[Collection] = []
    else:
        retry = [c for c in store.failed_collections() if f"{c[0]}.{c[1]}" not in pending.keys() | done]
        targets = queued + retry
    if not targets:
        return
    print(f"reconciling {len(targets)} {'requested ' if requested else ''}collections ({len(queued)} queued)")
    if retry:
        print(f"retrying {len(retry)} collections with outstanding failures")
    width = len(str(len(targets)))
    for i, (ns, name) in enumerate(targets):
        if time.monotonic() >= deadline:
            print(f"out of time: {len(targets) - i} collections left for the next run")
            return
        fqcn = f"{ns}.{name}"
        entry = pending.get(fqcn)
        label = f"[{i + 1:{width}}/{len(targets)}] {fqcn}"
        revalidate = bool(entry and entry.get("revalidate"))
        res = done[fqcn] = _reconcile_one(client, store, ns, name, label, revalidate, deadline)
        if isinstance(res, Result) and res.deferred:
            store.enqueue(fqcn, entry["source"] if entry else "listed")
        else:
            _ = pending.pop(fqcn, None)
        store.add_work()
        if store.save_due():
            store.save_state()  # keep progress if the run stops


def _report_count(client: GalaxyClient, store: Store) -> None:
    if missing := check_count(client, store):
        print(f"{missing} versions are gone from Galaxy but not yet yanked; the next sweep finds them")


def _finish(
    store: Store, client: GalaxyClient, alerts: Path | None, worked: dict[str, Result | str], elapsed: float
) -> None:
    """Write the aggregate files, the failure report and the state, then the run's summary."""
    store.write_galaxy_root()
    store.write_config()
    store.write_collections()
    plugins = store.write_plugins()
    summary = store.write_failure_report()
    store.save_state()
    _print_summary(store, worked, elapsed)
    print(f"plugin index: {plugins} lines")
    print(
        f"failures: {summary.outstanding} outstanding, {summary.new} new, {summary.resolved} resolved; "
        + f"{summary.conflicts} conflicts to inspect (see failures.md in the state bucket)"
    )
    print(f"{client.requests} Galaxy requests")
    if text := summary.alerts.text():
        print(text)
    if alerts is not None:
        _ = alerts.write_text(text + "\n" if text else "")


def cmd_sync(args: argparse.Namespace) -> int:
    """Queue what changed, take requests and work them, sweep once a day, then work the rest of the queue."""
    started = time.monotonic()
    deadline = started + args.budget * 60
    client = GalaxyClient(args.galaxy_url)
    store = _open_store(args)
    initial = "feed_cursor" not in store.state
    res = discover(client, store)
    if not res.ok:
        store.save_state()
        print("sync failed: neither the upload feed nor the collection listing could be read", file=sys.stderr)
        return _exit_code(store, args)
    if res.unchanged:
        print(f"the published repo hasn't changed since {res.published}")
    else:
        sources = " and ".join(name for name, used in (("upload feed", res.feed), ("listing", res.listed)) if used)
        hint = "; the repo changed without an upload, so the listing ran" if res.hinted else ""
        print(f"{len(res.collections)} collections new or changed (from the {sources}{hint})")

    if args.dry_run:
        for ns, name in res.collections:
            print(f"  {ns}.{name}")
        print(f"{len(store.state['pending'])} collections already queued, {len(store.requests())} requests waiting")
        store.save_state()
        return _exit_code(store, args)

    # Queued before the cursor moves, so nothing found is lost if the run stops.
    for ns, name in res.collections:
        store.enqueue(f"{ns}.{name}", "uploaded" if (ns, name) in res.uploads else "listed")
    if res.cursor:
        store.state["feed_cursor"] = res.cursor
    if res.published:
        store.state["repo_published"] = res.published
    if initial:
        store.state["last_sweep"] = now()  # the initial import's listing already covers everything
    store.save_state()

    requests = _queue_requests(client, store)
    # Requested collections go before the sweep, which can take most of the budget.
    worked: dict[str, Result | str] = {}
    _work(client, store, deadline, worked, requested=True)
    swept = _sweep_due(store) and _sweep(client, store)
    _work(client, store, deadline, worked)
    if res.unchanged and not (requests or swept or worked):
        return _exit_code(store, args)  # nothing changed: skip rewriting the aggregate files
    if not res.unchanged or swept:
        _report_count(client, store)
    _finish(store, client, args.alerts, worked, time.monotonic() - started)
    return _exit_code(store, args)


def cmd_refresh(args: argparse.Namespace) -> int:
    """Ask the next sync to reconcile collections first, or to sweep."""
    if args.revalidate and not args.targets:
        raise UsageError("--revalidate needs NS or NS.NAME targets")
    for target in args.targets:
        try:
            check_target(target)
        except ValueError as e:
            raise UsageError(str(e)) from None
    store = _open_store(args)
    key = store.add_request(args.targets, args.revalidate)
    what = ", ".join(args.targets) if args.targets else "a full sweep"
    print(f"queued {what} for the next sync ({key})")
    return 0


def _extract_builtin(store: Store, src: Path) -> None:
    """Extract ansible.builtin's plugins from an ansible-core checkout or sdist."""
    try:
        result = ext.extract(src, type="ansible", docs=True)
    except (OSError, tarfile.TarError, ValueError) as e:
        raise UsageError(f"can't extract {src}: {e}") from None
    lines = builtin_lines(result)
    store.save_builtin(lines)
    counts = collections.Counter(p.type for p in result.plugins)
    dated = sum(1 for line in lines if "since" in line)
    print(
        f"ansible.builtin {result.version or '(unknown version)'}: {sum(counts.values())} entries {dict(counts)}, "
        + f"{dated} with version_added, {len(lines) - len(result.plugins)} routing-only"
    )
    if unresolved := [i.path for i in result.issues if i.kind == "filter_test_unresolved"]:
        print(f"  unresolved filter/test files: {unresolved}")


def cmd_rebuild(args: argparse.Namespace) -> int:
    """Regenerate every file derived from the index, without contacting Galaxy."""
    store = _open_store(args)
    store.dl_url = args.dl or DEFAULT_DL  # rebuild is the one place the artifact URL changes
    if args.builtin is not None:
        _extract_builtin(store, args.builtin)
    count = store.rebuild_galaxy()
    store.write_config()
    store.write_collections()
    plugins = store.rebuild_plugins()
    store.save_state()
    print(f"rebuilt {count} collections ({plugins} plugin lines)")
    return 0


def _parser() -> argparse.ArgumentParser:
    storage = argparse.ArgumentParser(add_help=False)
    where = storage.add_mutually_exclusive_group()
    _ = where.add_argument(
        "--store",
        type=Path,
        default=Path("out"),
        metavar="DIR",
        help="local directory with one subdirectory per bucket (default: ./out)",
    )
    _ = where.add_argument(
        "--r2",
        action="store_true",
        help="use the R2 buckets; credentials and bucket names come from the environment (docs/storage.md)",
    )
    _ = storage.add_argument("-v", "--verbose", action="store_true", help="debug logging")

    parser = argparse.ArgumentParser(
        prog="galaxy-sync", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(required=True)

    p = sub.add_parser("sync", parents=[storage], help="queue what changed on Galaxy and work the queue (every 10 min)")
    _ = p.add_argument(
        "--budget",
        type=float,
        default=45,
        metavar="MINUTES",
        help="stop starting new work after this long; the rest stays queued (default: 45)",
    )
    _ = p.add_argument(
        "--alerts",
        type=Path,
        metavar="FILE",
        help="write what newly needs a person (counts only) here, or an empty file if nothing does",
    )
    _ = p.add_argument("--galaxy-url", default=BASE_URL, help=f"Galaxy server (default: {BASE_URL})")
    _ = p.add_argument("--strict", action="store_true", help="exit 1 if any failure or conflict is outstanding")
    _ = p.add_argument("--dry-run", action="store_true", help="only report what changed; queue nothing")
    p.set_defaults(func=cmd_sync)

    p = sub.add_parser(
        "refresh",
        parents=[storage],
        help="ask the next sync to reconcile NS[.NAME] first, or with no targets to run the sweep",
    )
    _ = p.add_argument("targets", nargs="*", metavar="NS[.NAME]")
    _ = p.add_argument(
        "--revalidate",
        action="store_true",
        help="also check every mirrored artifact against the one Galaxy publishes now (needs targets)",
    )
    p.set_defaults(func=cmd_refresh)

    p = sub.add_parser("rebuild", parents=[storage], help="regenerate the files derived from the index")
    _ = p.add_argument(
        "--dl",
        metavar="TEMPLATE",
        help="artifact URL template for config.json and the Galaxy view (default: the dl.alai.sh one)",
    )
    _ = p.add_argument(
        "--builtin", type=Path, metavar="SRC", help="re-extract ansible.builtin from this checkout or sdist"
    )
    p.set_defaults(func=cmd_rebuild)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(line_buffering=True)  # keep prints in order with the log on stderr when piped
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("galaxy_sync").setLevel(logging.DEBUG if args.verbose else logging.INFO)
    try:
        return args.func(args)
    except (UsageError, StorageError) as e:
        parser.exit(2, f"{parser.prog}: error: {e}\n")


if __name__ == "__main__":
    sys.exit(main())
