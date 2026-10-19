"""galaxy-sync: mirror galaxy.ansible.com into the alai static index.

    sync       every 10 minutes: queue what changed on Galaxy (and, once a
               day, everything that differs), then work the queue for up
               to --budget minutes
    refresh    ask the next sync to reconcile NS[.NAME] first, to sweep, or
               to acknowledge a conflict
    rebuild    regenerate the files derived from the index

Recorded failures and conflicts don't fail a run; they're reported in
failures.json in the state bucket and, through --report and --alerts, on a
tracking issue. --strict exits 1 while any is outstanding.
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

from .cdn import Purger
from .changes import (
    FEED_OVERLAP,
    Collection,
    Discovery,
    check_count,
    check_target,
    discover,
    few,
    resolve_targets,
    split_version_target,
    sweep,
)
from .galaxy import BASE_URL, GalaxyClient, GalaxyError
from .models import Pending, Request
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
    if args.r2:
        return Store(Buckets.r2(R2Config.from_env()), purger=Purger.from_env())
    return Store(Buckets.local(args.store))


def _exit_code(store: Store, args: argparse.Namespace) -> int:
    return 1 if args.strict and (store.state["failures"] or store.conflicts_to_inspect()) else 0


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
    if res.deprecated is not None:
        parts.append("deprecated" if res.deprecated else "no longer deprecated")
    if res.deferred:
        parts.append(f"{len(res.deferred)} left for the next run")
    if res.errors:
        parts.append(f"{len(res.errors)} errors")
    return parts


def _describe(res: Result) -> str:
    changes = _changes(res)
    if res.added or changes:
        return " ".join([f"+{len(res.added)}", *changes])
    checked = f", rechecked {res.rechecked} versions: all match" if res.rechecked else ""
    return f"+0, up to date ({res.mirrored}/{res.upstream} mirrored){checked}"


def _explain(entry: Pending | None, res: Result) -> list[str]:
    """Why a collection whose reconcile changed nothing was queued, and what may explain it."""
    if entry is None:
        return ["queued for its outstanding failures"]
    lines: list[str] = []
    if uploads := entry.get("uploads"):
        if seen := [v for v in uploads if v in res.listed]:
            lines.append(
                f"the upload feed saw {few(seen)}: already mirrored, so seen again in the feed's {_wait(FEED_OVERLAP)} overlap"
            )
        if unlisted := [v for v in uploads if v not in res.listed]:
            lines.append(
                f"the upload feed saw {few(unlisted)}: not in Galaxy's version list, so awaiting approval or deleted since"
            )
    for reason in entry.get("reasons", []):
        label, _, versions = reason.partition(": ")
        match label:
            case "new":
                lines.append(
                    "new in the listing" + ("; Galaxy lists no versions for it yet" if not res.upstream else "")
                )
            case "updated_at changed":
                lines.append(
                    "the listing's updated_at changed but no version did: possibly a highest version deleted or "
                    + "re-uploaded, or an edit on Galaxy that isn't a release"
                )
            case "deprecation changed":
                lines.append(
                    "the listing's deprecation flag changed but the collection's page didn't: the listing may lag"
                )
            case "gone from the listing":
                lines.append("missing from the listing, but Galaxy still serves it")
            case "gone":
                lines.append(
                    f"the sweep didn't see {versions} on Galaxy, but Galaxy still serves it: rows can shift while "
                    + "the feed is paged, or Galaxy's search index lost it. Not counted until the feed has it again"
                )
            case "unindexed":
                lines.append(f"Galaxy's search index still leaves out {versions}, but Galaxy still serves it")
            case "not mirrored":
                lines.append(f"the sweep saw {versions} unmirrored, but Galaxy's version list doesn't have it")
            case "re-uploaded":
                lines.append(f"the sweep saw {versions} re-uploaded, but Galaxy's sha256 still matches ours")
            case _:
                lines.append(reason)
    if not lines:
        lines.append(
            {
                "requested": "requested",
                "uploaded": "the upload feed saw an upload: probably seen again in the feed's overlap, "
                + "or awaiting approval on Galaxy",
                "listed": "found by the listing",
                "swept": "found by the sweep",
            }.get(entry["source"], entry["source"])
        )
    return lines


def _duration(seconds: float) -> str:
    minutes, seconds = divmod(round(seconds), 60)
    return f"{minutes}m {seconds}s" if minutes else f"{seconds}s"


def _wait(delta: datetime.timedelta) -> str:
    """A wait in hours and minutes."""
    hours, minutes = divmod(max(round(delta.total_seconds() / 60), 0), 60)
    return f"{hours}h {minutes}m" if hours and minutes else f"{hours}h" if hours else f"{minutes}m"


def _when(timestamp: str | datetime.datetime) -> str:
    """A timestamp in UTC to the second."""
    if isinstance(timestamp, str):
        timestamp = datetime.datetime.fromisoformat(timestamp)
    return timestamp.astimezone(datetime.UTC).strftime("%Y-%m-%d %H:%M:%SZ")


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
        unindexed = sum(len(c.get("unindexed", [])) for c in store.state["collections"].values())
        published += unindexed
        mirrored = sum(len(c["versions"]) for c in store.state["collections"].values())
        print(
            f"mirror: {mirrored} of {published} versions Galaxy publishes ({mirrored / published:.1%})"
            + (f", {unindexed} of them missing from its search index" if unindexed else "")
        )


def _print_warnings(worked: dict[str, Result | str]) -> None:
    """List the repacks: Galaxy serving different bytes with the same contents, which keeps ours."""
    repacks = [
        (fqcn, vers, shas)
        for fqcn, res in worked.items()
        if isinstance(res, Result)
        for vers, shas in res.repacked.items()
    ]
    if not repacks:
        return
    print(f"warnings: {len(repacks)} repacks (Galaxy serves different bytes with the same contents; ours kept)")
    for fqcn, vers, (ours, theirs) in repacks:
        print(f"  {fqcn} {vers}: ours {ours}, Galaxy's {theirs}")


# How many outstanding failures a run lists when it starts; the rest are in failures.json.
SHOW_FAILURES = 20

# When each step's run-level failure (collection GLOBAL) is retried.
GLOBAL_RETRIES = {
    "published": "retried every run",
    "feed": "retried every run",
    "listing": "retried when the listing runs",
    "requests": "retried with the requests",
    "sweep": "retried with the sweep",
    "count": "checked again after reconciling",
    "purge": "cleared by the next purge that works",
}


def _print_outstanding(store: Store, requests: list[tuple[str, Request]]) -> None:
    """List the failures earlier runs left, by when this run retries them."""
    failures = sorted(store.state["failures"].values(), key=lambda e: (e["collection"], e["version"] or "", e["kind"]))
    if not failures:
        return
    pending = store.state["pending"]
    targets = {t for _, request in requests for t in request["targets"]}
    groups: dict[str, list[str]] = {
        "queued, retried in their place in the queue": [],
        "retried after the queue, if there's time left": [],
        "run-level": [],
    }
    width = max(len(e["collection"]) for e in failures)
    vwidth = max(len(e["version"] or "*") for e in failures)
    kwidth = max(len(e["kind"]) for e in failures)
    for e in failures:
        fqcn = e["collection"]
        row = f"    {fqcn:{width}}  {e['version'] or '*':{vwidth}}  {e['kind']:{kwidth}}  {e['attempts']} attempts since {_when(e['first_seen'])}"
        message = e["message"].replace("\n", " ")[:200]
        if fqcn == GLOBAL:
            group = "run-level"
            row += f", {GLOBAL_RETRIES.get(e['version'] or '', 'retried every run')}"
        elif fqcn in pending or fqcn in targets or fqcn.partition(".")[0] in targets:
            group = "queued, retried in their place in the queue"
        else:
            group = "retried after the queue, if there's time left"
        groups[group].append(f"{row}: {message}")
    collections = {e["collection"] for e in failures} - {GLOBAL}
    run_level = len(groups["run-level"])
    print(
        f"outstanding failures: {len(failures) - run_level} in {len(collections)} collections"
        + (f", {run_level} run-level" if run_level else "")
    )
    shown = 0
    for title, rows in groups.items():
        if rows and shown < SHOW_FAILURES:
            print(f"  {title}")
            for row in rows[: SHOW_FAILURES - shown]:
                print(row)
            shown += min(len(rows), SHOW_FAILURES - shown)
    if len(failures) > SHOW_FAILURES:
        print(f"  and {len(failures) - SHOW_FAILURES} more; see failures.json in the state bucket")


def _asks(request: Request) -> list[str]:
    """What a `refresh` request asks for, one line each."""
    if acknowledge := request.get("acknowledge"):
        return [f"acknowledge {target}: {request.get('note', '')}" for target in acknowledge]
    if not request["targets"]:
        return ["sweep"]
    flags = " (revalidating)" if request["revalidate"] else ""
    return [f"reconcile {'' if '.' in t else 'namespace '}{t}{flags}" for t in request["targets"]]


def _sweep_status(store: Store, requested: bool = False) -> str:
    last = store.state.get("last_sweep")
    if requested:
        return "sweep: due now (requested)"
    if last is None:
        return "sweep: due now"
    due = datetime.datetime.fromisoformat(last) + SWEEP_EVERY
    if (wait := due - datetime.datetime.fromisoformat(now())) <= datetime.timedelta():
        return f"sweep: due now (last ran {_when(last)})"
    return f"sweep: next due {_when(due)} (in {_wait(wait)})"


def _print_plan(store: Store, requests: list[tuple[str, Request]], sweep_due: bool, budget: float) -> None:
    """What this run works, in order."""
    lines: list[str] = []
    if requests:
        lines.append(f"  requests: {len(requests)} waiting")
        for key, request in requests:
            lines.append(f"    {key}, from {_when(request['requested'])}")
            lines.extend(f"      {ask}" for ask in _asks(request))
    if sweep_due:
        lines.append("  sweep: queues everything that differs from Galaxy")
    pending = store.state["pending"]
    if pending:
        sources = collections.Counter(entry["source"] for entry in pending.values())
        lines.append(
            f"  queue: {len(pending)} collections ("
            + ", ".join(f"{sources[source]} {source}" for source in SOURCES)
            + ")"
        )
        lines.append(f"    {few([f'{ns}.{name}' for ns, name in store.queued()], 5)}")
    if retry := [c for c in store.failed_collections() if f"{c[0]}.{c[1]}" not in pending]:
        lines.append(f"  retry: {len(retry)} collections with outstanding failures, if there's time left")
    if not lines:
        print("plan: nothing to do")
        return
    print(f"plan (budget {budget:g}m):")
    for line in lines:
        print(line)


def _print_discovery(res: Discovery, previous: str | None, initial: bool) -> None:
    """Whether Galaxy changed since the last run, and what finding changes turned up."""
    if res.unchanged and res.published:
        print(f"galaxy: unchanged since {_when(res.published)}")
        return
    if initial:
        print("galaxy: no feed cursor yet, so this is the initial import")
    elif previous and res.published:
        print(f"galaxy: changed since the last run ({_when(previous)} → {_when(res.published)})")
    else:
        print("galaxy: checking for changes since the last run")
    if not res.feed:
        print("  upload feed: couldn't be read")
    elif not initial:
        new = f"{len(res.new_uploads)} collections with new uploads" if res.new_uploads else "no new uploads"
        again = len(res.uploads) - len(res.new_uploads)
        overlap = f", {again} collections seen again in the {_wait(FEED_OVERLAP)} overlap" if again else ""
        print(f"  upload feed: {new}{overlap}")
    if res.listed:
        why = "" if initial else " (the repo changed without an upload)" if res.hinted else " (in place of the feed)"
        print(f"  listing{why}: {len(res.listing)} collections new or changed")
    elif initial or res.hinted or not res.feed:
        print("  listing: couldn't be read")


def _queue_requests(client: GalaxyClient, store: Store, requests: list[tuple[str, Request]]) -> int:
    """Queue every waiting `refresh` request and delete it; returns how many were taken."""
    taken = 0
    failed = False
    for key, request in requests:
        what = f"request {key}"
        if acknowledge := request.get("acknowledge"):
            _acknowledge(store, key, acknowledge, request.get("note", ""))
        elif not request["targets"]:
            _ = store.state.pop("last_sweep", None)  # sweep this run
            print(f"{what}: sweeping this run")
        else:
            try:
                targets = resolve_targets(client, request["targets"])
            except GalaxyError as e:
                store.record_failure(GLOBAL, "requests", e.kind, f"expanding {', '.join(request['targets'])}: {e}")
                print(f"{what}: kept for the next run, as Galaxy couldn't expand it: {e}", file=sys.stderr)
                failed = True
                continue
            except ValueError as e:  # `refresh` checks names, so only a hand-written request
                print(f"{what}: dropped: {e}", file=sys.stderr)
                targets = []
            else:
                print(f"{what}: queued {len(targets)} collections ahead of everything else")
            for ns, name in targets:
                store.enqueue(f"{ns}.{name}", "requested", revalidate=request["revalidate"])
        store.save_state()  # before deleting, so a crash can only take a request twice
        store.delete_request(key)
        taken += 1
    if not failed:
        store.clear_failures(GLOBAL, "requests")
    return taken


def _acknowledge(store: Store, key: str, targets: list[str], note: str) -> None:
    """Acknowledge the conflicts a request names, so they're no longer reported."""
    for target in targets:
        what = f"request {key}: acknowledge {target}"
        try:
            fqcn, vers = split_version_target(target)
        except ValueError as e:  # `refresh` checks names, so only a hand-written request
            print(f"{what} dropped: {e}", file=sys.stderr)
            continue
        if store.acknowledge_conflict(fqcn, vers, note) is None:
            print(f"{what} dropped: it has no open conflict", file=sys.stderr)
        else:
            print(f"{what}: acknowledged")


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
    if unindexed := sum(map(len, res.unindexed.values())):
        print(
            f"  {unindexed} versions in {len(res.unindexed)} collections are still missing from Galaxy's search "
            + "index, though Galaxy served them last time; rechecking them"
        )
    for c in res.collections:
        store.enqueue(".".join(c), "swept", reasons=res.reasons(c), unseen=res.unseen(c))
    store.save_state()
    return True


def _reconcile_one(
    client: GalaxyClient, store: Store, ns: str, name: str, label: str, entry: Pending | None, deadline: float
) -> Result | str:
    """Reconcile one collection, recording rather than raising anything that goes wrong.

    Returns the result, or the kind of failure that stopped it.
    """
    fqcn = f"{ns}.{name}"
    try:
        res = reconcile(
            client,
            store,
            ns,
            name,
            revalidate=bool(entry and entry.get("revalidate")),
            deadline=deadline,
            unseen=entry.get("unseen", []) if entry else [],
        )
    except GalaxyError as e:  # e.g. listing its versions failed
        store.record_failure(fqcn, None, e.kind, str(e))
        print(f"{label}: FAILED ({e})", file=sys.stderr)
        return e.kind
    except Exception as e:
        store.record_failure(fqcn, None, "unexpected", "".join(traceback.format_exception_only(e)).strip())
        print(f"{label}: FAILED unexpectedly ({type(e).__name__}: {e})", file=sys.stderr)
        return "unexpected"
    print(f"{label}: {_describe(res)}")  # failures were logged as they were recorded
    if not (res.added or _changes(res)):
        for line in _explain(entry, res):
            print(f"{' ' * (len(label) - len(fqcn) + 2)}{line}")
    for vers, (ours, theirs) in res.repacked.items():
        print(
            f"WARNING {fqcn} {vers}: Galaxy now serves sha256 {theirs} instead of ours ({ours}); same contents, so ours is kept"
        )
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
        res = done[fqcn] = _reconcile_one(client, store, ns, name, label, entry, deadline)
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
    store: Store,
    client: GalaxyClient,
    args: argparse.Namespace,
    worked: dict[str, Result | str],
    elapsed: float,
) -> None:
    """Write the aggregate files, the failure report and the state, then the run's summary."""
    store.write_galaxy_root()
    store.write_config()
    plugins = store.write_plugins()
    _ = store.write_conflict_records()
    store.write_collections()  # last, as it purges the others from the CDN
    failures = store.write_failure_report()
    summary = failures.summary
    store.save_state()
    _print_summary(store, worked, elapsed)
    print(f"plugin index: {plugins} lines")
    print(
        f"failures: {summary.outstanding} outstanding, {summary.new} new, {summary.resolved} resolved; "
        + f"{summary.conflicts} conflicts to inspect (see failures.json in the state bucket)"
    )
    _print_warnings(worked)
    print(_sweep_status(store))
    print(f"{client.requests} Galaxy requests")
    if headline := summary.alerts.headline():
        print(headline)
    if args.alerts is not None:
        _write_alerts(args.alerts, summary.alerts.comments())
    if args.acknowledged is not None:
        _ = args.acknowledged.write_text(summary.alerts.acknowledged_json())
    if args.report is not None:
        _ = args.report.write_text(failures.markdown)


def _write_alerts(directory: Path, comments: list[str]) -> None:
    """Write each comment to its own file, in the order to post them, replacing any from before."""
    directory.mkdir(parents=True, exist_ok=True)
    for old in directory.glob("*.md"):
        old.unlink()
    for i, comment in enumerate(comments, 1):
        _ = (directory / f"{i:03}.md").write_text(comment)


def cmd_sync(args: argparse.Namespace) -> int:
    """Queue what changed, take requests and work them, sweep once a day, then work the rest of the queue."""
    started = time.monotonic()
    deadline = started + args.budget * 60
    client = GalaxyClient(args.galaxy_url)
    store = _open_store(args)
    initial = "feed_cursor" not in store.state
    previous = store.state.get("repo_published")
    res = discover(client, store)
    if not res.ok:
        store.save_state()
        print("sync failed: neither the upload feed nor the collection listing could be read", file=sys.stderr)
        return _exit_code(store, args)
    _print_discovery(res, previous, initial)

    if args.dry_run:
        for ns, name in res.collections:
            print(f"  {ns}.{name}")
        print(f"{len(store.state['pending'])} collections already queued, {len(store.requests())} requests waiting")
        store.save_state()
        return _exit_code(store, args)

    # Queued before the cursor moves, so nothing found is lost if the run stops.
    for c in res.collections:
        reasons = [reason] if (reason := res.listing.get(c)) else []
        source = "uploaded" if c in res.uploads else "listed"
        store.enqueue(".".join(c), source, reasons=reasons, uploads=res.uploads.get(c, []))
    if res.cursor:
        store.state["feed_cursor"] = res.cursor
    if res.published:
        store.state["repo_published"] = res.published
    if initial:
        store.state["last_sweep"] = now()  # the initial import's listing already covers everything
    store.save_state()

    requests = store.requests()
    asked = any(not r["targets"] and not r.get("acknowledge") for _, r in requests)
    print(_sweep_status(store, asked))
    _print_plan(store, requests, asked or _sweep_due(store), args.budget)
    _print_outstanding(store, requests)
    taken = _queue_requests(client, store, requests)
    # Requested collections go before the sweep, which can take most of the budget.
    worked: dict[str, Result | str] = {}
    _work(client, store, deadline, worked, requested=True)
    # Once started, the sweep runs to the end, so it isn't started after the
    # budget has run out; the next run starts with it instead.
    swept = time.monotonic() < deadline and _sweep_due(store) and _sweep(client, store)
    _work(client, store, deadline, worked)
    if res.unchanged and not (taken or swept or worked):
        return _exit_code(store, args)  # nothing changed: skip rewriting the aggregate files
    if not res.unchanged or swept:
        _report_count(client, store)
    _finish(store, client, args, worked, time.monotonic() - started)
    return _exit_code(store, args)


def cmd_refresh(args: argparse.Namespace) -> int:
    """Ask the next sync to reconcile collections first, or to sweep."""
    if args.revalidate and not args.targets:
        raise UsageError("--revalidate needs NS or NS.NAME targets")
    if args.acknowledge and args.targets:
        raise UsageError("--ack can't be combined with targets")
    if bool(args.acknowledge) != bool(args.note):
        raise UsageError("--ack and --note go together")
    try:
        for target in args.targets:
            check_target(target)
        for target in args.acknowledge:
            _ = split_version_target(target)
    except ValueError as e:
        raise UsageError(str(e)) from None
    store = _open_store(args)
    key = store.add_request(args.targets, args.revalidate, args.acknowledge, args.note)
    if args.acknowledge:
        what = f"acknowledging {', '.join(args.acknowledge)}"
    else:
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
    collections = store.index_collections()
    width = len(str(len(collections)))
    for i, (ns, name) in enumerate(collections):
        res = store.rebuild_collection(ns, name)
        parts = [
            f"{res.versions} versions",
            f"Galaxy view: {res.pages} pages written" if res.pages else "Galaxy view unchanged",
        ]
        parts.append(f"{res.plugins} plugin lines")
        print(f"[{i + 1:{width}}/{len(collections)}] {ns}.{name}: {', '.join(parts)}")
    store.write_galaxy_root()
    store.write_config()
    plugins = store.merge_plugins()
    _ = store.write_conflict_records(store.conflicted_collections())
    store.write_collections()  # last, as it purges the others from the CDN
    store.save_state()
    print(f"rebuilt {len(collections)} collections ({plugins} plugin lines)")
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
        metavar="DIR",
        help="write a Markdown comment per conflict opened this run, and one for the failures that became "
        + "persistent, into this directory; none if there were none",
    )
    _ = p.add_argument(
        "--acknowledged",
        type=Path,
        metavar="FILE",
        help="write the conflicts acknowledged this run here as JSON lines of their alert comment's marker and note",
    )
    _ = p.add_argument(
        "--report",
        type=Path,
        metavar="FILE",
        help="write the outstanding failures and conflicts here as a Markdown block for the tracking issue",
    )
    _ = p.add_argument("--galaxy-url", default=BASE_URL, help=f"Galaxy server (default: {BASE_URL})")
    _ = p.add_argument("--strict", action="store_true", help="exit 1 if any failure or conflict is outstanding")
    _ = p.add_argument("--dry-run", action="store_true", help="only report what changed; queue nothing")
    p.set_defaults(func=cmd_sync)

    p = sub.add_parser(
        "refresh",
        parents=[storage],
        help="ask the next sync to reconcile NS[.NAME] first, with no targets to run the sweep, "
        + "or to acknowledge conflicts",
    )
    _ = p.add_argument("targets", nargs="*", metavar="NS[.NAME]")
    _ = p.add_argument(
        "--revalidate",
        action="store_true",
        help="also check every mirrored artifact against the one Galaxy publishes now (needs targets)",
    )
    _ = p.add_argument(
        "--ack",
        dest="acknowledge",
        action="append",
        default=[],
        metavar="NS.NAME:VERSION",
        help="acknowledge a version's open conflict, so it's no longer reported (repeatable; needs --note)",
    )
    _ = p.add_argument("--note", default="", help="why the conflicts are acknowledged; published with them")
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
