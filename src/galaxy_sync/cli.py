"""galaxy-sync: mirror galaxy.ansible.com into the alai static index.

    sync       every 10 minutes: queue what changed on Galaxy (and, once a
               day, everything that differs), then work the queue for up
               to --budget minutes
    request    ask the next sync to mirror NS[.NAME] first, to sweep, or to
               generate plugins or index lines again from the artifacts

Recorded failures and conflicts don't fail a run; they're reported in
state.json in the state bucket and, through --report and --alerts, on a
tracking issue. --strict exits 1 while any is outstanding.
"""

import argparse
import collections
import datetime
import io
import logging
import sys
import tempfile
import time
import traceback
from pathlib import Path

from .changes import (
    FEED_OVERLAP,
    Collection,
    Discovery,
    check_count,
    check_target,
    discover,
    few,
    resolve_targets,
    sweep,
)
from .galaxy import BASE_URL, GalaxyClient, GalaxyError
from .models import Job, Pending, Request
from .reconcile import ReconcileFailed, Result, generate_indexes, generate_plugins, reconcile
from .storage import Buckets, R2Config, StorageError
from .store import ACTIONS, GLOBAL, NOT_RETRIED, RETIRED_GRACE, SOURCES, Store
from .times import now
from .tree import AlaiIndexError, Tree

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
        ("conflict resolved", res.resolved),
    ):
        if versions:
            parts.append(f"{label} {','.join(versions)}")
    if res.reextracted:
        # A whole collection can be extracted again, so only the count.
        parts.append(f"re-extracted {len(res.reextracted)} versions")
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
    return f"+0, up to date ({res.mirrored}/{res.upstream} mirrored)"


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
        mirrored = sum(c.get("mirrored", 0) for c in store.state["collections"].values())
        print(
            f"mirror: {mirrored} of {published} versions Galaxy publishes ({mirrored / published:.1%})"
            + (f", {unindexed} of them missing from its search index" if unindexed else "")
        )


# How many outstanding failures a run lists when it starts; the rest are in state.json.
SHOW_FAILURES = 20

# When each step's run-level failure (collection GLOBAL) is retried.
GLOBAL_RETRIES = {
    "published": "retried every run",
    "feed": "retried every run",
    "listing": "retried when the listing runs",
    "requests": "retried with the requests",
    "sweep": "retried with the sweep",
    "count": "checked again after reconciling",
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
        "extracted again by a generate-plugins request": [],
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
        elif e["kind"] in NOT_RETRIED:
            group = "extracted again by a generate-plugins request"
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
        print(f"  and {len(failures) - SHOW_FAILURES} more; see state.json in the state bucket")


def _is_sweep(request: Request) -> bool:
    return request.get("action") == "sweep"


def _what(action: str, targets: list[str], *, revalidate: bool = False, from_plugin_cache: bool = False) -> str:
    """What a request or job asks for."""
    every = ", ".join(targets) or "every collection"
    match action:
        case "mirror":
            return f"mirror {', '.join(targets)}" + (" (revalidating)" if revalidate else "")
        case "sweep":
            return "sweep"
        case "generate-plugins":
            return f"generate the plugins of {every}" + (" from the plugin cache" if from_plugin_cache else "")
        case _:
            return f"generate the index lines of {every}"


def _asks(request: Request) -> list[str]:
    """What a request asks for."""
    return [
        _what(
            request.get("action", "?"),
            request.get("targets", []),
            revalidate=request.get("revalidate", False),
            from_plugin_cache=request.get("from_plugin_cache", False),
        )
    ]


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
    for job in store.state.get("jobs", []):
        after = f", continuing after {last}" if (last := job.get("after")) else ""
        what = _what(job["action"], job["targets"], from_plugin_cache=job.get("from_plugin_cache", False))
        lines.append(f"  job: {what}, requested {_when(job['requested'])}{after}")
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
    """Take every waiting request and delete it; returns how many were taken."""
    taken = 0
    failed = False
    for key, request in requests:
        what = f"request {key}"
        action = request.get("action")
        if action in ("generate-plugins", "generate-indexes"):
            job: Job = {"action": action, "requested": request["requested"], "targets": request["targets"]}
            if request.get("from_plugin_cache"):
                job["from_plugin_cache"] = True
            store.state.setdefault("jobs", []).append(job)
            print(f"{what}: {_asks(request)[0]}, starting this run")
        elif action not in ("mirror", "sweep"):  # only a hand-written request, or one from before `request`
            print(f"{what}: dropped: it asks for {action!r}, which isn't one of {', '.join(ACTIONS)}", file=sys.stderr)
        elif action == "sweep":
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
            except ValueError as e:  # `request` checks names, so only a hand-written request
                print(f"{what}: dropped: {e}", file=sys.stderr)
                targets = []
            else:
                print(f"{what}: queued {len(targets)} collections ahead of everything else")
            for ns, name in targets:
                store.enqueue(f"{ns}.{name}", "requested", revalidate=request.get("revalidate", False))
        store.save_state()  # before deleting, so a crash can only take a request twice
        store.delete_request(key)
        taken += 1
    if not failed:
        store.clear_failures(GLOBAL, "requests")
    return taken


def _jobs(store: Store, tree: Tree, deadline: float) -> bool:
    """Work through the requested jobs, in order, until `deadline`; False if there were none.

    A job works a collection at a time, in name order, and keeps its progress
    in state, so one that runs out of time carries on in the next run.
    """
    jobs = store.state.get("jobs", [])
    if not jobs:
        return False
    while jobs:
        job = jobs[0]
        targets = job["targets"]
        names = sorted(f for f in tree.listing if not targets or f in targets or f.partition(".")[0] in targets)
        todo = [fqcn for fqcn in names if (after := job.get("after")) is None or fqcn > after]
        what = _what(job["action"], targets, from_plugin_cache=job.get("from_plugin_cache", False))
        print(f"job: {what}, {len(todo)} collections to go")
        if job["action"] == "generate-plugins" and not targets:
            # Every collection's lines are merged in again, so a lost plugins.jsonl is started afresh.
            tree.fetch_plugins(optional=True)
        changed = 0
        for i, fqcn in enumerate(todo):
            if time.monotonic() >= deadline:
                print(f"job: out of time, {len(todo) - i} collections left for the next run")
                return True
            label = f"job: [{i + 1:{len(str(len(todo)))}}/{len(todo)}] {fqcn}"
            try:
                if job["action"] == "generate-plugins":
                    done = generate_plugins(store, tree, fqcn, from_plugin_cache=job.get("from_plugin_cache", False))
                    print(f"{label}, {len(done)} versions extracted again")
                else:
                    done = generate_indexes(store, tree, fqcn)
                    print(f"{label}, {len(done)} lines changed")
                    for vers in done:
                        print(f"  {fqcn} {vers}: line changed")
            except AlaiIndexError as e:
                store.record_failure(fqcn, None, "alai_index", str(e))
                print(f"{label}: FAILED ({e})", file=sys.stderr)
                done = []
            changed += len(done)
            job["after"] = fqcn
            store.add_work()
            if store.save_due():
                store.save_state()  # keep progress if the run stops
        noun = "versions extracted again" if job["action"] == "generate-plugins" else "lines changed"
        print(f"job: done, {changed} {noun}, requested {_when(job['requested'])}")
        _ = jobs.pop(0)
    del store.state["jobs"]
    return True


def _sweep_due(store: Store) -> bool:
    last = store.state.get("last_sweep")
    return last is None or datetime.datetime.fromisoformat(now()) - datetime.datetime.fromisoformat(last) >= SWEEP_EVERY


def _sweep(client: GalaxyClient, store: Store, tree: Tree) -> bool:
    """Queue every collection that differs from Galaxy; False if the feed couldn't be read."""
    try:
        res = sweep(client, store, tree)
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
    client: GalaxyClient,
    store: Store,
    tree: Tree,
    ns: str,
    name: str,
    label: str,
    entry: Pending | None,
    deadline: float,
) -> Result | str:
    """Reconcile one collection, recording rather than raising anything that goes wrong.

    Returns the result, or the kind of failure that stopped it.
    """
    fqcn = f"{ns}.{name}"
    try:
        res = reconcile(
            client,
            store,
            tree,
            ns,
            name,
            revalidate=bool(entry and entry.get("revalidate")),
            deadline=deadline,
            unseen=entry.get("unseen", []) if entry else [],
        )
    except (GalaxyError, ReconcileFailed) as e:  # e.g. listing its versions failed
        store.record_failure(fqcn, None, e.kind, str(e))
        print(f"{label}: FAILED ({e})", file=sys.stderr)
        return e.kind
    except AlaiIndexError as e:  # refused, so nothing was written
        store.record_failure(fqcn, None, "alai_index", str(e))
        print(f"{label}: FAILED ({e})", file=sys.stderr)
        return "alai_index"
    except Exception as e:
        store.record_failure(fqcn, None, "unexpected", "".join(traceback.format_exception_only(e)).strip())
        print(f"{label}: FAILED unexpectedly ({type(e).__name__}: {e})", file=sys.stderr)
        return "unexpected"
    print(f"{label}: {_describe(res)}")  # failures were logged as they were recorded
    if not (res.added or _changes(res)):
        for line in _explain(entry, res):
            print(f"{' ' * (len(label) - len(fqcn) + 2)}{line}")
    return res


def _work(
    client: GalaxyClient,
    store: Store,
    tree: Tree,
    deadline: float,
    done: dict[str, Result | str],
    *,
    requested: bool = False,
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
        res = done[fqcn] = _reconcile_one(client, store, tree, ns, name, label, entry, deadline)
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
    tree: Tree,
    client: GalaxyClient,
    args: argparse.Namespace,
    worked: dict[str, Result | str],
    elapsed: float,
) -> None:
    """Publish the aggregate files, then write the failure report and the state, then the run's summary."""
    _ = tree.publish_aggregates()
    plugins = tree.plugin_lines()
    pruned = None
    if store.prune_due():
        pruned = store.prune_index_files({tree.index_file(fqcn) for fqcn in tree.listing})
    failures = store.failure_report()
    summary = failures.summary
    store.save_state()
    _print_summary(store, worked, elapsed)
    if plugins is not None:
        print(f"plugin index: {plugins} lines")
    if pruned:
        print(f"deleted {pruned} index files no listing has named for {RETIRED_GRACE.days} days")
    print(
        f"failures: {summary.outstanding} outstanding, {summary.new} new, {summary.resolved} resolved; "
        + f"{summary.conflicts} open conflicts (see state.json in the state bucket)"
    )
    print(_sweep_status(store))
    print(f"{client.requests} Galaxy requests")
    if headline := summary.alerts.headline():
        print(headline)
    if args.alerts is not None:
        _write_alerts(args.alerts, summary.alerts.comments())
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
    asked = any(_is_sweep(r) for _, r in requests)
    print(_sweep_status(store, asked))
    _print_plan(store, requests, asked or _sweep_due(store), args.budget)
    _print_outstanding(store, requests)
    taken = _queue_requests(client, store, requests)
    with tempfile.TemporaryDirectory(prefix="galaxy-sync-", ignore_cleanup_errors=True) as workdir:
        tree = Tree(store.buckets, Path(workdir))
        store.before_save = tree.publish_aggregates
        # What a person asked for goes before the sweep, which can take most of the budget.
        worked: dict[str, Result | str] = {}
        _work(client, store, tree, deadline, worked, requested=True)
        jobbed = _jobs(store, tree, deadline)
        # Once started, the sweep runs to the end, so it isn't started after the
        # budget has run out; the next run starts with it instead.
        swept = time.monotonic() < deadline and _sweep_due(store) and _sweep(client, store, tree)
        _work(client, store, tree, deadline, worked)
        if res.unchanged and not (taken or jobbed or swept or worked):
            store.save_state()  # any failure recorded
            return _exit_code(store, args)  # nothing changed: skip rewriting the aggregate files
        if not res.unchanged or swept:
            _report_count(client, store)
        _finish(store, tree, client, args, worked, time.monotonic() - started)
    return _exit_code(store, args)


def cmd_request(args: argparse.Namespace) -> int:
    """Ask the next sync to do something, without contacting Galaxy."""
    try:
        for target in args.targets:
            check_target(target)
    except ValueError as e:
        raise UsageError(str(e)) from None
    revalidate, from_plugin_cache = getattr(args, "revalidate", False), getattr(args, "from_plugin_cache", False)
    store = _open_store(args)
    key = store.add_request(args.action, args.targets, revalidate=revalidate, from_plugin_cache=from_plugin_cache)
    what = _what(args.action, args.targets, revalidate=revalidate, from_plugin_cache=from_plugin_cache)
    print(f"requested {what} from the next sync ({key})")
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
        help="write a Markdown comment for the failures that became persistent this run into this "
        + "directory; none if there were none",
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
        "request",
        help="ask the next sync to do something, which it does before the rest of the queue",
        description="Ask the next sync to do something. It writes a request to the state bucket, which the next sync "
        + "takes, so it never contacts Galaxy.",
    )
    actions = p.add_subparsers(dest="action", required=True)
    a = actions.add_parser(
        "mirror",
        parents=[storage],
        help="mirror these collections, or every collection in these namespaces, ahead of everything else",
    )
    _ = a.add_argument("targets", nargs="+", metavar="NS[.NAME]")
    _ = a.add_argument(
        "--revalidate",
        action="store_true",
        help="also check every mirrored artifact against the one Galaxy publishes now",
    )
    _ = actions.add_parser(
        "sweep", parents=[storage], help="compare everything Galaxy publishes with the index now, not once a day"
    )
    a = actions.add_parser(
        "generate-plugins",
        parents=[storage],
        help="extract plugins again from the artifacts and work plugins.jsonl out again, over as many syncs as it takes",
    )
    _ = a.add_argument("targets", nargs="*", metavar="NS[.NAME]", help="none for every collection")
    _ = a.add_argument(
        "--from-plugin-cache",
        action="store_true",
        help="only fetch the artifacts the plugin cache needs, those whose extraction is missing or out of date",
    )
    a = actions.add_parser(
        "generate-indexes",
        parents=[storage],
        help="make index lines again from the artifacts, over as many syncs as it takes",
    )
    _ = a.add_argument("targets", nargs="*", metavar="NS[.NAME]", help="none for every collection")
    p.set_defaults(func=cmd_request, targets=[])

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
    except (UsageError, StorageError, AlaiIndexError) as e:
        parser.exit(2, f"{parser.prog}: error: {e}\n")


if __name__ == "__main__":
    sys.exit(main())
