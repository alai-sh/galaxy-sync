"""Finding which collections need a reconcile, from Galaxy's best-effort endpoints.

`discover` is the frequent, incremental check behind `sync`; `sweep` is the
full comparison behind `refresh`; `check_count` spots deletions neither can
see. None of them change the index: they only pick collections for
reconcile, which checks the contract endpoints before yanking anything.
See docs/sync.md.
"""

import datetime
import logging
from dataclasses import dataclass, field

from . import semver
from .galaxy import GalaxyClient, GalaxyError
from .reconcile import NAME, check_name
from .store import GLOBAL, Store
from .times import rfc3339

log = logging.getLogger(__name__)

type Collection = tuple[str, str]  # (namespace, name)

# How far before the cursor the upload feed is read again. `pulp_created` is
# the upload time, but a version only joins `published` once approved, and
# offset paging can shift under concurrent changes. Re-reading costs a no-op
# reconcile per collection.
FEED_OVERLAP = datetime.timedelta(hours=1)


def _valid(namespace: str, name: str) -> bool:
    try:
        check_name(namespace, name)
    except ValueError:
        log.warning("skipping invalid collection name %s.%s", namespace, name)
        return False
    return True


def check_target(target: str) -> None:
    """Raise ValueError unless `target` is a valid `ns` or `ns.name`."""
    if "." in target:
        check_name(*target.split(".", 1))
    elif not NAME.fullmatch(target):
        raise ValueError(f"invalid namespace {target}")


def split_version_target(target: str) -> tuple[str, str]:
    """Split `ns.name:version` into the collection and version; ValueError if it isn't one."""
    fqcn, _, vers = target.partition(":")
    if "." not in fqcn or not vers:
        raise ValueError(f"expected NS.NAME:VERSION, got {target}")
    check_name(*fqcn.split(".", 1))
    return fqcn, vers


def resolve_targets(client: GalaxyClient, targets: list[str]) -> list[Collection]:
    """Expand `ns` or `ns.name` targets into collections. Raises ValueError for a bad name."""
    result: list[Collection] = []
    for target in targets:
        check_target(target)
        if "." in target:
            ns, name = target.split(".", 1)
            result.append((ns, name))
        else:
            result.extend(
                (c["namespace"], c["name"])
                for c in client.list_collections(target)
                if _valid(c["namespace"], c["name"])
            )
    return result


def _listed_changes(client: GalaxyClient, store: Store) -> dict[Collection, str]:
    """Collections the full listing says need a reconcile, with why.

    New collections, and ones whose `deprecated` flag or `updated_at` differs
    from the last reconcile. Galaxy's `updated_at` is the timestamp of the
    collection's *highest* version, so it only catches changes to that one.
    Mirrored collections missing from the listing are returned too. Raises
    GalaxyError if any page fails, since a partial listing can't say what's
    missing.
    """
    log.info("reading the collection listing")
    listed: set[Collection] = set()
    reasons: dict[str, set[Collection]] = {
        "new": set(),
        "updated_at changed": set(),
        "deprecation changed": set(),
        "gone from the listing": set(),
    }
    for item in client.list_collections():
        ns, name = item["namespace"], item["name"]
        if not _valid(ns, name):
            continue
        listed.add((ns, name))
        cstate = store.state["collections"].get(f"{ns}.{name}")
        if cstate is None:
            reasons["new"].add((ns, name))
        elif cstate.get("upstream_updated") != item.get("updated_at"):
            reasons["updated_at changed"].add((ns, name))
        elif cstate.get("upstream_deprecated", False) != bool(item.get("deprecated")):
            reasons["deprecation changed"].add((ns, name))

    for fqcn, cstate in store.state["collections"].items():
        ns, _, name = fqcn.partition(".")
        if (ns, name) not in listed and "upstream_updated" in cstate:
            reasons["gone from the listing"].add((ns, name))
    log.info(
        "collection listing: %d collections; %s",
        len(listed),
        ", ".join(f"{len(found)} {reason}" for reason, found in reasons.items()),
    )
    for reason, found in reasons.items():
        for ns, name in sorted(found):
            log.debug("%s.%s: %s in the listing", ns, name, reason)
    return {c: reason for reason, found in reasons.items() for c in found}


def try_listing(client: GalaxyClient, store: Store) -> dict[Collection, str] | None:
    """The listing's changes, or None (with `*/listing` recorded) if it failed."""
    try:
        found = _listed_changes(client, store)
    except GalaxyError as e:
        store.record_failure(GLOBAL, "listing", e.kind, f"listing collections: {e}")
        return None
    store.clear_failures(GLOBAL, "listing")
    return found


def _feed_changes(
    client: GalaxyClient, cursor: str | None
) -> tuple[str | None, dict[Collection, set[str]], set[Collection]]:
    """Walk the upload feed back to `cursor`.

    Returns the new cursor, the versions seen by collection, and the
    collections with an upload newer than `cursor`. With no cursor only the
    newest entry is read, to start one. Entries up to FEED_OVERLAP older than
    the cursor are included again.
    """
    since = datetime.datetime.fromisoformat(cursor) if cursor else None
    stop = since - FEED_OVERLAP if since else None
    if stop:
        log.info("reading the upload feed back to %s", stop.isoformat())
    else:
        log.info("reading the newest upload to start the feed cursor")
    newest: str | None = None
    found: dict[Collection, set[str]] = {}
    fresh: set[Collection] = set()
    for row in client.published_feed():
        cv = row["collection_version"]
        newest = newest or cv["pulp_created"]
        if stop is None or datetime.datetime.fromisoformat(cv["pulp_created"]) < stop:
            break
        if _valid(cv["namespace"], cv["name"]):
            found.setdefault((cv["namespace"], cv["name"]), set()).add(cv["version"])
            if since is None or datetime.datetime.fromisoformat(cv["pulp_created"]) > since:
                fresh.add((cv["namespace"], cv["name"]))
    if newest is None or (since is not None and datetime.datetime.fromisoformat(newest) < since):
        # Nothing newer, and the cursor's own version may have been deleted.
        newest = cursor
    if stop:
        log.info("upload feed: %d collections have new uploads; the newest is from %s", len(found), newest)
    return newest, found, fresh


@dataclass
class Discovery:
    collections: list[Collection] = field(default_factory=list)
    uploads: dict[Collection, list[str]] = field(default_factory=dict)  # the versions the upload feed found
    new_uploads: set[Collection] = field(default_factory=set)  # those uploaded since the cursor, not just the overlap
    listing: dict[Collection, str] = field(default_factory=dict)  # what the listing found, and why
    cursor: str | None = None  # save as `feed_cursor` once `collections` are queued
    published: str | None = None  # save as `repo_published` once `collections` are queued
    unchanged: bool = False  # the repo hasn't changed since the last run, so nothing else was read
    feed: bool = False  # the upload feed was read
    listed: bool = False  # the collection listing was read
    hinted: bool = False  # the listing ran because the repo changed without a new upload

    @property
    def ok(self) -> bool:
        """Whether anything could be read; if not, nothing is known to be up to date."""
        return self.unchanged or self.feed or self.listed


def discover(client: GalaxyClient, store: Store) -> Discovery:
    """Find collections that changed since the last run.

    The repo metadata says whether the `published` repo changed at all since
    `repo_published`; if not, that one request is the whole run. Otherwise the
    upload feed gives every version uploaded since `feed_cursor`, including
    releases below a collection's highest version.

    The collection listing also runs when there's no cursor yet (the initial
    import), when the feed fails, or when the repo changed but nothing was
    uploaded: a deprecation, a deleted collection or highest version, or a
    namespace edit. Failures are recorded under GLOBAL and never raise.
    """
    res = Discovery()
    cursor = store.state.get("feed_cursor")
    previous = store.state.get("repo_published")
    try:
        res.published = client.repo_published()
    except GalaxyError as e:
        # Only an optimisation: carry on as if the repo changed.
        store.record_failure(GLOBAL, "published", e.kind, f"repo metadata: {e}")
        log.info("can't tell whether the published repo changed; assuming it did")
    else:
        store.clear_failures(GLOBAL, "published")
        log.info("published repo last changed %s; the last run saw %s", res.published, previous or "nothing")
        if cursor is not None and res.published == previous:
            res.unchanged = True
            res.cursor = cursor
            return res

    list_all = cursor is None
    if list_all:
        log.info("no feed cursor yet (the initial import): every collection will be listed")
    try:
        res.cursor, uploads, res.new_uploads = _feed_changes(client, cursor)
        res.uploads = {c: _sorted(versions) for c, versions in uploads.items()}
    except GalaxyError as e:
        store.record_failure(GLOBAL, "feed", e.kind, f"upload feed: {e}")
        log.info("the upload feed failed: falling back to the collection listing")
        list_all = True
        res.published = None  # the gate must not skip what the feed missed
    else:
        res.feed = True
        store.clear_failures(GLOBAL, "feed")
        if cursor is not None and res.cursor == cursor and res.published is not None and previous is not None:
            log.info("the repo changed without a new upload (a deletion, deprecation or namespace edit)")
            res.hinted = list_all = True

    if list_all:
        if (listed := try_listing(client, store)) is None:
            res.published = None  # nor what the listing missed
        else:
            res.listed = True
            res.listing = listed

    res.collections = sorted(res.uploads.keys() | res.listing.keys())
    return res


@dataclass
class Sweep:
    gone: dict[str, list[str]] = field(default_factory=dict)  # mirrored, not yanked, no longer published
    unmirrored: dict[str, list[str]] = field(default_factory=dict)  # published, not in the index
    replaced: dict[str, list[str]] = field(default_factory=dict)  # mirrored, re-uploaded since
    # Mirrored and not in the feed, but a reconcile found Galaxy still serves them.
    unindexed: dict[str, list[str]] = field(default_factory=dict)
    published: int = 0  # versions the feed listed
    listed: dict[Collection, str] | None = None  # from the collection listing, with why; None if it failed

    @property
    def collections(self) -> list[Collection]:
        names = set(self.gone) | set(self.unmirrored) | set(self.replaced) | set(self.unindexed)
        found = {(ns, name) for ns, _, name in (fqcn.partition(".") for fqcn in names)}
        return sorted(found | (self.listed or {}).keys())

    def reasons(self, collection: Collection) -> list[str]:
        """What the sweep saw for a collection it queued."""
        fqcn = ".".join(collection)
        found = [
            f"{label}: {few(versions)}"
            for label, by in (
                ("gone", self.gone),
                ("not mirrored", self.unmirrored),
                ("re-uploaded", self.replaced),
                ("unindexed", self.unindexed),
            )
            if (versions := by.get(fqcn))
        ]
        if reason := (self.listed or {}).get(collection):
            found.append(reason)
        return found

    def unseen(self, collection: Collection) -> list[str]:
        """Mirrored versions of a collection the feed didn't list."""
        fqcn = ".".join(collection)
        return [*self.gone.get(fqcn, []), *self.unindexed.get(fqcn, [])]


def few(versions: list[str], shown: int = 3) -> str:
    """The first few versions, and how many more there are."""
    more = f" and {len(versions) - shown} more" if len(versions) > shown else ""
    return ", ".join(versions[:shown]) + more


def _sorted(versions: set[str]) -> list[str]:
    return sorted(versions, key=semver.sort_key)


def sweep(client: GalaxyClient, store: Store) -> Sweep:
    """Compare every published version on Galaxy with the index.

    Reads the whole upload feed (~530 pages) and finds versions deleted on
    Galaxy but not yet yanked, published but not mirrored (missed uploads,
    failures, or a yanked version that's back), and re-uploaded since the last
    reconcile (their upload time no longer matches). Then reads the listing
    for deprecation changes; if only that fails, the rest still stands.

    New uploads during the walk shift rows to later pages, so they're seen
    twice, never skipped. A delete during the walk can shift a row past us,
    making it look gone; reconcile checks before yanking. Galaxy's search
    index can also lose versions it still serves: once a reconcile has found
    them served, they're reported as unindexed rather than gone, and still
    queued so reconcile notices if they're deleted. Raises GalaxyError
    if any feed page fails, since a partial walk would make everything after
    it look deleted.
    """
    published: dict[str, dict[str, str | None]] = {}  # fqcn -> version -> upload time
    res = Sweep()
    log.info("reading the whole upload feed")
    for row in client.published_feed():
        cv = row["collection_version"]
        if _valid(cv["namespace"], cv["name"]):
            res.published += 1
            published.setdefault(f"{cv['namespace']}.{cv['name']}", {})[cv["version"]] = rfc3339(cv["pulp_created"])

    mirrored: set[str] = set()
    for ns, name in store.index_collections():
        fqcn = f"{ns}.{name}"
        mirrored.add(fqcn)
        upstream = published.get(fqcn, {})
        have = {line["vers"] for line in store.read_index(ns, name) if not line["yanked"]}
        cstate = store.collection_state(fqcn)
        seen = cstate["versions"]
        replaced = {
            v
            for v in have & upstream.keys()
            if (state := seen.get(v)) and state["created_at"] and upstream[v] and state["created_at"] != upstream[v]
        }
        unlisted = have - upstream.keys()
        # Those back in the feed are indexed again.
        if unindexed := set(cstate.get("unindexed", [])) & unlisted:
            cstate["unindexed"] = _sorted(unindexed)
        else:
            _ = cstate.pop("unindexed", None)
        for found, versions in (
            (res.gone, unlisted - unindexed),
            (res.unmirrored, upstream.keys() - have),
            (res.unindexed, unindexed),
        ):
            if versions:
                found[fqcn] = _sorted(versions)
        if replaced:
            res.replaced[fqcn] = _sorted(replaced)
    for fqcn, versions in published.items():
        if fqcn not in mirrored:
            res.unmirrored[fqcn] = _sorted(set(versions))
    for label, found in (("not mirrored", res.unmirrored), ("re-uploaded", res.replaced), ("unindexed", res.unindexed)):
        for fqcn, versions in sorted(found.items()):
            log.debug("%s: %s %s", fqcn, label, ", ".join(versions))
    res.listed = try_listing(client, store)
    return res


def check_count(client: GalaxyClient, store: Store) -> int | None:
    """Compare Galaxy's published version count with what the mirror last saw.

    Deleting a version that isn't a collection's highest changes nothing
    `discover` can see, but it lowers the count. Returns how many versions are
    unaccounted for (recorded as `deletions_suspected`, cleared once they
    match), or None if the count couldn't be read. Run it after reconciling:
    versions uploaded since only raise Galaxy's count, so they can hide a
    deletion until the next run but never fake one. Versions a reconcile found
    served but missing from the search index aren't in Galaxy's count, so
    they aren't expected either.
    """
    try:
        count = client.published_count()
    except GalaxyError as e:
        store.record_failure(GLOBAL, "count", e.kind, f"published count: {e}")
        return None
    store.clear_failures(GLOBAL, "count", exclude=("deletions_suspected",))
    store.state["published_count"] = count
    collections = store.state["collections"].values()
    unindexed = sum(len(c.get("unindexed", [])) for c in collections)
    expected = sum(c.get("upstream_count", 0) for c in collections) - unindexed
    missing = expected - count
    log.info(
        "Galaxy publishes %d versions; the mirror last saw %d%s",
        count,
        expected,
        f", not counting {unindexed} missing from its search index" if unindexed else "",
    )
    if missing > 0:
        store.record_failure(
            GLOBAL,
            "count",
            "deletions_suspected",
            f"Galaxy publishes {count} versions but the mirror last saw {expected}; the next sweep finds and yanks them",
        )
    else:
        store.clear_failures(GLOBAL, "count", kinds=("deletions_suspected",))
    return max(missing, 0)
