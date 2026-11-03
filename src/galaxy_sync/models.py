"""Shapes of the JSON the sync reads from Galaxy, the index and its own state.

Galaxy responses only list the fields the sync uses. They aren't
validated at runtime; a response missing a required field fails the
collection, which the failure ledger records.
"""

from typing import NotRequired, TypedDict

# Galaxy's contract endpoints (docs/galaxy-api.md).


class GalaxyCollection(TypedDict):
    created_at: str
    updated_at: str
    deprecated: NotRequired[bool]  # best effort, not part of the contract


class GalaxyVersion(TypedDict):
    version: str
    created_at: NotRequired[str | None]
    requires_ansible: NotRequired[str | None]


class GalaxyLinks(TypedDict):
    next: NotRequired[str | None]


class GalaxyCount(TypedDict):
    count: int


class GalaxyPage(TypedDict):
    """One page of any paginated listing; the items' shape depends on the endpoint."""

    # Galaxy returns `data`; older servers used `results`.
    data: NotRequired[list[object]]
    results: NotRequired[list[object]]
    meta: NotRequired[GalaxyCount]
    links: NotRequired[GalaxyLinks | None]


class GalaxyArtifact(TypedDict):
    sha256: str


class GalaxyVersionMetadata(TypedDict):
    dependencies: NotRequired[dict[str, str] | None]


class GalaxyVersionDetail(TypedDict):
    download_url: str
    artifact: GalaxyArtifact
    metadata: GalaxyVersionMetadata
    created_at: NotRequired[str | None]
    requires_ansible: NotRequired[str | None]


# Galaxy's best-effort endpoints.


class GalaxyHighestVersion(TypedDict):
    version: str


class GalaxyListedCollection(TypedDict):
    namespace: str
    name: str
    updated_at: NotRequired[str]
    deprecated: NotRequired[bool]
    highest_version: NotRequired[GalaxyHighestVersion | None]


class GalaxyRepoMetadata(TypedDict):
    published: str  # pulp_created of the repo's latest version


class GalaxySearchCollectionVersion(TypedDict):
    namespace: str
    name: str
    version: str
    pulp_created: str


class GalaxySearchRow(TypedDict):
    collection_version: GalaxySearchCollectionVersion


class GalaxySearchPage(TypedDict):
    meta: GalaxyCount


# Index lines (docs/index-spec/), as alai-index writes them.


class Dependency(TypedDict):
    name: str
    req: str


class IndexLine(TypedDict):
    v: int
    name: str
    vers: str
    deps: list[Dependency]
    sha256: str
    size: int
    requires_ansible: NotRequired[str]
    yanked: bool
    deprecated: NotRequired[bool]
    published: str


# alai-index's JSON output (alai-index's README.md, "Results").


class IndexChange(TypedDict):
    name: str
    version: str | None  # None for the whole collection
    result: str  # added, changed, conflict, deferred, failed, ...
    details: dict[str, object] | None


# dl is relative to the artifacts' directory, so the dl bucket's keys, index is relative to the tree, in the order to
# publish them, and plugin-cache is relative to the plugin cache.
IndexFiles = TypedDict("IndexFiles", {"dl": list[str], "index": list[str], "plugin-cache": list[str]})


class IndexOutput(TypedDict):
    versions: list[IndexChange]
    written: IndexFiles
    deleted: IndexFiles
    stale: list[str]
    # Versions whose plugin extraction is missing or out of date: name, version, and dl, their artifact's key.
    outdated: list[dict[str, str]]


# state.json (docs/storage.md).


class Failure(TypedDict):
    collection: str
    version: str | None
    kind: str
    message: str
    first_seen: str
    last_seen: str
    attempts: int
    alerted: NotRequired[str]  # when a run first reported it as persistent


class UpstreamVerdict(TypedDict):
    """The last comparison of Galaxy's artifact with ours, when their sha256s differ."""

    sha256: str  # Galaxy's artifact
    same_contents: bool
    uploaded: NotRequired[str | None]  # Galaxy's created_at for it, so the sweep doesn't see it as a new upload


class CollectionState(TypedDict):
    upstream_updated: NotRequired[str]
    upstream_deprecated: NotRequired[bool]
    upstream_count: NotRequired[int]  # versions Galaxy listed at the last reconcile
    # Versions Galaxy serves that its search index (the upload feed and count) leaves out.
    unindexed: NotRequired[list[str]]
    mirrored: NotRequired[int]  # versions in the index that aren't yanked, at the last reconcile
    # The last comparison of Galaxy's artifact with ours, by version, for versions where they differ.
    verdicts: NotRequired[dict[str, UpstreamVerdict]]


class Observation(TypedDict):
    """One artifact Galaxy served for a conflicted version."""

    sha256: str
    uploaded: str | None  # Galaxy's created_at for that upload
    seen: str  # when a reconcile first compared it
    changes: str  # files added, removed and changed from ours, or why they couldn't be compared
    saved: str  # state bucket key of the artifact; never published


class Conflict(TypedDict):
    """Galaxy serves different contents for a mirrored version. Ours is kept.

    A version has at most one unresolved conflict, which collects every
    differing artifact Galaxy serves until it's resolved.
    """

    collection: str
    version: str
    ours: str  # sha256 in the index
    opened: str
    observed: list[Observation]  # oldest first
    resolved: NotRequired[str]  # when Galaxy served our contents again


class Pending(TypedDict):
    """A collection waiting for a reconcile."""

    source: str  # why it's queued, one of store.SOURCES
    queued: str
    revalidate: NotRequired[bool]
    reasons: NotRequired[list[str]]  # what change detection saw, e.g. "updated_at changed"
    uploads: NotRequired[list[str]]  # versions the upload feed saw
    unseen: NotRequired[list[str]]  # mirrored versions the sweep didn't see in the upload feed


class Job(TypedDict):
    """A requested job worked a collection at a time, over as many syncs as it takes."""

    action: str  # generate-plugins or generate-indexes
    requested: str
    targets: list[str]  # NS or NS.NAME, or empty for every collection
    from_plugin_cache: NotRequired[bool]  # generate-plugins: fetch only the artifacts the plugin cache needs
    after: NotRequired[str]  # the last collection done, in name order


class SyncState(TypedDict):
    collections: dict[str, CollectionState]
    conflicts: list[Conflict]
    failures: dict[str, Failure]  # keyed by "<fqcn>@<version or *>#<kind>"
    pending: dict[str, Pending]  # keyed by fqcn
    feed_cursor: NotRequired[str]  # pulp_created of the newest upload discover has seen
    repo_published: NotRequired[str]  # repo metadata `published` at the last discover
    last_sweep: NotRequired[str]  # when the last full sweep finished
    published_count: NotRequired[int]  # versions Galaxy published at the last count check
    jobs: NotRequired[list[Job]]  # requested jobs that aren't finished yet, worked in order
    retired_index_files: NotRequired[dict[str, str]]  # index bucket keys no listing names, and since when
    last_prune: NotRequired[str]  # when retired index files were last pruned


# requests/ in the state bucket.


class Request(TypedDict):
    """A `request` asking the next sync to do something, one of store.ACTIONS."""

    requested: str
    action: str
    targets: list[str]  # NS or NS.NAME, or for generate-*, empty for every collection
    revalidate: NotRequired[bool]  # mirror: check every mirrored artifact against Galaxy's
    from_plugin_cache: NotRequired[bool]  # generate-plugins: fetch only the artifacts the plugin cache needs
