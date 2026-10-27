"""Shapes of the JSON the sync reads from Galaxy and writes to the index.

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


# PyPI's JSON API, for ansible-core releases.


class PyPIDigests(TypedDict):
    sha256: str


class PyPIFile(TypedDict):
    """One file of a release in PyPI's JSON API (`/pypi/<project>/json`)."""

    packagetype: str  # "sdist" or "bdist_wheel"
    url: str
    digests: PyPIDigests
    yanked: NotRequired[bool]


class PyPIProject(TypedDict):
    releases: dict[str, list[PyPIFile]]


# Index and plugin index lines (docs/index-spec/).


class Dependency(TypedDict):
    name: str
    req: str


class IndexLine(TypedDict):
    v: int
    name: str
    vers: str
    deps: list[Dependency]
    cksum: str
    size: int
    requires_ansible: NotRequired[str]
    yanked: bool
    deprecated: NotRequired[bool]
    published: str


class PluginLine(TypedDict):
    v: int
    type: str
    name: str
    collection: str
    since: NotRequired[str]
    until: NotRequired[str]
    short_description: NotRequired[str]
    routing: NotRequired[dict[str, object]]


# Stored extraction results.


class PluginEntry(TypedDict):
    type: str
    name: str
    short_description: NotRequired[str]


class ExtractionData(TypedDict):
    """The part of an alai-plugin-extractor result the plugin index is built from."""

    format: int  # plugins.EXTRACTION_FORMAT when it was written
    plugins: list[PluginEntry]
    routing: dict[str, dict[str, object]]  # "type:name" -> meta/runtime.yml entry


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


class VersionState(TypedDict):
    created_at: str | None
    upstream: NotRequired[UpstreamVerdict]


class CollectionState(TypedDict):
    versions: dict[str, VersionState]
    updated: NotRequired[str]
    sha256: NotRequired[str]  # of its index file, the key it's published under; none until it has one
    upstream_updated: NotRequired[str]
    upstream_deprecated: NotRequired[bool]
    upstream_count: NotRequired[int]  # versions Galaxy listed at the last reconcile
    # Versions Galaxy serves that its search index (the upload feed and count) leaves out.
    unindexed: NotRequired[list[str]]


class Observation(TypedDict):
    """One artifact Galaxy served for a conflicted version."""

    sha256: str
    uploaded: str | None  # Galaxy's created_at for that upload
    seen: str  # when a reconcile first compared it
    changes: str  # files added, removed and changed from ours, or why they couldn't be compared
    saved: str  # state bucket key of the artifact; never published


class Acknowledgement(TypedDict):
    at: str
    note: str


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
    acknowledged: NotRequired[Acknowledgement]  # a person looked at it; no longer reported
    resolved: NotRequired[str]  # when Galaxy served our contents again


class Pending(TypedDict):
    """A collection waiting for a reconcile."""

    source: str  # why it's queued, one of store.SOURCES
    queued: str
    revalidate: NotRequired[bool]
    reasons: NotRequired[list[str]]  # what change detection saw, e.g. "updated_at changed"
    uploads: NotRequired[list[str]]  # versions the upload feed saw
    unseen: NotRequired[list[str]]  # mirrored versions the sweep didn't see in the upload feed


class BuiltinState(TypedDict):
    checked: str  # when PyPI was last checked for a new ansible-core minor release
    version: NotRequired[str]  # the ansible-core release builtin.jsonl was extracted from
    format: NotRequired[int]  # its plugins.EXTRACTION_FORMAT


class RebuildState(TypedDict):
    requested: str
    after: NotRequired[str]  # the last collection rebuilt, in (namespace, name) order


class MigrationState(TypedDict):
    after: NotRequired[str]  # the last collection rewritten without the dropped fields, in (namespace, name) order
    done: NotRequired[bool]  # every index file is without them


class SyncState(TypedDict):
    collections: dict[str, CollectionState]
    conflicts: list[Conflict]
    failures: dict[str, Failure]  # keyed by "<fqcn>@<version or *>#<kind>"
    pending: dict[str, Pending]  # keyed by fqcn
    feed_cursor: NotRequired[str]  # pulp_created of the newest upload discover has seen
    repo_published: NotRequired[str]  # repo metadata `published` at the last discover
    last_sweep: NotRequired[str]  # when the last full sweep finished
    published_count: NotRequired[int]  # versions Galaxy published at the last count check
    plugins_pending: NotRequired[list[str]]  # collections whose plugin lines aren't in plugins.jsonl yet
    builtin: NotRequired[BuiltinState]
    rebuild: NotRequired[RebuildState]  # a requested rebuild that isn't finished yet
    migration: NotRequired[MigrationState]  # rewriting index files without fields dropped from the spec
    retired_index_files: NotRequired[dict[str, str]]  # index bucket keys no listing names, and since when
    last_prune: NotRequired[str]  # when retired index files were last pruned


# requests/ in the state bucket.


class Request(TypedDict):
    """A `refresh` asking the next sync to reconcile collections, sweep, rebuild or acknowledge conflicts."""

    requested: str
    targets: list[str]  # NS or NS.NAME; empty for a sweep, unless it acknowledges
    revalidate: bool
    acknowledge: NotRequired[list[str]]  # NS.NAME:VERSION conflicts to acknowledge, instead of targets
    note: NotRequired[str]  # why, for the acknowledgements
    rebuild: NotRequired[bool]  # regenerate every file derived from the index, instead of targets
    dl: NotRequired[str]  # a new artifact URL template for config.json, instead of targets
