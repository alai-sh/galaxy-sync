# alai index format

**Status:** draft, format `1`.

This document specifies the static index that the alai client reads to
resolve and download Ansible collections. It's a set of plain files with no
server-side logic, so any static host (R2, S3, a web server, a local
directory) can serve an index. Copying the files is enough to make a private
or offline mirror.

The design is modelled on Cargo's sparse registry protocol
([RFC 2789](https://rust-lang.github.io/rfcs/2789-sparse-index.html), and
[the Cargo registry index docs](https://doc.rust-lang.org/cargo/reference/registry-index.html)):
a `config.json` at the root, one file per package, one JSON line per version.
Field names follow Cargo's where the meaning is the same. The line schema,
paths and dependency syntax are Ansible-specific. [Differences from
Cargo](#differences-from-cargo) lists every deviation.

The plugin index used by `find` is specified in [`plugins.md`](plugins.md).
A worked example lives in [`example/`](example/).

The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are used as in
RFC 2119.

## Overview

An index has two roots, which MAY be on different hosts:

```
<index root>                             e.g. https://index.alai.sh/
  config.json                            how to read this index
  collections.json                       every collection, with last-updated times and sha256s
  index/<namespace>/<name>.jsonl         one line per collection version, found by name
    or index/<aa>/<sha256>.jsonl         or by sha256, as config.json's index template says
  conflicts/<namespace>/<name>.json      mirrors: why versions differ from the origin

<artifact root>                          e.g. https://dl.alai.sh/
  sha256/<aa>/<sha256>.tar.gz            collection tarballs, content-addressed
```

Resolving a collection:

1. Fetch `config.json`, and pick the highest format the client supports.
2. Decide which index the requested collection comes from (see [Client
   rules](#client-rules)). `collections.json` says whether an index has it
   without probing for a 404.
3. Fetch the collection's [index file](#index-files), from the `index`
   template, and for each dependency fetch its file from the same index.
   Requests SHOULD run in parallel.
4. Resolve offline using the lines.
5. Download each chosen artifact using the `dl` template, and verify its
   sha256 against `cksum`.

## Conventions

- **Encoding:** every file is UTF-8 JSON. `.jsonl` files contain one JSON
  object per line, separated by `\n`. Blank lines MUST be ignored.
- **Unknown fields:** clients MUST ignore fields they don't recognize, in
  every file. Strict deserializers that reject unknown keys are not
  conforming. This is what allows the format to grow without a version bump.
  See [Versioning](#versioning).
- **Key naming:** following Cargo, `config.json` keys are kebab-case
  (`moved-to`). Keys in every other file are snake_case
  (`requires_ansible`, `short_description`).
- **Timestamps:** RFC 3339 in UTC with a `Z` suffix and second precision,
  e.g. `2026-10-05T17:30:02Z`.
- **Collection names:** `<namespace>.<name>`. Both parts MUST be valid
  according to ansible-core's
  `AnsibleCollectionRef.is_valid_collection_name`: each part is a Python
  identifier and not a Python keyword. Additionally, indexes MUST only contain
  lowercase names (`[a-z0-9_]`). Galaxy enforces this already, and it gives
  case-sensitive hosts a single canonical path.
- **Versions:** semantic version strings (SemVer 2.0.0), stored exactly as
  published, including any pre-release (`-rc.1`) or build metadata
  (`+git940b822`) suffix. Clients sort by SemVer precedence. When two versions
  differ only in build metadata, clients tie-break by comparing the strings
  so the order is stable.
- **Version requirements** (`req`): Ansible's syntax, copied verbatim from
  the collection's `MANIFEST.json`. This is **not** Cargo's semver
  requirement syntax. See [Requirement syntax](#requirement-syntax).
- **`requires_ansible`:** a [PEP 440 version
  specifier](https://peps.python.org/pep-0440/#version-specifiers), such as
  `>=2.17.0` or `>=2.19.0b1`, copied verbatim from the collection's
  `meta/runtime.yml`. ansible-core versions are PEP 440, not SemVer. See
  [`requires_ansible` syntax](#requires_ansible-syntax).

## `config.json`

Lives at the index root. It is the only file whose location is fixed, so its
schema only ever gains fields and never changes incompatibly.

```json
{
  "formats": {
    "1": {
      "index": "index/{namespace}/{name}.jsonl",
      "collections": "collections.json"
    }
  },
  "dl": "https://dl.alai.sh/sha256/{sha256-prefix}/{sha256-checksum}.tar.gz",
  "moved-to": null
}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `formats` | object | yes | Map of format number (as a string) to the paths for that format, relative to the index root. An index MAY serve several formats at once during a transition. Clients MUST pick the highest format they support, and MUST fail with a clear error ("this index requires a newer alai") if they support none |
| `formats.<n>.index` | string | yes | URL template for each collection's [index file](#index-files), with the placeholders in [The `index` template](#the-index-template) |
| `formats.<n>.collections` | string | yes | Path of the [collection listing](#collectionsjson) |
| `formats.<n>.plugins` | string | no | Path of the [plugin index](plugins.md). Absent if the index has none |
| `dl` | string | yes | Artifact URL template. See [Artifacts](#artifacts) |
| `moved-to` | string or null | no | If set, the index has moved to this index root URL. Clients SHOULD follow it, tell the user to update their configuration, and MAY remember it. Clients MUST NOT follow a `moved-to` that downgrades from `https` to `http` |

The names `api` (a publishing endpoint) and `auth-required` (both from Cargo)
are reserved. An index without `api` is read-only.

The paths in `formats`, and the `index` template once filled in, are URL
references resolved against the index root as RFC 3986 says: relative, or
absolute and on another host. The index root ends in `/`, so a path without
a leading `/` stays under it. Clients MUST refuse one that leads from an
`https` index to anything but `https`, or from an index that isn't on the
client's own disk to a `file:` URL.

### The `index` template

`formats.<n>.index` says where each collection's index file is. A client
fills in its placeholders for the collection it wants, then resolves the
result against the index root:

| Placeholder | Replaced with |
|---|---|
| `{namespace}` | The collection's namespace |
| `{name}` | The collection's name, without the namespace |
| `{sha256-checksum}` | The collection's `sha256` in [`collections.json`](#collectionsjson): the SHA-256 of its index file, not of an artifact |
| `{sha256-prefix}` | The first two characters of that `sha256` |

The template MUST name each file on its own: it has both `{namespace}` and
`{name}`, or `{sha256-checksum}`. `{sha256-prefix}` alone doesn't, and is
for spreading files across directories. Any other text is kept as it is.
Which placeholders an index uses decides how it works:

- **By name**, such as `index/{namespace}/{name}.jsonl`, which gives
  `index/community/general.jsonl`. A collection's file keeps its URL and is
  replaced in place when it changes, so it's served with a short cache
  lifetime ([HTTP behaviour](#http-behaviour)). A client can build the URL
  without `collections.json`, and an index can generate files on request.
  `sha256` in `collections.json` stays optional.
- **By SHA-256**, such as `index/{sha256-prefix}/{sha256-checksum}.jsonl`,
  which gives `index/a4/a4472ff7….jsonl`. A file's URL changes with its
  content, so a file never changes and can be cached as long as an artifact.
  Only `collections.json` says where a collection's file is, which makes it
  required reading, and its `sha256` required in every entry (see
  [below](#index-templates-by-sha-256)). The index MUST keep each file
  available for at least 7 days after the last `collections.json` that
  named it was replaced, longer than any copy of `collections.json` lives in
  a cache, and MAY delete it after that.

A template can use both kinds, such as
`index/{namespace}/{name}-{sha256-checksum}.jsonl`; any template with
`{sha256-checksum}` or `{sha256-prefix}` is by SHA-256.

## `collections.json`

Lists every collection in the index, grouped by namespace, with the time its
index file last changed.

```json
{
  "v": 1,
  "namespaces": {
    "alai_example": {
      "demo": {"updated": "2026-09-20T09:00:00Z", "sha256": "605a4d6889b5538c4898bf11276f380f8146b5e017735926cf9516c9432ebf9b"}
    },
    "community": {
      "general": {"updated": "2026-10-05T17:30:02Z", "sha256": "a4472ff7659bc3d80663664d2abe233c48b8cc40af100c5d43b4efb4dc1a29cf"},
      "library_inventory_filtering_v1": {"updated": "2025-10-25T11:31:55Z", "sha256": "6cdcc7c93ebabc19be364ff579267dce86961c1c0f4fd24ee7b530597b2c007b"}
    }
  }
}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `v` | integer | yes | Schema version of this file. Currently `1` |
| `namespaces` | object | yes | Map of namespace to its collections |
| `namespaces.<ns>.<name>` | object | yes | One entry per collection that has an index file |
| `namespaces.<ns>.<name>.updated` | string | yes | When that collection's index file last changed for any reason: a new version, a yank or a deprecation (RFC 3339) |
| `namespaces.<ns>.<name>.sha256` | string | no | Lowercase hex SHA-256 of that collection's index file, over its bytes exactly as stored (before any transfer compression). Lets a client tell whether its cached copy is current without a request; see [Cached index files](#cached-index-files). MUST be in every entry when the `index` template is [by SHA-256](#index-templates-by-sha-256); otherwise indexes SHOULD include it |

What it's for:

- **Choosing an index** without probing each one for a 404.
- **Enumerating an index**, e.g. for name completion, "did you mean…?", or
  copying collections into another index.
- **A change feed for mirrors.** A downstream mirror polls this one file,
  compares `updated` with what it last saw, and fetches only the index files
  that changed. A collection that disappears from the listing was removed
  entirely.
- **Skipping unchanged index files.** A client compares each `sha256` with its
  cached copies and only requests the files that changed, so a resolve where
  nothing changed costs this file and `config.json`, however many
  collections it needs. See [Cached index files](#cached-index-files).

For the Galaxy mirror this is about 4.6k entries: roughly 720 KB raw and
250 KB compressed with `sha256` (340 KB and 60 KB without). It changes on
every publish, so it's served with a short cache lifetime (see
[HTTP behaviour](#http-behaviour)).

`sha256` is the full hash rather than a prefix because collection authors
control much of their index file's content and can predict the next one (the
same file with a version yanked, say). With a 64-bit prefix, finding a file
that shares its prefix with its own future yanked form takes about 2^32
attempts, after which clients that had cached it would never see the yank.

Descriptions, tags and other search data are deliberately left out. They
belong in the catalog (see [Planned additions](#planned-additions)), which is
larger and used less often.

### Index templates by SHA-256

When the [`index` template](#the-index-template) uses `{sha256-checksum}`
or `{sha256-prefix}`, `collections.json` is how a client finds index files,
not just a shortcut:

- **Every entry MUST have `sha256`.** A client treats an entry without one
  as an error for that collection, as it can't say where its file is.
- **A client MUST read `collections.json` before any index file**, and
  can't fetch a file for a collection it doesn't list. A collection that
  isn't listed isn't in the index, without probing for a 404.
- **It's the only file that changes**, apart from `config.json`, so how
  fresh it is decides how fresh the whole index is to a client. A stale
  copy only names older index files, which the index still keeps, so a
  client sees an older index, never a mix of old and new.
- **A changed collection gets a new `sha256` and a new URL.** A downstream
  mirror that polls `updated` fetches the file the new `sha256` names, and
  can reuse the `sha256` rather than hash the file again if it publishes
  the same bytes.

## Index files

Each collection's index file is where the
[`index` template](#the-index-template) in `config.json` says: by name, such
as `index/community/general.jsonl`, or by SHA-256, such as
`index/a4/a4472ff7….jsonl`.

Each line describes one version of the collection. Lines appear in **order of
publication**, so clients MUST NOT assume they're sorted by version.

```json
{"v":1,"name":"community.general","vers":"13.5.0","deps":[{"name":"community.library_inventory_filtering_v1","req":">=1.0.0"}],"cksum":"54598287e0ea0504aee2d32c884208ddf26f3ac41e0b4e773ff514af210787c4","size":2913455,"requires_ansible":">=2.18.0","yanked":false,"published":"2026-10-05T17:30:02Z"}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `v` | integer | yes | Schema version of this line. Clients MUST skip lines with a `v` higher than they support (see [Versioning](#versioning)) |
| `name` | string | yes | Full collection name, `<namespace>.<name>`. MUST match the file's path |
| `vers` | string | yes | The version |
| `deps` | array | yes | Collection dependencies, possibly empty. See below |
| `cksum` | string | yes | Lowercase hex sha256 of the artifact. For a version mirrored from Galaxy this equals Galaxy's `artifact.sha256` |
| `size` | integer | yes | Artifact size in bytes |
| `requires_ansible` | string | no | ansible-core version requirement from `meta/runtime.yml`, a [PEP 440 specifier](#requires_ansible-syntax). Absent means no constraint |
| `yanked` | bool | yes | See [Yanking and removal](#yanking-and-removal) |
| `deprecated` | `true` or string | no | The version is deprecated. A string is a message to show the user. Galaxy deprecation is collection-level, so for a version mirrored from Galaxy it appears on every line |
| `published` | string | yes | When this version was first published at its origin (RFC 3339). For a version mirrored from Galaxy this is Galaxy's `created_at`, which for versions published before Galaxy's 2023-05 migration is the migration time, not the real publication time |

### `deps` entries

```json
{"name": "community.general", "req": ">=13.0.0,<14.0.0"}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `name` | string | yes | Collection name of the dependency |
| `req` | string | yes | Version requirement in [Ansible's syntax](#requirement-syntax) |

### Reserved fields

These names are reserved for planned additions and MUST NOT be used for
anything else:

- `requirements`: non-collection dependencies, such as Python packages and
  system packages (bindep) from `meta/execution-environment.yml`.
- `deps[].registry`: an index root URL a dependency must come from, as in
  Cargo. Dependencies currently always resolve from the same index (see
  [Index completeness](#index-completeness)). This is reserved in case
  explicit cross-index dependencies are ever needed.

### Requirement syntax

Defined by ansible-core's
`ansible.galaxy.dependency_resolution.versioning.meets_requirements`, which
clients MUST match:

- A requirement is a comma-separated list of clauses, all of which must hold.
- A clause is an operator (`==`, `=`, `!=`, `>`, `>=`, `<`, `<=`) followed by
  a version, or a bare version (meaning `==`), or `*` (anything).
- The version in a clause is parsed loosely, so `>=1.0` means `>=1.0.0`.

Examples: `*`, `>=1.0.0`, `>=13.0.0,<14.0.0`, `!=2.1.0`, `1.2.3`.

Galaxy accepts requirements ansible-core can't read, such as npm-style
`^3.0.0`, so some lines carry them. Indexes copy them verbatim; see
[Client rules](#client-rules) for what clients do with them.

### `requires_ansible` syntax

A PEP 440 version specifier set, as ansible-core checks it in
`ansible.plugins.loader._does_collection_support_ansible_version` with
`packaging.specifiers.SpecifierSet`. ansible-core compares its own version
with any pre-release, post-release or dev part dropped, so 2.19.0rc1 meets
`>=2.19.0`. Clients MUST compare the same way.

Examples: `>=2.17.0`, `>=2.15.0,<2.19.0`, `>=2.19.0b1`.

### Line mutability

Once published, a line's `name`, `vers`, `deps`, `cksum`, `size`,
`requires_ansible` and `published` MUST NOT change. The only fields
that may change afterwards are `yanked` and `deprecated`.
A version's `cksum` never changes, for any reason.

## Yanking and removal

- **Yanked** (`"yanked": true`): the version stays in the index and its
  artifact stays available. Clients MUST NOT choose it in a new resolution,
  but MUST still install it when a lock file pins it. A version deleted on
  Galaxy is yanked in the mirror.
- **Removed:** for takedowns (malware, leaked secrets, legal requests), the
  line is deleted and the artifact removed. Lock files that pin it will fail
  to install. That's intended.

## Conflict records

When the origin serves a different artifact for a version the index holds,
and its contents differ from the one at `cksum` (a repack of the same files
doesn't count), the index keeps the original. A mirror SHOULD then publish a
conflict record for the collection at `conflicts/<namespace>/<name>.json`,
so anyone can see why the index's artifact differs from the origin's. A record lists every
conflict the collection has had, including settled ones, and entries are
never removed. A collection that never had one has no record (404).

```json
{
 "collection": "ns.coll",
 "conflicts": [
  {
   "version": "1.0.0",
   "ours": "1ec07d4f...",
   "opened": "2026-10-18T23:20:18Z",
   "observed": [
    {"sha256": "31526b65...", "uploaded": "2026-10-18T23:05:51Z", "seen": "2026-10-18T23:20:18Z", "changes": "added playbooks/a.yml; changed roles/b/meta/main.yml"},
    {"sha256": "8caedf6f...", "uploaded": "2026-10-18T23:42:26Z", "seen": "2026-10-19T00:06:49Z", "changes": "added playbooks/a.yml"}
   ],
   "acknowledged": {"at": "2026-10-19T09:00:00Z", "note": "The author republished 1.0.0 with new content"}
  }
 ]
}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `collection` | string | yes | `<namespace>.<name>` |
| `conflicts[].version` | string | yes | The version |
| `conflicts[].ours` | string | yes | The index's `cksum` for it |
| `conflicts[].opened` | string | yes | When the mirror first saw the origin serve different contents |
| `conflicts[].observed` | array | yes | Each differing artifact the origin served, oldest first: `sha256`, `uploaded` (the origin's upload time, or `null` if unknown), `seen` (when the mirror first compared it), and `changes` (the files added, removed and changed from ours, or why they couldn't be compared) |
| `conflicts[].acknowledged` | object | no | The mirror's operators inspected it and keep the original: `at`, and a `note` saying why |
| `conflicts[].resolved` | string | no | When the origin served the original's contents again |

A version has at most one conflict without `resolved`. Conflicts never
change the index: its line stays as it was, acknowledged or not.

## Index completeness

Every index MUST be self-contained: it can satisfy the dependencies of every
collection it hosts. Clients rely on this to resolve dependencies from the
same index as the collection that needs them.

- **At publish time:** whatever writes a version into the index MUST refuse
  it unless every `deps[].req` is matched by at least one non-yanked version
  already in the same index. Every dependency was itself checked when it was
  published, so checking direct dependencies is enough to cover the whole
  dependency graph.
- **On removal:** a takedown MUST NOT leave a requirement of a remaining
  non-yanked line unmatched. The writer either refuses the removal, or also
  yanks the versions that depend on it.
- **Yanking** is allowed even if it leaves a dependency matched only by
  yanked versions. Locked installs keep working, and new resolutions fail
  with a normal resolver error.
- **Mirrors** copy upstream content as-is, so they can only be as complete as
  the upstream. A Galaxy collection can declare a dependency that doesn't
  exist on Galaxy, and the mirror still includes it. Clients report the
  unresolvable dependency like any other resolver error.
- Completeness guarantees that each requirement can be met on its own, not
  that every combination resolves. Two collections can still require
  incompatible versions of the same dependency.

Private indexes usually meet this by copying the public collections they
depend on. Because artifacts are copied byte for byte, the `cksum` values are
unchanged and client caches share them.

## Artifacts

The `dl` template in `config.json` builds each artifact's URL. It MUST contain
at least one placeholder. Unlike Cargo, there is no default suffix appended
to a template without placeholders. Placeholders:

| Placeholder | Replaced with |
|---|---|
| `{sha256-checksum}` | `cksum` |
| `{sha256-prefix}` | First two characters of `cksum` |
| `{namespace}` | Namespace |
| `{name}` | Collection name, without the namespace |
| `{version}` | `vers`, percent-encoded (`+` becomes `%2B`) |

Artifacts are the exact tarballs published at the origin, byte for byte, and
are never repackaged. Clients MUST verify the sha256 of every download
against `cksum`, regardless of where it came from, and MUST reject a
mismatch.

### Trust model

As with crates.io, trust comes from HTTPS and `cksum`: clients trust the
index, and the hash proves the artifact is the one the index lists. For a
version mirrored from Galaxy, `cksum` also equals Galaxy's own sha256, so anyone can
cross-check the mirror.

Signatures aren't carried yet. No version on galaxy.ansible.com was signed as
of 2026-10-16, and the wider ecosystem is moving from GPG towards
Sigstore-style attestations (PyPI's PEP 740, npm provenance). They can be
added later as an optional field without a format version change.

## HTTP behaviour

- **Caching:** `config.json`, `collections.json` and index files found by
  name SHOULD be served with a short `Cache-Control` (e.g. `max-age=60`) and
  an `ETag`. Clients SHOULD revalidate with `If-None-Match` on every
  resolve, unless running offline, except for index files whose `sha256`
  shows the cached copy is current (see
  [Cached index files](#cached-index-files)). Index files found by SHA-256,
  and artifacts, never change and SHOULD be served with
  `Cache-Control: public, max-age=31536000, immutable`. These are advice:
  clients don't depend on any lifetime.
- **Query strings:** an index host MUST serve `collections.json` and index
  files found by name the same whatever query string is added, and MUST
  NOT leave the query string out of the cache key for them. Clients rely on
  a fresh query string to get past a stale cached copy.
- **Purging:** an index behind a CDN MAY purge each changed file from the
  CDN when it publishes, in the [publishing order](#publishing-order). This
  shrinks the window in which clients see stale data from the CDN's
  lifetime to however long the purge takes to spread, at the cost of the
  next request waiting for the origin. Purging only reaches the CDN, not
  proxies or browsers after it, so the `max-age` sent to clients stays
  short either way. An index whose files are found by name and that
  doesn't purge leaves a client briefly seeing a `collections.json` and an
  index file that disagree, which [Cached index files](#cached-index-files)
  handles; one by SHA-256 can't, as only `collections.json` changes. A host that wants its CDN to answer at
  once when its copy expires can add `stale-while-revalidate`, but not with
  `s-maxage`: shared caches treat `s-maxage` as `proxy-revalidate` and
  never serve an expired copy. Index files found by SHA-256 are never
  replaced, so there's nothing to purge for them.
- **Compression:** clients SHOULD send `Accept-Encoding` (gzip, br, zstd).
  Servers MAY compress. Index files compress well.
- **Content-Type:** clients MUST NOT depend on it.
- **Not found:** a `404`, `410` or `451` for an index file found by name
  means the collection doesn't exist in this index. Clients MAY cache that
  briefly. For one found by SHA-256, see
  [Cached index files](#cached-index-files). Other errors are failures,
  not "not found".
- **Redirects:** clients MUST follow redirects, but MUST NOT follow one from
  `https` to `http`.
- **Authentication** is the host's concern, not the format's. The public
  index needs none. A private index can put any authentication in front of
  the same files. Clients send credentials only to indexes the user has
  configured credentials for, and treat `401` as "credentials required".

## Versioning

There are three layers, each for a different kind of change.

1. **Adding an optional field: no version change.** Clients ignore fields
   they don't recognize, so new data is invisible to old clients. Most
   changes are of this kind.
2. **Per-line `v`: when an old client would misinterpret a line.** For
   example, a new dependency kind that a resolver must not silently drop.
   Such lines get a higher `v`. Old clients skip them, so they don't see that
   version at all, rather than resolving it wrongly. Clients SHOULD tell the
   user when lines were skipped ("3 newer versions require a newer alai").
   This works the same way as Cargo's `v`.
3. **Format number in `config.json`: when the layout changes.** For example,
   new paths, file splitting or encoding. The new layout is written under a
   new path alongside the old one, and both are listed in `formats` during a
   transition. Clients pick the highest format they support.

`collections.json`, and any future file other than `config.json`, carries its
own top-level `v` so each file can evolve independently.

Changes to the data itself (new versions, yanks) aren't versioned. ETags
handle them.

## Client rules

These don't affect the file format, but every conforming client needs them
to resolve consistently.

- **Choosing an index:** each top-level requirement resolves from exactly one
  index. That's either the index the user pinned it to (e.g.
  `<index>:ns.name`), or else the first index, in configured priority order,
  that has the collection. A client MUST NOT pick a collection from one index
  just because it has a higher version than another. That is how dependency
  confusion attacks work.
- **Dependencies** resolve from the same index as the collection that
  requires them. Index completeness guarantees they're there.
- **One source per name:** within one environment, each `ns.name` resolves to
  exactly one index and version, since it installs to a single
  `ansible_collections/<ns>/<name>` directory. If two requirements would pull
  the same name from different indexes, the client MUST fail and ask the user
  to pin one.
- **Lock files** record the index, `name`, `vers` and `cksum` of everything
  installed.
- **Cache:** clients SHOULD store extracted collections keyed by `cksum`, not
  by index or name, so identical artifacts from different indexes are stored
  once and an index being renamed or moved doesn't invalidate anything.
- **Pre-releases** are skipped unless the user asks for them, or a
  requirement explicitly names a pre-release version.
- **Yanked** versions are skipped unless locked.
- **Unreadable requirements:** a version with a `deps[].req` the
  [requirement syntax](#requirement-syntax) can't read (ansible-galaxy fails
  the whole install on one), or a `requires_ansible` that isn't a valid
  PEP 440 specifier, SHOULD be skipped as though it were yanked, with a
  warning naming it. Failing instead would leave anything that resolves to it
  uninstallable.
- **`requires_ansible`** SHOULD be checked against the target ansible-core
  version when it's known.

### Cached index files

`collections.json` is revalidated on every resolve, like any other file.
Each index file the resolve needs is then handled by comparing the listing's
`sha256` with the SHA-256 the client computes over its own cached copy:

1. **They match:** the cached copy is current. The client MAY use it without
   a request.
2. **They differ, there's no cached copy, or the listing has no `sha256`:** the
   client fetches the file as usual, revalidating any cached copy with
   `If-None-Match`.
3. **The fetched file's SHA-256 still differs from the listing's:** for a
   file found by name, a cache between the client and the index served an
   old copy (for up to the file's `max-age` after a change), or the listing
   is the older of the two. The client SHOULD fetch once more with a query
   string nobody could predict, such as `?r=<16 random hex characters>`,
   which no cache holds, and then use whatever it gets. For a file found by
   SHA-256, the file isn't the one its URL names, and the client MUST treat
   it as an error.

For an index whose template finds files by SHA-256, a collection
`collections.json` doesn't list isn't in the index, without a request, as
nothing else says where its file would be. A `404` for a listed file means
the client's `collections.json` is older than the index keeps replaced files
for: the client SHOULD fetch `collections.json` once more past any cache, as
above, and try the file it names now, and fail if that's also missing.

The client caches each file with the hash of its own bytes, never the
listing's value, and MUST NOT treat a cached file as current on the strength
of `updated` alone. Both rules matter for the same reason: anyone can make a
CDN cache old content under a URL they choose, including one with a future
`updated` in its query, but nobody can make old content hash to the new
value. A client that follows them can be served stale data for one resolve,
inside the `max-age` window, but never gets stuck on it. Files found by
SHA-256 can't be stale at all: only `collections.json` can be.

### Publishing order

So that a client which sees a new `sha256` or `updated` can fetch the content
it describes, whatever writes an index MUST write a changed index file
before the `collections.json` that describes it. An index served through a
CDN it purges SHOULD purge the changed index files found by name between
the two writes, and purge `collections.json` after it. See
[HTTP behaviour](#http-behaviour). With files found by SHA-256, a stale
`collections.json` only names older files, which the index still keeps, so
a client sees an older index but never a mix of old and new.

## Planned additions

These will be specified separately, and will be discoverable through new
fields in `config.json`:

- **Catalog:** every collection with its description, links and tags, for
  search.

## Differences from Cargo

| Aspect | Cargo | alai |
|---|---|---|
| File path | Sharded by name prefix (`se/rd/serde`) | `index/<namespace>/<name>.jsonl`. Ansible names already have two levels, and object storage doesn't need sharding |
| File extension | None | `.jsonl` |
| `config.json` | `dl`, `api`, `auth-required` | `dl`, plus `formats` (multiple layouts at once) and `moved-to`. `api` and `auth-required` are reserved; authentication is left to the host |
| `dl` template | Placeholders `{crate}`, `{version}`, `{prefix}`, `{lowerprefix}`, `{sha256-checksum}`. With none, cargo appends `/{crate}/{version}/download` | Placeholders `{namespace}`, `{name}`, `{version}`, `{sha256-prefix}`, `{sha256-checksum}`. Placeholders are required; there's no default suffix |
| `deps` entries | `name`, `req`, `features`, `optional`, `default_features`, `target`, `kind`, `registry`, `package` | `name`, `req`. `registry` is reserved; the others have no Ansible equivalent |
| Requirement syntax | Cargo semver (`1.2` means `^1.2`) | Ansible's (`1.2` means `==1.2.0`) |
| Cross-index dependencies | Allowed via `deps[].registry`. crates.io forbids them | Not allowed; every index is self-contained |
| Other line fields | `features`, `features2`, `links`, `rust_version` | `size`, `requires_ansible`, `deprecated`, `published` |
| Collection listing | None (crates.io lists crates through its web API) | `collections.json`, with per-collection `updated` times |
