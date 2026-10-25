# alai plugin index

**Status:** draft, part of index format `1`.

The plugin index maps plugin, role and playbook names to the collections that
provide them. It's what the client's `find` command uses: "which collection
has a `to_seconds` filter?", or "where did `community.general.docker_container`
go?".

It's a single file that clients download, cache with an ETag, and search
locally, much like `apt-file`. Substring and fuzzy search then need no
server.

Conventions (encoding, unknown fields, timestamps, versions) are the same as
in the [index format](README.md#conventions).

## Location

An optional `plugins` path in the format's entry in `config.json`:

```json
"formats": {"1": {"index": "index/{namespace}/{name}.jsonl", "collections": "collections.json", "plugins": "plugins.jsonl"}}
```

If it's absent, the index has no plugin index. Clients that don't support
`find` ignore it, as they would any unknown field.

## Lines

One JSON object per line. Order is unspecified.

```json
{"v":1,"type":"filter","name":"to_seconds","collection":"alai_example.demo","since":"0.1.0","short_description":"Convert a duration string to seconds"}
{"v":1,"type":"module","name":"old_thing","collection":"alai_example.demo","since":"0.1.0","until":"0.2.0","routing":{"redirect":"alai_example.demo.new_thing"}}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `v` | integer | yes | Schema version of this line. Clients MUST skip lines with a higher `v` than they support |
| `type` | string | yes | See [Types](#types) |
| `name` | string | yes | Short name, e.g. `user`. The full name is `<collection>.<name>` |
| `collection` | string | yes | Collection that provides (or provided) it, e.g. `community.general`. `ansible.builtin` for ansible-core's own plugins |
| `since` | string | no | Lowest version of the collection that contains it. See [`since` and `until`](#since-and-until) |
| `until` | string | no | The version in which it was removed. Absent if the latest version still contains it |
| `short_description` | string | no | One line from the plugin's documentation |
| `routing` | object | no | The entry for this plugin from the collection's `meta/runtime.yml` `plugin_routing` (`extensions/eda/eda_runtime.yml` for `event_source` and `event_filter`), copied verbatim (e.g. `redirect`, `deprecation`, `tombstone`) |

`(type, collection, name)` is unique within the file.

### Types

`type` uses the names that `ansible-doc -t` accepts, plus `playbook` and the
Event-Driven Ansible types that ansible-rulebook loads from a collection:

| `type` | Source in a collection |
|---|---|
| `module` | `plugins/modules/<name>.{py,ps1}`, plus `plugins/action/<name>.py` when there is no matching module (an action-only plugin) |
| `become`, `cache`, `callback`, `cliconf`, `connection`, `httpapi`, `inventory`, `lookup`, `netconf`, `shell`, `strategy`, `vars` | `plugins/<type>/<name>.py` |
| `filter`, `test` | Names defined in `plugins/filter/*.py` or `plugins/test/*.py`. One file can define several, so file names alone aren't enough. See [Extraction](#extraction) |
| `role` | `roles/<name>/`. `short_description` comes from `meta/argument_specs.yml` (`argument_specs.main.short_description`) if present |
| `playbook` | `playbooks/<name>.yml` or `.yaml` |
| `event_source`, `event_filter` | `extensions/eda/plugins/<type>/<name>.py`, `extensions/eda/plugins/<type>s/<name>.py` or the older `plugins/<type>/<name>.py`. When a name is in more than one, the first in that order, as ansible-rulebook searches them |
| `rulebook` | `extensions/eda/rulebooks/<name>.yml`, or `rulebooks/<name>.yml` (first wins). Only `.yml`: ansible-rulebook doesn't load `.yaml` |

Not indexed, because they aren't referenced by name in playbooks:
`module_utils`, `doc_fragments`, `plugin_utils`, and action plugins that back
a module of the same name.

**New types may be added without a format change.** Clients MUST show lines
with an unknown `type` as plain names, not hide them.

## `since` and `until`

Both are computed by the sync from every mirrored version of the collection.
Pre-releases are ignored unless only pre-releases contain the plugin.

- `since`: the lowest version, by SemVer precedence, that contains it.
- `until`: the lowest version, by precedence, that is higher than every
  version containing it. Absent if the highest version contains it.

Using precedence rather than publication date keeps maintenance branches
from confusing things. A plugin removed in `13.0.0` but still present in
`12.6.6` (published later) gets `until: "13.0.0"`.

A plugin that was removed and later added back is described by a single
`since`/`until` range, which over-simplifies that rare case.

## Routing and history

One file covers both what exists now and what used to exist:

- A plugin in the latest version: `since`, no `until`.
- A plugin that was removed: it keeps its line, with `until`, and with
  `routing` if the collection's latest `meta/runtime.yml` mentions it.
- A plugin known only from routing (the collection never had it as a file in
  a mirrored version, but redirects it): a line with `routing` and no
  `since`.

So `find docker_container` can show both "community.general (removed in
5.0.0, see routing)" and "community.docker (since 1.0.0)".

`routing` is copied verbatim, so it uses ansible-core's
`meta/runtime.yml` structure, for example:

```json
"routing": {"redirect": "community.general.say"}
"routing": {"tombstone": {"removal_version": "2.0.0", "warning_text": "Use the 'default' callback plugin with 'display_skipped_hosts = no' and 'display_ok_hosts = no' options."}}
"routing": {"deprecation": {"removal_version": "14.0.0", "warning_text": "Please migrate to the 'community.general.loganalytics_ingestion' plugin."}}
```

## `ansible.builtin`

ansible-core's own plugins aren't a Galaxy collection, but `find` needs
them. Their lines use `collection: "ansible.builtin"`. They're generated from
one ansible-core release rather than by comparing every release: `since` is
the plugin's documented `version_added` (an ansible-core, or older Ansible,
version), omitted when that's `historical` or missing, and there's no
`until`, since removed plugins keep a tombstone in their `routing`.

The extraction is the same as for collections, but run over an ansible-core
release's `lib/ansible/plugins/<type>/` and `lib/ansible/modules/`. Routing
comes from `lib/ansible/config/ansible_builtin_runtime.yml`. That file holds
the redirects for every module that moved out of ansible-core into a
collection in 2.10, so `find docker_container` points to `community.docker`
even for users who remember it as a core module.

## Extraction

The sync handles arbitrary, untrusted collections and has write access to the
index, so **it MUST NOT execute collection code**. That rules out
`ansible-doc --metadata-dump` and importing plugins.

- **Names** come from file and directory names, except for filters and tests.
- **Filters and tests:** parse the file's Python AST and read the dict
  literal returned by `FilterModule.filters()` / `TestModule.tests()`. Use
  documentation sidecars (`plugins/filter/<name>.yml`) where present. If the
  AST can't be resolved statically (e.g. the dict is built dynamically), fall
  back to Galaxy's `contents` for that version (best effort; Galaxy's
  importer computed it in a sandbox). Measured across the latest version of
  every Galaxy collection (2026-10-10), static resolution handled 909 of 915
  filter/test files (99.3%), so the fallback is rarely needed.
- **YAML** is parsed with a safe loader that keeps timestamps as strings, so
  values such as `removal_date: 2024-01-01` in `routing` stay JSON strings.
- **`short_description`:** read the `DOCUMENTATION` string constant from the
  Python AST (or the `.yml` sidecar, or the `.ps1` module's adjacent `.py` /
  `.yml` doc file), then `yaml.safe_load` it.

## Size

From the latest version of every Galaxy collection (2026-10-09):

| | Count |
|---|---|
| Module entries | ~57k (38k unique short names, 10.9k in more than one collection) |
| Other plugin files | ~4k |
| Roles | ~17.5k |
| Playbook files | ~5.4k |

That's roughly 85k lines including history, around 10 MB raw and 1-2 MB
compressed. If it outgrows a single file, a future format can shard it by
`type`.

## Full documentation (planned)

The plugin index deliberately holds only `short_description`. Full
documentation is far too large to download up front. Module docs alone
(`DOCUMENTATION`, `EXAMPLES`, `RETURN`) total ~293 MB across the latest
versions (55.7k modules, ~5 KB each).

Full docs will be a separate, per-artifact file, fetched only when a user
inspects a plugin:

```
<artifact root>/docs/sha256/<aa>/<sha256>.json
```

- **Keyed by the artifact's sha256:** the docs describe exactly those bytes,
  so the file is immutable and cacheable forever, just like the artifact.
- **Size:** community.general's 703 documented plugins come to 2.9 MB raw,
  559 KB gzipped, so fetching on demand is fine.
- **Shape:** aligned with `ansible-doc --json` output, so existing tooling
  (and antsibull-docs' expectations) carry over.
- **Extracted statically**, as above. `extends_documentation_fragment` must
  be merged by parsing the fragment classes' string attributes, including
  fragments from other collections (e.g. `ansible.netcommon`), which the
  mirror has.

The reserved name for its location is `formats.<n>.docs` in `config.json`.

## Not covered yet

- **Standalone roles** (Galaxy's v1 API, e.g. `geerlingguy.mysql`) aren't
  collections and aren't mirrored. If they ever are, they'd fit as
  `type: "role"` with a different `collection` convention.
- **Ranking:** a bare name like `user` matches 75 collections. Ranking needs a
  popularity signal, such as Galaxy's per-collection `download_count`. That
  belongs in the catalog, which isn't specified yet.
