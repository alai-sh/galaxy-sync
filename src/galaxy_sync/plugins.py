"""The plugin index: what's stored from each extraction, and how it becomes plugins.jsonl lines.

Extraction itself is alai-plugin-extractor's; see docs/index-spec/plugins.md
for the line format and the `since` / `until` rules.
"""

import alai_plugin_extractor as ext

from . import semver
from .models import ExtractionData, PluginEntry, PluginLine

# Revision of stored extraction results. Bump it when a change here, or an
# alai-plugin-extractor upgrade whose release notes say its output changed,
# would extract the same artifact differently: reconcile re-extracts every
# stored result with an older format.
EXTRACTION_FORMAT = 1


def extraction_data(result: ext.Collection) -> ExtractionData:
    """What gets stored from an extraction: plugin names, descriptions and routing."""
    plugins: list[PluginEntry] = []
    for p in result.plugins:
        entry: PluginEntry = {"type": p.type, "name": p.name}
        if p.short_description:
            entry["short_description"] = p.short_description
        plugins.append(entry)
    return {
        "format": EXTRACTION_FORMAT,
        "plugins": plugins,
        "routing": {f"{r.type}:{r.name}": dict(r.entry) for r in result.routing},
    }


def plugin_lines(collection: str, extractions: dict[str, ExtractionData]) -> list[PluginLine]:
    """Build plugin index lines for one collection from every extracted version.

    `extractions` maps each extracted version to its extraction data.
    Implements the `since` / `until` rules from docs/index-spec/plugins.md.
    """
    if not extractions:
        return []

    versions = sorted(extractions, key=semver.sort_key)
    stable = [v for v in versions if not semver.is_prerelease(v)]
    latest = versions[-1] if not stable else stable[-1]

    present: dict[tuple[str, str], list[str]] = {}
    descs: dict[tuple[str, str], str] = {}
    for v in versions:  # ascending, so later versions' descriptions win
        for p in extractions[v]["plugins"]:
            key = (p["type"], p["name"])
            present.setdefault(key, []).append(v)
            if desc := p.get("short_description"):
                descs[key] = desc

    routing = extractions[latest]["routing"]
    result: list[PluginLine] = []
    for key, containing in present.items():
        considered = [v for v in containing if not semver.is_prerelease(v)] or containing
        since = min(considered, key=semver.sort_key)
        highest = max(containing, key=semver.sort_key)
        pool = stable or versions
        later = [v for v in pool if semver.sort_key(v) > semver.sort_key(highest)]
        entry: PluginLine = {"v": 1, "type": key[0], "name": key[1], "collection": collection, "since": since}
        if later:
            entry["until"] = min(later, key=semver.sort_key)
        if key in descs:
            entry["short_description"] = descs[key]
        if (r := routing.get(f"{key[0]}:{key[1]}")) is not None:
            entry["routing"] = r
        result.append(entry)

    for rkey, r in routing.items():
        ptype, _, name = rkey.partition(":")
        if (ptype, name) not in present:
            result.append({"v": 1, "type": ptype, "name": name, "collection": collection, "routing": r})
    return result


def _version_added(plugin: ext.Plugin) -> str | None:
    """A plugin's documented `version_added`, or None if it's missing or `historical`.

    Unquoted values reach us as numbers, so `version_added: 2.20` would read
    as `2.2`; ansible-core quotes most of them.
    """
    doc = plugin.docs.documentation if plugin.docs else None
    value = doc.get("version_added") if doc else None
    if isinstance(value, int | float) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, str) and (value := value.strip()) and value != "historical":
        return value
    return None


def builtin_lines(result: ext.Collection) -> list[PluginLine]:
    """Plugin index lines for ansible.builtin from one ansible-core extraction.

    ansible.builtin isn't on Galaxy, so there's a single snapshot rather than
    every version to compare. `since` comes from each plugin's documented
    `version_added` instead, which ansible-core keeps accurate; it's omitted
    when that's `historical` or missing. There's no `until`: plugins removed
    from ansible-core are covered by the tombstones in their routing. The
    extraction must be made with `docs=True`.
    """
    data = extraction_data(result)
    added = {(p.type, p.name): _version_added(p) for p in result.plugins}
    lines: list[PluginLine] = []
    for p in data["plugins"]:
        line: PluginLine = {"v": 1, "type": p["type"], "name": p["name"], "collection": "ansible.builtin"}
        if since := added.get((p["type"], p["name"])):
            line["since"] = since
        if desc := p.get("short_description"):
            line["short_description"] = desc
        if (r := data["routing"].get(f"{p['type']}:{p['name']}")) is not None:
            line["routing"] = r
        lines.append(line)
    for key, r in data["routing"].items():
        ptype, _, name = key.partition(":")
        if (ptype, name) not in added:
            lines.append({"v": 1, "type": ptype, "name": name, "collection": "ansible.builtin", "routing": r})
    return lines
