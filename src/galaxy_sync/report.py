"""The failure report: failures.json for tooling and failures.md for people.

Both stay in the private state bucket. What's public is only `Alerts`: how
many things newly need a person, without naming them.
"""

import json
from dataclasses import dataclass

from .models import Conflict, Failure


@dataclass(frozen=True)
class Alerts:
    """What newly needs a person this run, as counts only."""

    conflicts: int = 0  # conflicts opened
    persistent: int = 0  # failures that just passed store.PERSISTENT

    def text(self) -> str:
        """A short message for a notification, or "" when there's nothing new."""
        parts: list[str] = []
        if self.conflicts:
            parts.append(f"{self.conflicts} new conflicts")
        if self.persistent:
            parts.append(f"{self.persistent} failures outstanding for over a day")
        return f"galaxy-sync: {' and '.join(parts)}; see failures.md in the state bucket." if parts else ""


@dataclass(frozen=True)
class Summary:
    outstanding: int
    new: int
    resolved: int
    conflicts: int
    alerts: Alerts = Alerts()


def _cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")[:300]


def render(
    failures: dict[str, Failure],
    conflicts: list[Conflict],
    new: set[str],
    resolved: set[str],
    generated: str,
    alerts: Alerts,
) -> tuple[str, str, Summary]:
    """Return failures.json, failures.md and the counts.

    `failures` is the ledger, `conflicts` the open conflicts, and `new` /
    `resolved` the ledger keys that appeared or cleared during this run.
    """
    outstanding = sorted(failures.values(), key=lambda e: (e["collection"], e["version"] or "", e["kind"]))
    conflicts = sorted(conflicts, key=lambda c: (c["collection"], c["version"], c["seen"]))
    summary = Summary(len(failures), len(new), len(resolved), len(conflicts), alerts)

    as_json = json.dumps(
        {
            "generated": generated,
            "outstanding": outstanding,
            "resolved_this_run": sorted(resolved),
            "conflicts": conflicts,
        },
        indent=1,
    )

    lines = [
        "# galaxy-sync failures",
        "",
        f"Generated {generated}. {summary.outstanding} outstanding, "
        + f"{summary.new} new this run, {summary.resolved} resolved this run.",
        "",
    ]
    if outstanding:
        lines += ["| Collection | Version | Kind | Attempts | First seen | Message |", "|---|---|---|---|---|---|"]
        lines += [
            f"| {e['collection']} | {e['version'] or '*'} | {e['kind']} | {e['attempts']} "
            + f"| {e['first_seen']} | {_cell(e['message'])} |"
            for e in outstanding
        ]
    if conflicts:
        lines += [
            "",
            "## Conflicts to inspect",
            "",
            "Galaxy now serves different contents for these versions. The mirror keeps its",
            "original; Galaxy's artifact is saved in the state bucket.",
            "",
            "| Collection | Version | Ours | Galaxy | Seen | Changes | Saved as |",
            "|---|---|---|---|---|---|---|",
        ]
        lines += [
            f"| {c['collection']} | {c['version']} | {c['ours'][:12]} | {c['upstream'][:12]} "
            + f"| {c['seen']} | {_cell(c['changes'])} | {c['saved']} |"
            for c in conflicts
        ]
    return as_json + "\n", "\n".join(lines) + "\n", summary
