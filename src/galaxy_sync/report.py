"""The failure report: failures.json for tooling, and Markdown for the tracking issue.

failures.json stays in the private state bucket with every detail. What's
public is the Markdown: a report block of everything outstanding, which
`sync --report` writes for the issue's description, and an alert of the
conflicts opened and failures that became persistent this run, which
`sync --alerts` writes for a comment. Both leave
out what shouldn't be published: the state bucket key of a conflict's
artifact, and URLs in `unexpected` messages, which can name the storage
endpoint.
"""

import json
import re
from dataclasses import dataclass, field

from .models import Conflict, Failure

REPORT_START = "<!-- galaxy-sync:report -->"
REPORT_END = "<!-- /galaxy-sync:report -->"

_URL = re.compile(r"https?://\S+")


def _n(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"


def _cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")[:300]


def _message(e: Failure) -> str:
    """A failure's message as it may be published."""
    message = e["message"]
    if e["kind"] == "unexpected":
        message = _URL.sub("<url>", message)
    return _cell(message)


def _failure_rows(failures: list[Failure], *, attempts: bool) -> list[str]:
    columns = ["Collection", "Version", "Kind"] + (["Attempts"] if attempts else []) + ["First seen", "Message"]
    rows = [f"| {' | '.join(columns)} |", "|" + "---|" * len(columns)]
    for e in failures:
        cells = [e["collection"], e["version"] or "*", e["kind"]]
        if attempts:
            cells.append(str(e["attempts"]))
        cells += [e["first_seen"], _message(e)]
        rows.append(f"| {' | '.join(cells)} |")
    return rows


def _conflict_rows(conflicts: list[Conflict]) -> list[str]:
    return [
        "| Collection | Version | Ours | Galaxy | Seen | Changes |",
        "|---|---|---|---|---|---|",
        *(
            f"| {c['collection']} | {c['version']} | {c['ours'][:12]} | {c['upstream'][:12]} "
            + f"| {c['seen']} | {_cell(c['changes'])} |"
            for c in conflicts
        ),
    ]


def _sorted_failures(failures: list[Failure]) -> list[Failure]:
    return sorted(failures, key=lambda e: (e["collection"], e["version"] or "", e["kind"]))


def _sorted_conflicts(conflicts: list[Conflict]) -> list[Conflict]:
    return sorted(conflicts, key=lambda c: (c["collection"], c["version"], c["seen"]))


@dataclass(frozen=True)
class Alerts:
    """What this run should notify a person about: conflicts opened, and failures that just passed store.PERSISTENT."""

    conflicts: list[Conflict] = field(default_factory=list)
    persistent: list[Failure] = field(default_factory=list)

    def headline(self) -> str:
        """One line for the run's output, or "" when there's nothing new."""
        parts: list[str] = []
        if self.conflicts:
            parts.append(_n(len(self.conflicts), "new conflict"))
        if self.persistent:
            parts.append(_n(len(self.persistent), "failure") + " outstanding for over a day")
        return f"galaxy-sync: {' and '.join(parts)}" if parts else ""

    def markdown(self) -> str:
        """The notification comment, or "" when there's nothing new."""
        headline = self.headline()
        if not headline:
            return ""
        lines = [f"**{headline}**"]
        if self.conflicts:
            lines += [
                "",
                "New conflicts. The mirror keeps its original; Galaxy's artifact is saved in the state bucket.",
                "",
                *_conflict_rows(_sorted_conflicts(self.conflicts)),
            ]
        if self.persistent:
            lines += [
                "",
                "Failures outstanding for over a day, each reported once:",
                "",
                *_failure_rows(_sorted_failures(self.persistent), attempts=True),
            ]
        lines += ["", "The full current report is in this issue's description."]
        return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class Summary:
    outstanding: int
    new: int
    resolved: int
    conflicts: int
    alerts: Alerts = field(default_factory=Alerts)


@dataclass(frozen=True)
class Report:
    json: str  # failures.json
    markdown: str  # the report block for the tracking issue, between REPORT_START and REPORT_END
    summary: Summary


def render(
    failures: dict[str, Failure],
    conflicts: list[Conflict],
    new: set[str],
    resolved: set[str],
    generated: str,
    alerts: Alerts,
) -> Report:
    """Render the report from the ledger, the open conflicts, and the keys that appeared or cleared this run."""
    outstanding = _sorted_failures(list(failures.values()))
    conflicts = _sorted_conflicts(conflicts)
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

    # No timestamp or attempt counts: the block only changes when what's
    # outstanding does, so the issue isn't edited every run.
    lines = [REPORT_START, "## galaxy-sync report", ""]
    if not outstanding and not conflicts:
        lines.append("No outstanding failures or open conflicts.")
    else:
        lines.append(f"{_n(len(outstanding), 'failure')} outstanding, {_n(len(conflicts), 'conflict')} to inspect.")
    if outstanding:
        lines += ["", *_failure_rows(outstanding, attempts=False)]
    if conflicts:
        lines += [
            "",
            "### Conflicts to inspect",
            "",
            "Galaxy now serves different contents for these versions. The mirror keeps its",
            "original; Galaxy's artifact is saved in the state bucket.",
            "",
            *_conflict_rows(conflicts),
        ]
    lines.append(REPORT_END)
    return Report(as_json + "\n", "\n".join(lines) + "\n", summary)
