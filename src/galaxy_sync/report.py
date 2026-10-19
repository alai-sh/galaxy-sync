"""The failure report: failures.json for tooling, Markdown for the tracking issue, and the public conflict records.

failures.json stays in the private state bucket with every detail. What's
public is the Markdown: a report block of everything outstanding, which
`sync --report` writes for the issue's description, and the alerts, which
`sync --alerts` writes as one comment per conflict opened this run plus one
for the failures that became persistent. `sync --acknowledged` lists the
conflicts acknowledged this run, so the workflow can mark their comments
resolved. Each collection's conflict record, published in the index bucket,
is the permanent history. None of these include what shouldn't be
published: the state bucket key of a conflict's artifact, and URLs in
`unexpected` messages, which can name the storage endpoint.
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
    rows = [
        "| Collection | Version | Ours | Galaxy | Builds seen | Opened | Changes |",
        "|---|---|---|---|---|---|---|",
    ]
    for c in conflicts:
        latest = c["observed"][-1]
        rows.append(
            f"| {c['collection']} | {c['version']} | {c['ours'][:12]} | {latest['sha256'][:12]} "
            + f"| {len(c['observed'])} | {c['opened']} | {_cell(latest['changes'])} |"
        )
    return rows


def conflict_marker(conflict: Conflict) -> str:
    """The hidden line that finds a conflict's alert comment again once it's acknowledged."""
    return f"<!-- galaxy-sync:conflict {conflict['collection']} {conflict['version']} {conflict['opened']} -->"


def _conflict_comment(conflict: Conflict) -> str:
    fqcn, vers = conflict["collection"], conflict["version"]
    latest = conflict["observed"][-1]
    changes = latest["changes"].replace("\n", " ")
    if len(changes) > 2000:
        changes = changes[:2000] + " ..."
    return "\n".join(
        [
            conflict_marker(conflict),
            f"**galaxy-sync: new conflict in {fqcn} {vers}**",
            "",
            "Galaxy now serves different contents for this version. The mirror keeps its original;",
            "Galaxy's artifact is saved in the state bucket. Once it's inspected, acknowledge it with",
            f"the Refresh workflow (`acknowledge: {fqcn}:{vers}` and a note) to stop it being reported.",
            "",
            "| Ours | Galaxy | Uploaded to Galaxy | Seen |",
            "|---|---|---|---|",
            f"| {conflict['ours']} | {latest['sha256']} | {latest['uploaded'] or 'unknown'} | {latest['seen']} |",
            "",
            f"Changes: {changes}",
            "",
        ]
    )


def _sorted_failures(failures: list[Failure]) -> list[Failure]:
    return sorted(failures, key=lambda e: (e["collection"], e["version"] or "", e["kind"]))


def _sorted_conflicts(conflicts: list[Conflict]) -> list[Conflict]:
    return sorted(conflicts, key=lambda c: (c["collection"], c["version"], c["opened"]))


def conflict_record(fqcn: str, conflicts: list[Conflict]) -> str:
    """A collection's public conflict record: every conflict it has had, without the private keys."""
    entries: list[dict[str, object]] = []
    for c in sorted(conflicts, key=lambda c: c["opened"]):
        entry: dict[str, object] = {
            "version": c["version"],
            "ours": c["ours"],
            "opened": c["opened"],
            "observed": [
                {"sha256": o["sha256"], "uploaded": o["uploaded"], "seen": o["seen"], "changes": o["changes"]}
                for o in c["observed"]
            ],
        }
        if "acknowledged" in c:
            entry["acknowledged"] = c["acknowledged"]
        if "resolved" in c:
            entry["resolved"] = c["resolved"]
        entries.append(entry)
    return json.dumps({"collection": fqcn, "conflicts": entries}, indent=1, ensure_ascii=False) + "\n"


@dataclass(frozen=True)
class Alerts:
    """What this run should tell a person about.

    Conflicts opened, failures that just passed store.PERSISTENT, and the
    conflicts acknowledged, whose comments can be marked resolved.
    """

    conflicts: list[Conflict] = field(default_factory=list)
    persistent: list[Failure] = field(default_factory=list)
    acknowledged: list[Conflict] = field(default_factory=list)

    def headline(self) -> str:
        """One line for the run's output, or "" when there's nothing new."""
        parts: list[str] = []
        if self.conflicts:
            parts.append(_n(len(self.conflicts), "new conflict"))
        if self.persistent:
            parts.append(_n(len(self.persistent), "failure") + " outstanding for over a day")
        return f"galaxy-sync: {' and '.join(parts)}" if parts else ""

    def comments(self) -> list[str]:
        """The notification comments: one per new conflict, then one for the persistent failures."""
        comments = [_conflict_comment(c) for c in _sorted_conflicts(self.conflicts)]
        if self.persistent:
            headline = _n(len(self.persistent), "failure") + " outstanding for over a day"
            lines = [
                f"**galaxy-sync: {headline}**",
                "",
                "Each failure is reported once:",
                "",
                *_failure_rows(_sorted_failures(self.persistent), attempts=True),
                "",
                "The full current report is in this issue's description.",
            ]
            comments.append("\n".join(lines) + "\n")
        return comments

    def acknowledged_json(self) -> str:
        """The conflicts acknowledged this run, as JSON lines of `marker` and `note`."""
        return "".join(
            json.dumps({"marker": conflict_marker(c), "note": c["acknowledged"]["note"]}) + "\n"
            for c in _sorted_conflicts(self.acknowledged)
            if "acknowledged" in c
        )


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
    to_inspect = [c for c in conflicts if "acknowledged" not in c]
    summary = Summary(len(failures), len(new), len(resolved), len(to_inspect), alerts)

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
    if not outstanding and not to_inspect:
        lines.append("No outstanding failures or conflicts to inspect.")
    else:
        lines.append(f"{_n(len(outstanding), 'failure')} outstanding, {_n(len(to_inspect), 'conflict')} to inspect.")
    if outstanding:
        lines += ["", *_failure_rows(outstanding, attempts=False)]
    if to_inspect:
        lines += [
            "",
            "### Conflicts to inspect",
            "",
            "Galaxy now serves different contents for these versions. The mirror keeps its",
            "original; Galaxy's artifacts are saved in the state bucket. Changes are from the",
            "latest build Galaxy served. Acknowledge each with the Refresh workflow once inspected.",
            "",
            *_conflict_rows(to_inspect),
        ]
    lines.append(REPORT_END)
    return Report(as_json + "\n", "\n".join(lines) + "\n", summary)
