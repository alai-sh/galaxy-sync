"""The failure report: Markdown for the tracking issue.

The full detail stays in state.json, in the private state bucket, which
keeps every conflict, resolved ones included. What's public is the Markdown:
a report block of the outstanding failures and every open conflict, which
`sync --report` writes for the issue's description, and the alert, which
`sync --alerts` writes as one comment for the failures that became
persistent. Neither includes what shouldn't be published: the state bucket
key of a conflict's artifact, and URLs in `unexpected` messages, which can
name the storage endpoint.
"""

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


# Every problem the report counts, in order, as its heading, the failure kinds it holds and what it means. A kind
# ending in "_" is a prefix. Conflicts aren't failures, so they hold no kinds.
SECTIONS: list[tuple[str, tuple[str, ...], str]] = [
    (
        "Extraction failures",
        ("extract_error",),
        "alai-index couldn't read the plugins of these versions. Each version is still mirrored, but plugins.jsonl "
        + "has none of its plugins until a generate-plugins request extracts it again.",
    ),
    (
        "Artifact conflicts",
        (),
        "Galaxy serves another artifact for these versions than the one the mirror has, and the mirror keeps its "
        + "own. Galaxy's artifacts are saved in the state bucket, and the changes are from the latest one it served.",
    ),
    (
        "Galaxy unavailable",
        ("galaxy_unavailable",),
        "Galaxy timed out or answered with a 429 or 5xx, so these are tried again on the next run.",
    ),
    ("Galaxy HTTP errors", ("galaxy_http_",), "Galaxy answered with another HTTP error, such as a 403."),
    (
        "Checksum mismatches",
        ("sha256_mismatch",),
        "The artifact downloaded from Galaxy doesn't match the sha256 Galaxy gives for it.",
    ),
    (
        "Invalid artifacts",
        ("bad_artifact", "manifest_mismatch"),
        "The artifact Galaxy serves can't be read, or its MANIFEST.json is for another collection or version.",
    ),
    ("Invalid version details", ("bad_detail",), "Galaxy's page for the version has no download URL or sha256."),
    (
        "Missing artifacts",
        ("artifact_missing", "artifact_lost"),
        "The version's artifact isn't in the artifact bucket, so it can't be installed. An artifact_missing is "
        + "downloaded from Galaxy again on the next run. An artifact_lost is one Galaxy deleted or serves other bytes "
        + "for, so a person has to put it back.",
    ),
    (
        "alai-index refusals",
        ("alai_index",),
        "alai-index refused a command for the collection, which leaves the index as it was.",
    ),
    (
        "Suspected deletions",
        ("deletions_suspected",),
        "Galaxy publishes fewer versions than the mirror last saw, so the next sweep looks for them to yank.",
    ),
    (
        "Unexpected errors",
        ("unexpected",),
        "Something failed in galaxy-sync itself while reconciling the collection.",
    ),
    ("Other failures", (), "A failure of a kind this report doesn't know yet."),
]


def _section(kind: str) -> str:
    """The heading of the section a failure kind is listed in."""
    for heading, kinds, _ in SECTIONS:
        if any(kind == k or (k.endswith("_") and kind.startswith(k)) for k in kinds):
            return heading
    return "Other failures"


def _anchor(heading: str) -> str:
    """The id GitHub gives a heading, so the count table can link to it."""
    return re.sub(r"[^a-z0-9 _-]", "", heading.lower()).replace(" ", "-")


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


def sorted_failures(failures: list[Failure]) -> list[Failure]:
    return sorted(failures, key=lambda e: (e["collection"], e["version"] or "", e["kind"]))


def _sorted_conflicts(conflicts: list[Conflict]) -> list[Conflict]:
    return sorted(conflicts, key=lambda c: (c["collection"], c["version"], c["opened"]))


@dataclass(frozen=True)
class Alerts:
    """What this run should tell a person about.

    Conflicts opened, which are only in the report block and the run's
    output, and failures that just passed store.PERSISTENT.
    """

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

    def comments(self) -> list[str]:
        """The notification comments: one for the persistent failures."""
        comments: list[str] = []
        if self.persistent:
            headline = _n(len(self.persistent), "failure") + " outstanding for over a day"
            lines = [
                f"**galaxy-sync: {headline}**",
                "",
                "Each failure is reported once:",
                "",
                *_failure_rows(sorted_failures(self.persistent), attempts=True),
                "",
                "The full current report is in this issue's description.",
            ]
            comments.append("\n".join(lines) + "\n")
        return comments


@dataclass(frozen=True)
class Summary:
    outstanding: int
    new: int
    resolved: int
    conflicts: int
    alerts: Alerts = field(default_factory=Alerts)


@dataclass(frozen=True)
class Report:
    markdown: str  # the report block for the tracking issue, between REPORT_START and REPORT_END
    summary: Summary


def render(
    failures: dict[str, Failure],
    conflicts: list[Conflict],
    new: set[str],
    resolved: set[str],
    alerts: Alerts,
) -> Report:
    """Render the report from the ledger, the open conflicts, and the keys that appeared or cleared this run."""
    outstanding = sorted_failures(list(failures.values()))
    conflicts = _sorted_conflicts(conflicts)
    summary = Summary(len(failures), len(new), len(resolved), len(conflicts), alerts)

    # No timestamp or attempt counts, so the block only changes when what's
    # outstanding does, and the issue isn't edited every run.
    listed: dict[str, list[str]] = {heading: [] for heading, _, _ in SECTIONS}
    by_section: dict[str, list[Failure]] = {}
    for e in outstanding:
        by_section.setdefault(_section(e["kind"]), []).append(e)
    for heading, rows in by_section.items():
        listed[heading] = _failure_rows(rows, attempts=False)
    if conflicts:
        listed["Artifact conflicts"] = _conflict_rows(conflicts)
    counts = {heading: len(by_section.get(heading, [])) for heading, _, _ in SECTIONS}
    counts["Artifact conflicts"] = len(conflicts)

    lines = [REPORT_START, "## galaxy-sync report", ""]
    if any(counts.values()):
        lines += ["| Error | Count |", "|---|---|"]
        lines += [f"| [{h}](#{_anchor(h)}) | {n} |" for h, n in counts.items() if n]
    else:
        lines.append("No outstanding failures or open conflicts.")
    for heading, _, meaning in SECTIONS:
        if rows := listed[heading]:
            noun = "conflict" if heading == "Artifact conflicts" else "failure"
            lines += [
                "",
                f"### {heading}",
                "",
                meaning,
                "",
                "<details>",
                f"<summary>{_n(counts[heading], noun)}, click to expand</summary>",
            ]
            lines += ["", *rows, "", "</details>"]
    lines.append(REPORT_END)
    return Report("\n".join(lines) + "\n", summary)
