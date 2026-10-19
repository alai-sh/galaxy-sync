"""Find the alert comments of the conflicts a sync acknowledged, and add the note to each.

Usage: resolve_comments.py COMMENTS ACKNOWLEDGED OUTDIR

COMMENTS holds the tracking issue's comments as JSON lines of `id`,
`node_id` and `body` (`gh api --paginate .../comments --jq '.[] | {id,
node_id, body}'`), ACKNOWLEDGED what `sync --acknowledged` wrote: JSON lines
of a conflict's comment `marker` and the `note`. For each comment carrying
an acknowledged marker, the new body, with the note below the alert, is
written to OUTDIR/<id>.md and "<id> <node_id>" is printed, for the workflow
to edit the comment and mark it resolved. Acknowledging again replaces the
note.

Runs under the system python3 on the runner, so it depends on nothing.
"""

import json
import sys
from pathlib import Path

ACKNOWLEDGED = "<!-- galaxy-sync:acknowledged -->"


def with_note(body: str, note: str) -> str:
    """The comment's body with `note` below the alert, replacing an earlier one."""
    alert = body.replace("\r\n", "\n").partition(ACKNOWLEDGED)[0].rstrip()
    return f"{alert}\n\n{ACKNOWLEDGED}\n**Acknowledged:** {note}\n"


def resolve(comments: list[dict[str, object]], acknowledged: list[dict[str, str]]) -> list[tuple[str, str, str]]:
    """(id, node_id, new body) for each comment of an acknowledged conflict."""
    notes = {a["marker"]: a["note"] for a in acknowledged}
    result: list[tuple[str, str, str]] = []
    for comment in comments:
        body = str(comment["body"] or "")
        first = body.partition("\n")[0].strip()
        if first in notes:
            result.append((str(comment["id"]), str(comment["node_id"]), with_note(body, notes[first])))
    return result


def _jsonl(path: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        sys.stderr.write(f"usage: {argv[0]} COMMENTS ACKNOWLEDGED OUTDIR\n")
        return 2
    acknowledged = [{"marker": str(a["marker"]), "note": str(a["note"])} for a in _jsonl(argv[2])]
    out = Path(argv[3])
    out.mkdir(parents=True, exist_ok=True)
    for comment_id, node_id, body in resolve(_jsonl(argv[1]), acknowledged):
        _ = (out / f"{comment_id}.md").write_text(body)
        sys.stdout.write(f"{comment_id} {node_id}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
