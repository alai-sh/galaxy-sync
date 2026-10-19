"""The workflow script that adds the note to an acknowledged conflict's alert comment."""

import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).parent.parent / ".github" / "scripts" / "resolve_comments.py"

MARKER = "<!-- galaxy-sync:conflict ns.coll 1.0.0 2026-10-18T23:20:18Z -->"
ALERT = f"{MARKER}\n**galaxy-sync: new conflict in ns.coll 1.0.0**\n\nChanges: added a.yml\n"


def run(tmp_path, comments, acknowledged):
    (tmp_path / "comments.jsonl").write_text("".join(json.dumps(c) + "\n" for c in comments))
    (tmp_path / "acknowledged.jsonl").write_text("".join(json.dumps(a) + "\n" for a in acknowledged))
    proc = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-I",
            str(SCRIPT),
            str(tmp_path / "comments.jsonl"),
            str(tmp_path / "acknowledged.jsonl"),
            str(tmp_path / "out"),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout


def test_adds_the_note_to_the_matching_comment(tmp_path):
    comments = [
        {"id": 1, "node_id": "IC_1", "body": "Someone's comment mentioning " + MARKER},
        {"id": 2, "node_id": "IC_2", "body": ALERT},
        {"id": 3, "node_id": "IC_3", "body": ALERT.replace("1.0.0 2026-10-18T23:20:18Z", "1.0.0 2026-01-01T00:00:00Z")},
        {"id": 4, "node_id": "IC_4", "body": None},
    ]
    out = run(tmp_path, comments, [{"marker": MARKER, "note": "the author republished it"}])
    assert out == "2 IC_2\n"
    body = (tmp_path / "out" / "2.md").read_text()
    assert body == ALERT + "\n<!-- galaxy-sync:acknowledged -->\n**Acknowledged:** the author republished it\n"


def test_acknowledging_again_replaces_the_note(tmp_path):
    first = ALERT.replace("\n", "\r\n") + "\r\n<!-- galaxy-sync:acknowledged -->\r\n**Acknowledged:** first\r\n"
    assert run(tmp_path, [{"id": 2, "node_id": "IC_2", "body": first}], [{"marker": MARKER, "note": "second"}])
    body = (tmp_path / "out" / "2.md").read_text()
    assert body == ALERT + "\n<!-- galaxy-sync:acknowledged -->\n**Acknowledged:** second\n"


def test_nothing_acknowledged(tmp_path):
    assert run(tmp_path, [{"id": 2, "node_id": "IC_2", "body": ALERT}], []) == ""


def test_usage():
    proc = subprocess.run(  # noqa: S603
        [sys.executable, "-I", str(SCRIPT)], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 2
    assert "usage:" in proc.stderr
