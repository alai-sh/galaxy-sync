"""The workflow script that keeps the report block at the top of the tracking issue."""

import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent.parent / ".github" / "scripts" / "issue_body.py"

BLOCK = "<!-- galaxy-sync:report -->\n## galaxy-sync report\n\n1 failure outstanding, 0 conflicts to inspect.\n<!-- /galaxy-sync:report -->\n"
EMPTY = "<!-- galaxy-sync:report -->\n## galaxy-sync report\n\nNo outstanding failures or open conflicts.\n<!-- /galaxy-sync:report -->\n"


def run(tmp_path, current, block):
    (tmp_path / "body.md").write_text(current)
    (tmp_path / "report.md").write_text(block)
    proc = subprocess.run(  # noqa: S603
        [sys.executable, "-I", str(SCRIPT), str(tmp_path / "body.md"), str(tmp_path / "report.md")],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout


def test_goes_above_a_body_without_a_block(tmp_path):
    assert run(tmp_path, "My notes.\n\n- one\n", BLOCK) == BLOCK + "\nMy notes.\n\n- one\n"


def test_empty_body(tmp_path):
    assert run(tmp_path, "", BLOCK) == BLOCK


def test_replaces_the_block_and_keeps_the_notes(tmp_path):
    current = BLOCK + "\nMy notes.\n"
    assert run(tmp_path, current, EMPTY) == EMPTY + "\nMy notes.\n"


def test_keeps_text_above_the_block(tmp_path):
    current = "Intro.\n\n" + BLOCK + "\nMy notes.\n"
    assert run(tmp_path, current, EMPTY) == "Intro.\n\n" + EMPTY + "\nMy notes.\n"


@pytest.mark.parametrize("current", [BLOCK + "\nMy notes.\n", BLOCK.replace("\n", "\r\n") + "\r\nMy notes.\r\n"])
def test_prints_nothing_when_unchanged(tmp_path, current):
    assert run(tmp_path, current, BLOCK) == ""


def test_unterminated_block_is_treated_as_notes(tmp_path):
    current = "<!-- galaxy-sync:report -->\nSomeone deleted the end marker.\n"
    assert run(tmp_path, current, BLOCK) == BLOCK + "\n" + current


def test_usage(tmp_path):
    proc = subprocess.run(  # noqa: S603
        [sys.executable, "-I", str(SCRIPT)], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 2
    assert "usage:" in proc.stderr
