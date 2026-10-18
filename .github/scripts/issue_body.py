"""Put the report block `sync --report` wrote at the top of the tracking issue's body.

Usage: issue_body.py CURRENT REPORT

CURRENT holds the issue's body as `gh issue view --json body -q .body` prints
it, REPORT the block, between the `galaxy-sync:report` markers. The block
replaces the one already in the body, or goes above a body without one, so
notes kept below it survive. The new body is printed when it differs from the
current one, and nothing is printed when it doesn't, so the workflow can skip
the edit: GitHub keeps a history of every edit, and runs are every 10 minutes.

Runs under the system python3 on the runner, so it depends on nothing.
"""

import sys
from pathlib import Path

START = "<!-- galaxy-sync:report -->"
END = "<!-- /galaxy-sync:report -->"


def merge(current: str, block: str) -> str:
    """The body with `block` in place of its report block, or on top if it has none."""
    block = block.strip() + "\n"
    if START in current and END in current.partition(START)[2]:
        head, _, rest = current.partition(START)
        _, _, tail = rest.partition(END)
    else:
        head, tail = "", current
    head = head.rstrip() + "\n\n" if head.strip() else ""
    tail = "\n" + tail.strip() + "\n" if tail.strip() else ""
    return head + block + tail


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        sys.stderr.write(f"usage: {argv[0]} CURRENT REPORT\n")
        return 2
    current = Path(argv[1]).read_text().replace("\r\n", "\n")
    merged = merge(current, Path(argv[2]).read_text())
    if merged != current:
        sys.stdout.write(merged)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
