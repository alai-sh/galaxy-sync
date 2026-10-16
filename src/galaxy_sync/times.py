"""Timestamps as the index and state store them: RFC 3339, UTC, second precision."""

import datetime

_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def now() -> str:
    return datetime.datetime.now(datetime.UTC).strftime(_FORMAT)


def rfc3339(value: str | None) -> str | None:
    """Normalise one of Galaxy's timestamps, or None if there isn't one (None or empty)."""
    if not value:
        return None
    return datetime.datetime.fromisoformat(value).astimezone(datetime.UTC).strftime(_FORMAT)
