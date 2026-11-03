"""Sorting versions by SemVer 2.0.0 precedence, as alai-core compares them."""

from alai_core.semver import Version

type SortKey = tuple[int, Version | None, str]


def sort_key(version: str) -> SortKey:
    """Key giving SemVer precedence, then the full string as alai-core orders them.

    Versions that aren't valid SemVer, which Galaxy's feed could still give, sort below every valid one.
    """
    try:
        return (1, Version(version), "")
    except ValueError:
        return (0, None, version)
