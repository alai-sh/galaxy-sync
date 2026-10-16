"""SemVer 2.0.0 precedence, as the index spec requires for sorting versions."""

import re

_SEMVER = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    + r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    + r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)

type _Identifier = tuple[int, int | str]
type SortKey = tuple[int, tuple[int, ...], tuple[int, tuple[_Identifier, ...]], str]


def _identifier(value: str) -> _Identifier:
    # Numeric identifiers sort before alphanumeric ones, and numerically.
    return (0, int(value)) if value.isdigit() else (1, value)


def sort_key(version: str) -> SortKey:
    """Key giving SemVer precedence, with the full string as a stable tie-break.

    Versions that aren't valid SemVer sort below every valid one.
    """
    m = _SEMVER.match(version)
    if not m:
        return (0, (), (0, ()), version)

    major, minor, patch, pre, _build = m.groups()
    # A release sorts after any of its pre-releases.
    pre_key = (0, tuple(_identifier(p) for p in pre.split("."))) if pre else (1, ())
    return (1, (int(major), int(minor), int(patch)), pre_key, version)


def is_prerelease(version: str) -> bool:
    m = _SEMVER.match(version)
    return bool(m and m.group(4))
