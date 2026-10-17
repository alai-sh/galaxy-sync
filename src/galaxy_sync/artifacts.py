"""Reading collection tarballs.

The sync never extracts or runs collection code. These functions only read
member bytes from a tarball held in memory: `MANIFEST.json` to check what
was downloaded, and a digest of the contents to tell a repack from a real
change. Plugin extraction is alai-plugin-extractor's job (see plugins.py).
"""

import contextlib
import hashlib
import io
import json
import posixpath
import tarfile
from typing import IO, cast

# Larger MANIFEST.json files are refused.
_MAX_MANIFEST = 20 * 1024 * 1024
# Decompressed bytes hashed per artifact before giving up, against tar bombs.
_MAX_CONTENTS = 2 * 1024**3
# Compared as parsed JSON, so formatting differences between builders don't count.
_JSON_FILES = ("MANIFEST.json", "FILES.json")


def _member_name(member: tarfile.TarInfo) -> str:
    return posixpath.normpath(member.name).lstrip("/")


def manifest_info(data: bytes) -> dict[str, object]:
    """`collection_info` from a tarball's MANIFEST.json, or {} if it has none.

    Raises tarfile.TarError or ValueError if the tarball or the JSON can't be
    read. Extraction never blocks mirroring, so this is what checks that the
    download is the collection version it claims to be.

    A tarball can hold MANIFEST.json more than once (appended with `tar -r`).
    The last one is read, as tar, ansible-galaxy and alai-plugin-extractor do.
    """
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        found = None
        for member in tar:
            if member.isfile() and _member_name(member) == "MANIFEST.json":
                found = member
        if found is None:
            return {}
        if found.size > _MAX_MANIFEST:
            raise ValueError(f"MANIFEST.json is {found.size} bytes")
        f = tar.extractfile(found)
        manifest: object = json.loads(f.read()) if f else None
        info = cast(dict[str, object], manifest).get("collection_info") if isinstance(manifest, dict) else None
        return cast(dict[str, object], info) if isinstance(info, dict) else {}


def _sha256(f: IO[bytes]) -> str:
    h = hashlib.sha256()
    while chunk := f.read(1024 * 1024):
        h.update(chunk)
    return h.hexdigest()


def contents_digest(data: bytes) -> dict[str, str]:
    """What a collection tarball contains, independent of how it was packed.

    Maps each regular file to the sha256 of its bytes and each link to its
    target. Directories, timestamps, owners, modes and member order are
    ignored, so rebuilding the same files gives the same digest. MANIFEST.json
    and FILES.json are compared as parsed JSON. Raises tarfile.TarError or
    ValueError if the tarball can't be read or holds more than _MAX_CONTENTS.
    """
    digest: dict[str, str] = {}
    total = 0
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        for member in tar:
            name = _member_name(member)
            if member.issym() or member.islnk():
                digest[name] = f"link:{member.linkname}"
                continue
            if not member.isfile() or (f := tar.extractfile(member)) is None:
                continue
            total += member.size
            if total > _MAX_CONTENTS:
                raise ValueError(f"more than {_MAX_CONTENTS} bytes of contents")
            if name in _JSON_FILES:
                body = f.read()
                with contextlib.suppress(ValueError):  # unparseable: compare the raw bytes
                    body = json.dumps(json.loads(body), sort_keys=True).encode()
                digest[name] = hashlib.sha256(body).hexdigest()
            else:
                digest[name] = _sha256(f)
    return digest


def describe_changes(ours: dict[str, str], theirs: dict[str, str], limit: int = 10) -> str:
    """'added a; removed b; changed c' between two contents digests."""
    parts: list[str] = []
    for label, names in (
        ("added", sorted(theirs.keys() - ours.keys())),
        ("removed", sorted(ours.keys() - theirs.keys())),
        ("changed", sorted(k for k in ours.keys() & theirs.keys() if ours[k] != theirs[k])),
    ):
        if names:
            more = f" and {len(names) - limit} more" if len(names) > limit else ""
            parts.append(f"{label} {', '.join(names[:limit])}{more}")
    return "; ".join(parts)
