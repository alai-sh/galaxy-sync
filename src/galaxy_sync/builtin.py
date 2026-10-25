"""ansible.builtin's plugin lines, extracted from ansible-core's latest minor release on PyPI.

ansible.builtin isn't on Galaxy. `sync` checks PyPI once a day and
re-extracts it when there's a new X.Y.0 release, when the stored lines'
extraction format is outdated, or after a rebuild. Pre-releases aren't
what anyone installs, and patch releases don't add plugins, so only X.Y.0
releases count.
"""

import datetime
import hashlib
import io
import pathlib
import re
import tarfile
import tempfile
from dataclasses import dataclass
from typing import cast

import alai_plugin_extractor as ext

from .galaxy import GalaxyClient
from .models import PluginLine, PyPIProject
from .plugins import EXTRACTION_FORMAT, builtin_lines
from .store import Store
from .times import now

PYPI_URL = "https://pypi.org"

# How often `sync` asks PyPI for a new release.
CHECK_EVERY = datetime.timedelta(days=1)

_MINOR_RELEASE = re.compile(r"(\d+)\.(\d+)\.0")

# The parts of the sdist the extraction reads, below its top-level directory.
_EXTRACTED = (
    "lib/ansible/modules/",
    "lib/ansible/plugins/",
    "lib/ansible/config/ansible_builtin_runtime.yml",
    "lib/ansible/release.py",
)


class BuiltinError(Exception):
    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


@dataclass(frozen=True)
class Release:
    version: str
    url: str  # the sdist
    sha256: str


@dataclass(frozen=True)
class Extracted:
    release: Release
    result: ext.Collection
    lines: list[PluginLine]


def due(store: Store) -> bool:
    """Whether PyPI is due a check: daily, or now if the stored lines need extracting again."""
    state = store.state.get("builtin")
    if state is None or state.get("format", EXTRACTION_FORMAT) < EXTRACTION_FORMAT:
        return True
    checked = datetime.datetime.fromisoformat(state["checked"])
    return datetime.datetime.fromisoformat(now()) - checked >= CHECK_EVERY


def latest_minor(client: GalaxyClient) -> Release | None:
    """ansible-core's highest X.Y.0 release with an sdist that isn't yanked."""
    project = cast(PyPIProject, client.get_json("/pypi/ansible-core/json"))
    best: tuple[tuple[int, int], Release] | None = None
    for version, files in project["releases"].items():
        if not (m := _MINOR_RELEASE.fullmatch(version)):
            continue
        sdist = next((f for f in files if f["packagetype"] == "sdist" and not f.get("yanked")), None)
        key = (int(m[1]), int(m[2]))
        if sdist and (best is None or key > best[0]):
            best = (key, Release(version, sdist["url"], sdist["digests"]["sha256"]))
    return best[1] if best else None


def check(client: GalaxyClient, store: Store) -> Extracted | None:
    """Re-extract ansible.builtin into builtin.jsonl if it's out of date; None if it isn't.

    Raises GalaxyError if PyPI can't be read, and BuiltinError if the sdist
    is wrong or can't be extracted.
    """
    release = latest_minor(client)
    state = store.state.get("builtin", {"checked": now()})
    if release is None or (state.get("version"), state.get("format")) == (release.version, EXTRACTION_FORMAT):
        store.state["builtin"] = {**state, "checked": now()}
        return None
    data = client.download(release.url)
    if (sha := hashlib.sha256(data).hexdigest()) != release.sha256:
        raise BuiltinError(
            "sha256_mismatch", f"ansible-core {release.version} sdist: {sha}, PyPI says {release.sha256}"
        )
    try:
        result = _extract_sdist(data)
    except (OSError, tarfile.TarError, ValueError) as e:
        raise BuiltinError("extract_error", f"ansible-core {release.version}: {e}") from None
    lines = builtin_lines(result)
    store.save_builtin(lines)
    store.state["builtin"] = {"checked": now(), "version": release.version, "format": EXTRACTION_FORMAT}
    return Extracted(release, result, lines)


def _extract_sdist(data: bytes) -> ext.Collection:
    """Extract ansible.builtin from an ansible-core sdist.

    The extractor reads ansible-core only from a directory, so the parts it
    reads are unpacked into a temporary one, with tarfile's `data` filter
    keeping every member inside it. Nothing is imported or run.

    Temporary: once alai-plugin-extractor reads ansible-core from a tarball
    (its TODO.md), this goes, and `check` calls
    `ext.extract_bytes(data, type="ansible", docs=True)` instead.
    """
    with tempfile.TemporaryDirectory() as tmp, tarfile.open(fileobj=io.BytesIO(data)) as tar:
        root = pathlib.Path(tmp)
        for member in tar.getmembers():
            _, _, path = member.name.partition("/")  # below ansible_core-X.Y.Z/
            if (member.isfile() or member.issym()) and path.startswith(_EXTRACTED):
                tar.extract(member.replace(name=path), root, filter="data")
        return ext.extract(root, type="ansible", docs=True)
