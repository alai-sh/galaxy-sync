"""Comparing collection tarballs."""

import hashlib
import json

import pytest
from helpers import tarball

from galaxy_sync import artifacts
from galaxy_sync.artifacts import contents_digest, describe_changes


def test_contents_digest(monkeypatch):
    manifest = {"collection_info": {"name": "coll", "namespace": "ns"}}
    a = tarball({"MANIFEST.json": json.dumps(manifest), "x.py": "1", "link": ("link", "x.py")})
    b = tarball({"MANIFEST.json": json.dumps(manifest, indent=4), "x.py": "1", "link": ("link", "x.py")}, mtime=99)
    assert contents_digest(a) == contents_digest(b)
    assert contents_digest(a)["link"] == "link:x.py"
    bad_json = tarball({"MANIFEST.json": "{not json"})
    assert contents_digest(bad_json)["MANIFEST.json"] == hashlib.sha256(b"{not json").hexdigest()

    monkeypatch.setattr(artifacts, "_MAX_CONTENTS", 1)
    with pytest.raises(ValueError, match="more than 1 bytes"):
        contents_digest(a)


def test_describe_changes():
    ours = {f"f{i}": "a" for i in range(12)} | {"same": "s", "edit": "1"}
    theirs = {"same": "s", "edit": "2", "new": "n"}
    assert describe_changes(ours, theirs, limit=2) == "added new; removed f0, f1 and 10 more; changed edit"
