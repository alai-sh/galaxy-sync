"""Builders shared by the tests."""

import io
import json
import tarfile

DETAIL = "/api/v3/collections/ns/coll/versions/{}/"
LISTING = "/api/v3/plugin/ansible/content/published/collections/index/?limit=100"
REPO_METADATA = "/api/v3/plugin/ansible/content/published/collections/"
SEARCH = "/api/v3/plugin/ansible/search/collection-versions/"
FEED = SEARCH + "?repository_name=published&order_by=-pulp_created&limit=100"
COUNT = SEARCH + "?repository_name=published&limit=1"
PYPI = "/pypi/ansible-core/json"


def feed_row(ns, name, version, created):
    return {"collection_version": {"namespace": ns, "name": name, "version": version, "pulp_created": created}}


UNAVAILABLE = (503, {"detail": "Service Unavailable"}, {})


def tarball(members, mtime=0):
    """A gzipped tarball of {path: content}; a content of ("link", target) is a symlink."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for path, content in members.items():
            info = tarfile.TarInfo(path)
            info.mtime = mtime
            if isinstance(content, tuple):
                info.type, info.linkname = tarfile.SYMTYPE, content[1]
                tar.addfile(info)
                continue
            data = content.encode() if isinstance(content, str) else content
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def artifact(ns, name, vers, mtime=0, modules=("thing",)):
    """A collection artifact; a different `mtime` repacks the same files."""
    files = {"MANIFEST.json": json.dumps({"collection_info": {"namespace": ns, "name": name, "version": vers}})}
    for module in modules:
        files[f"plugins/modules/{module}.py"] = 'DOCUMENTATION = "short_description: A thing"\n'
    return tarball(files, mtime)


def extraction(store, sha256, ns="ns", name="coll"):
    """The stored extraction for one artifact, or None."""
    return store.load_extractions(ns, name).get(sha256)
