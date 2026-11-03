"""Builders shared by the tests."""

import gzip
import hashlib
import io
import json
import tarfile

DETAIL = "/api/v3/collections/ns/coll/versions/{}/"
LISTING = "/api/v3/plugin/ansible/content/published/collections/index/?limit=100"
REPO_METADATA = "/api/v3/plugin/ansible/content/published/collections/"
SEARCH = "/api/v3/plugin/ansible/search/collection-versions/"
FEED = SEARCH + "?repository_name=published&order_by=-pulp_created&limit=100"
COUNT = SEARCH + "?repository_name=published&limit=1"


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


def _entry(name, data):
    """A FILES.json entry, a directory without `data`."""
    if data is None:
        return {"name": name, "ftype": "dir", "chksum_type": None, "chksum_sha256": None}
    return {"name": name, "ftype": "file", "chksum_type": "sha256", "chksum_sha256": hashlib.sha256(data).hexdigest()}


def artifact(ns, name, vers, mtime=0, modules=("thing",)):
    """A collection artifact, as ansible-galaxy builds one; a different `mtime` repacks the same files."""
    files = {
        f"plugins/modules/{m}.py": b'DOCUMENTATION = """\nmodule: x\nshort_description: A thing\n"""\n' for m in modules
    }
    dirs = ["plugins", "plugins/modules"] if modules else []
    entries = [_entry(d, None) for d in [".", *dirs]] + [_entry(f, data) for f, data in sorted(files.items())]
    files_json = json.dumps({"files": entries, "format": 1}).encode()
    manifest = {
        "collection_info": {"namespace": ns, "name": name, "version": vers, "dependencies": {}},
        "file_manifest_file": {
            "name": "FILES.json",
            "ftype": "file",
            "chksum_type": "sha256",
            "chksum_sha256": hashlib.sha256(files_json).hexdigest(),
            "format": 1,
        },
        "format": 1,
    }
    buf = io.BytesIO()
    # The gzip header's time is set too, so the same arguments give the same bytes.
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=mtime) as gz, tarfile.open(fileobj=gz, mode="w") as tar:
        for d in dirs:
            info = tarfile.TarInfo(d)
            info.type, info.mode, info.mtime = tarfile.DIRTYPE, 0o755, mtime
            tar.addfile(info)
        for path, data in [
            ("MANIFEST.json", json.dumps(manifest).encode()),
            ("FILES.json", files_json),
            *sorted(files.items()),
        ]:
            info = tarfile.TarInfo(path)
            info.size, info.mode, info.mtime = len(data), 0o644, mtime
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()
