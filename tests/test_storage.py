import hashlib
import io

import pytest
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.stub import Stubber

from galaxy_sync.storage import (
    IMMUTABLE,
    INDEX_CACHE,
    Buckets,
    LocalBackend,
    PreconditionFailed,
    R2Backend,
    R2Config,
    StorageError,
    check_key,
    content_type,
)


def test_local_backend(tmp_path):
    backend = LocalBackend(tmp_path / "bucket")
    assert list(backend.list("")) == []
    assert backend.read("a/b.json") is None
    assert not backend.exists("a/b.json")

    backend.write("a/b.json", b"1")
    backend.write("a/c.json", b"2")
    backend.write("z.json", b"3")
    (tmp_path / "bucket" / "a" / "partial.json.tmp").write_bytes(b"")
    assert backend.read("a/b.json") == b"1"
    assert backend.exists("a/b.json")
    assert list(backend.list("a/")) == ["a/b.json", "a/c.json"]
    assert list(backend.list("")) == ["a/b.json", "a/c.json", "z.json"]

    backend.delete("z.json")
    backend.delete("z.json")  # already gone
    assert list(backend.list("")) == ["a/b.json", "a/c.json"]


def test_local_buckets_layout(tmp_path):
    buckets = Buckets.local(tmp_path)
    buckets.index.write("config.json", b"{}")
    buckets.dl.write("sha256/aa/x.tar.gz", b"")
    buckets.state.write("state.json", b"{}")
    assert (tmp_path / "index-root" / "config.json").is_file()
    assert (tmp_path / "dl-root" / "sha256" / "aa" / "x.tar.gz").is_file()
    assert (tmp_path / "state" / "state.json").is_file()


@pytest.mark.parametrize("key", ["", "/abs", "a/../b", "..", "a//b", "./a", "a/"])
def test_bad_keys_are_rejected(tmp_path, key):
    with pytest.raises(StorageError, match="invalid storage key"):
        check_key(key)
    with pytest.raises(StorageError):
        LocalBackend(tmp_path).write(key, b"")


R2_ENVIRON = {
    "R2_ACCOUNT_ID": "acct",
    "R2_ACCESS_KEY_ID": "key",
    "R2_SECRET_ACCESS_KEY": "s3cret",
    "ALAI_INDEX_BUCKET": "alai-index",
    "ALAI_DL_BUCKET": "alai-dl",
    "ALAI_STATE_BUCKET": "alai-sync-state",
}


def test_r2_config_from_env():
    config = R2Config.from_env(R2_ENVIRON)
    assert (config.account_id, config.index_bucket, config.state_bucket) == ("acct", "alai-index", "alai-sync-state")
    assert "s3cret" not in repr(config)


def test_r2_config_lists_missing_variables():
    environ = {**R2_ENVIRON, "R2_SECRET_ACCESS_KEY": "", "ALAI_DL_BUCKET": ""}
    del environ["R2_ACCOUNT_ID"]
    with pytest.raises(StorageError, match=r"R2_ACCOUNT_ID, R2_SECRET_ACCESS_KEY, ALAI_DL_BUCKET$"):
        R2Config.from_env(environ)


@pytest.fixture
def r2():
    config = R2Config.from_env(R2_ENVIRON)
    client = config.client()
    with Stubber(client) as stub:
        yield R2Backend(client, "alai-index", INDEX_CACHE), stub
        stub.assert_no_pending_responses()


def _put(key, data, content_type, **extra):
    return {
        "Bucket": "alai-index",
        "Key": key,
        "Body": data,
        "ContentType": content_type,
        "CacheControl": INDEX_CACHE,
        **extra,
    }


def test_r2_read(r2):
    backend, stub = r2
    stub.add_response(
        "get_object", {"Body": StreamingBody(io.BytesIO(b"{}"), 2)}, {"Bucket": "alai-index", "Key": "config.json"}
    )
    stub.add_client_error("get_object", "NoSuchKey", http_status_code=404)
    assert backend.read("config.json") == b"{}"
    assert backend.read("missing.json") is None
    with pytest.raises(StorageError):
        backend.read("../escape")


def test_r2_write_sets_headers(r2):
    backend, stub = r2
    stub.add_response("put_object", {}, _put("index/ns/coll.jsonl", b"x", "text/plain; charset=utf-8"))
    stub.add_response("put_object", {}, _put("collections.json", b"{}", "application/json"))
    backend.write("index/ns/coll.jsonl", b"x")
    backend.write("collections.json", b"{}")


def test_r2_replace_is_conditional(r2):
    backend, stub = r2
    stub.add_response("put_object", {}, _put("state.json", b"1", "application/json", IfNoneMatch="*"))
    etag = f'"{hashlib.md5(b"1", usedforsecurity=False).hexdigest()}"'
    stub.add_response("put_object", {}, _put("state.json", b"2", "application/json", IfMatch=etag))
    stub.add_client_error("put_object", "PreconditionFailed", http_status_code=412)
    stub.add_client_error("put_object", "InternalError", http_status_code=500)
    backend.replace("state.json", b"1", None)
    backend.replace("state.json", b"2", b"1")
    with pytest.raises(PreconditionFailed):
        backend.replace("state.json", b"3", b"1")
    with pytest.raises(ClientError):
        backend.replace("state.json", b"3", b"2")


def test_r2_exists_delete_and_list(r2):
    backend, stub = r2
    stub.add_response("head_object", {}, {"Bucket": "alai-index", "Key": "a.json"})
    stub.add_client_error("head_object", "404", http_status_code=404)
    stub.add_client_error("head_object", "403", http_status_code=403)
    stub.add_response("delete_object", {}, {"Bucket": "alai-index", "Key": "a.json"})
    stub.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": "index/a.jsonl"}], "IsTruncated": True, "NextContinuationToken": "t"},
        {"Bucket": "alai-index", "Prefix": "index/"},
    )
    stub.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": "index/b.jsonl"}], "IsTruncated": False},
        {"Bucket": "alai-index", "Prefix": "index/", "ContinuationToken": "t"},
    )
    assert backend.exists("a.json")
    assert not backend.exists("b.json")
    with pytest.raises(ClientError):
        backend.exists("c.json")
    backend.delete("a.json")
    assert list(backend.list("index/")) == ["index/a.jsonl", "index/b.jsonl"]


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("sha256/aa/aa.tar.gz", "application/gzip"),
        ("extract/ns/coll.json.zst", "application/zstd"),
        ("plugins.jsonl", "text/plain; charset=utf-8"),
        ("other.bin", "application/octet-stream"),
    ],
)
def test_content_type(key, expected):
    assert content_type(key) == expected


def test_r2_index_files_are_immutable():
    config = R2Config.from_env(R2_ENVIRON)
    client = config.client()
    backend = R2Backend(client, "alai-index", INDEX_CACHE, {"index/": IMMUTABLE})
    with Stubber(client) as stub:
        stub.add_response(
            "put_object", {}, {**_put("index/aa/x.jsonl", b"x", "text/plain; charset=utf-8"), "CacheControl": IMMUTABLE}
        )
        stub.add_response("put_object", {}, _put("collections.json", b"{}", "application/json"))
        backend.write("index/aa/x.jsonl", b"x")
        backend.write("collections.json", b"{}")
        stub.assert_no_pending_responses()


def test_r2_buckets():
    buckets = Buckets.r2(R2Config.from_env(R2_ENVIRON))
    assert isinstance(buckets.index, R2Backend)
    assert buckets.index.cache_for("index/aa/x.jsonl") == "public, max-age=31536000, immutable"
    assert buckets.index.cache_for("collections.json") == "public, max-age=600, stale-while-revalidate=86400"
    assert isinstance(buckets.dl, R2Backend)
    assert (buckets.dl.bucket, buckets.dl.cache_control) == ("alai-dl", "public, max-age=31536000, immutable")
    assert isinstance(buckets.state, R2Backend)
    assert (buckets.state.bucket, buckets.state.cache_control) == ("alai-sync-state", "private, no-store")
