"""Where the sync's output lives: the index, artifact and state buckets.

`Store` reads and writes through one `Backend` per bucket. `LocalBackend` is a
directory standing in for a bucket, used for tests and local runs.
`R2Backend` is a Cloudflare R2 bucket, configured from the environment only,
so credentials never appear on a command line or in shell history.
"""

import functools
import hashlib
import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import boto3
import botocore.config
import botocore.exceptions

if TYPE_CHECKING:
    from types_boto3_s3 import S3Client


class StorageError(Exception):
    pass


class PreconditionFailed(StorageError):
    """A conditional write found the object changed since it was read."""


class Backend(Protocol):
    """One bucket. Keys are relative POSIX paths such as `index/ns/name.jsonl`."""

    def read(self, key: str) -> bytes | None:
        """Return the object's bytes, or None if it doesn't exist."""
        ...

    def write(self, key: str, data: bytes) -> None:
        """Create or replace an object. Readers never see a partial write."""
        ...

    def replace(self, key: str, data: bytes, previous: bytes | None) -> None:
        """Write only if the object still holds `previous` (None: doesn't exist), else raise PreconditionFailed."""
        ...

    def exists(self, key: str) -> bool: ...

    def delete(self, key: str) -> None:
        """Remove an object; a missing one is not an error."""
        ...

    def list(self, prefix: str) -> Iterator[str]:
        """Every key starting with `prefix`, in sorted order."""
        ...


def check_key(key: str) -> str:
    """Reject keys that could escape the bucket: absolute, empty or `..` parts."""
    if any(part in ("", ".", "..") for part in key.split("/")):
        raise StorageError(f"invalid storage key {key!r}")
    return key


class LocalBackend:
    """A directory standing in for a bucket."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, key: str) -> Path:
        return self.root / check_key(key)

    def read(self, key: str) -> bytes | None:
        path = self.path(key)
        return path.read_bytes() if path.is_file() else None

    def write(self, key: str, data: bytes) -> None:
        path = self.path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        _ = tmp.write_bytes(data)
        _ = tmp.replace(path)

    def replace(self, key: str, data: bytes, previous: bytes | None) -> None:
        # Not atomic, which is fine for a directory only one run uses.
        if self.read(key) != previous:
            raise PreconditionFailed(f"{key} changed since it was read")
        self.write(key, data)

    def exists(self, key: str) -> bool:
        return self.path(key).is_file()

    def delete(self, key: str) -> None:
        self.path(key).unlink(missing_ok=True)

    def list(self, prefix: str) -> Iterator[str]:
        if not self.root.is_dir():
            return
        keys = (p.relative_to(self.root).as_posix() for p in self.root.rglob("*") if p.is_file())
        yield from sorted(k for k in keys if k.startswith(prefix) and not k.endswith(".tmp"))


@dataclass(frozen=True)
class Buckets:
    index: Backend
    dl: Backend
    state: Backend

    @classmethod
    def local(cls, root: Path) -> Buckets:
        """The layout `--store DIR` uses: one subdirectory per bucket."""
        return cls(
            LocalBackend(root / "index-root"),
            LocalBackend(root / "dl-root"),
            LocalBackend(root / "state"),
        )

    @classmethod
    def r2(cls, config: R2Config) -> Buckets:
        client = config.client()
        return cls(
            R2Backend(client, config.index_bucket, INDEX_CACHE, {INDEX_FILES: IMMUTABLE}),
            R2Backend(client, config.dl_bucket, IMMUTABLE),
            R2Backend(client, config.state_bucket, PRIVATE),
        )


# Cache-Control, as docs/index-spec/README.md asks. Index files and artifacts
# are named by their sha256, so they never change and are cached for a year.
# The rest of the index bucket changes on every publish, and isn't purged: 10
# minutes matches the sync's schedule, `*/10` in scheduler/wrangler.toml, so a
# cached copy is at most about one sync behind; change both together.
# stale-while-revalidate lets Cloudflare answer at once when its copy
# expires, and fetch from R2 behind it. No s-maxage, which would stop that:
# Cloudflare treats it as proxy-revalidate, so it never serves an expired
# copy. See docs/storage.md, "Caching".
# The state bucket isn't public; no-store keeps it that way if it ever is.
INDEX_CACHE = "public, max-age=600, stale-while-revalidate=86400"
IMMUTABLE = "public, max-age=31536000, immutable"
PRIVATE = "private, no-store"
# The index bucket's prefix for index files, which get IMMUTABLE.
INDEX_FILES = "index/"

# Clients mustn't depend on Content-Type; these let Cloudflare compress the
# text and browsers show it.
CONTENT_TYPES = {
    ".json": "application/json",
    ".jsonl": "text/plain; charset=utf-8",
    ".tar.gz": "application/gzip",
    ".zst": "application/zstd",
}


def content_type(key: str) -> str:
    return next((t for suffix, t in CONTENT_TYPES.items() if key.endswith(suffix)), "application/octet-stream")


class R2Backend:
    """An R2 bucket, through its S3-compatible API."""

    def __init__(
        self, client: S3Client, bucket: str, cache_control: str, overrides: Mapping[str, str] | None = None
    ) -> None:
        self.client = client
        self.bucket = bucket
        self.cache_control = cache_control
        # Cache-Control by key prefix, for keys that differ from the bucket's.
        self.overrides = dict(overrides or {})

    def cache_for(self, key: str) -> str:
        return next((value for prefix, value in self.overrides.items() if key.startswith(prefix)), self.cache_control)

    def read(self, key: str) -> bytes | None:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=check_key(key))
        except self.client.exceptions.NoSuchKey:
            return None
        return response["Body"].read()

    def write(self, key: str, data: bytes) -> None:
        _ = self.client.put_object(
            Bucket=self.bucket,
            Key=check_key(key),
            Body=data,
            ContentType=content_type(key),
            CacheControl=self.cache_for(key),
        )

    def replace(self, key: str, data: bytes, previous: bytes | None) -> None:
        put = functools.partial(
            self.client.put_object,
            Bucket=self.bucket,
            Key=check_key(key),
            Body=data,
            ContentType=content_type(key),
            CacheControl=self.cache_for(key),
        )
        try:
            if previous is None:
                _ = put(IfNoneMatch="*")
            else:  # a single-part upload's ETag is the MD5 of its bytes
                _ = put(IfMatch=f'"{hashlib.md5(previous, usedforsecurity=False).hexdigest()}"')
        except botocore.exceptions.ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("PreconditionFailed", "412"):
                raise PreconditionFailed(f"{key} changed since it was read") from None
            raise

    def exists(self, key: str) -> bool:
        try:
            _ = self.client.head_object(Bucket=self.bucket, Key=check_key(key))
        except botocore.exceptions.ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                return False
            raise
        return True

    def delete(self, key: str) -> None:
        _ = self.client.delete_object(Bucket=self.bucket, Key=check_key(key))

    def list(self, prefix: str) -> Iterator[str]:
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            yield from (obj["Key"] for obj in page.get("Contents", []) if "Key" in obj)


R2_ENV = {
    "account_id": "R2_ACCOUNT_ID",
    "access_key_id": "R2_ACCESS_KEY_ID",
    "secret_access_key": "R2_SECRET_ACCESS_KEY",
    "index_bucket": "ALAI_INDEX_BUCKET",
    "dl_bucket": "ALAI_DL_BUCKET",
    "state_bucket": "ALAI_STATE_BUCKET",
}


@dataclass(frozen=True)
class R2Config:
    account_id: str
    access_key_id: str
    secret_access_key: str = field(repr=False)
    index_bucket: str
    dl_bucket: str
    state_bucket: str

    @classmethod
    def from_env(cls, environ: Mapping[str, str] = os.environ) -> R2Config:
        if missing := [var for var in R2_ENV.values() if not environ.get(var)]:
            raise StorageError(f"R2 storage needs these environment variables set: {', '.join(missing)}")
        return cls(**{attr: environ[var] for attr, var in R2_ENV.items()})

    def client(self) -> S3Client:
        return boto3.client(
            "s3",
            endpoint_url=f"https://{self.account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name="auto",
            config=botocore.config.Config(
                retries={"mode": "standard", "max_attempts": 5},
                connect_timeout=10,
                read_timeout=60,
            ),
        )
