"""
Unified storage abstraction over local filesystem and AWS S3 via fsspec.

Usage:
    storage = StorageBackend.from_settings(settings)
    with storage.open("path/to/file.csv") as f:
        df = pd.read_csv(f)
"""

from __future__ import annotations

import fsspec

from pipeline.config import PipelineSettings


class StorageBackend:
    """Wraps fsspec to provide a single open() / ls() interface regardless of backend."""

    def __init__(self, protocol: str, storage_options: dict) -> None:
        self.protocol = protocol
        self.storage_options = storage_options
        self._fs: fsspec.AbstractFileSystem = fsspec.filesystem(protocol, **storage_options)

    @classmethod
    def from_settings(cls, settings: PipelineSettings) -> "StorageBackend":
        if settings.storage_backend == "s3":
            return cls(
                protocol="s3",
                storage_options={
                    "key": settings.aws_access_key_id,
                    "secret": settings.aws_secret_access_key,
                    "token": settings.aws_session_token,
                },
            )
        return cls(protocol="file", storage_options={})

    def open(self, path: str, mode: str = "rb"):
        """Return a file-like object. Compatible with pandas / pyarrow readers."""
        return self._fs.open(path, mode=mode)

    def ls(self, path: str, detail: bool = False):
        """List contents of a path."""
        return self._fs.ls(path, detail=detail)

    def exists(self, path: str) -> bool:
        return self._fs.exists(path)

    def move(self, src: str, dst: str) -> None:
        """Move/rename a file within the same filesystem."""
        self._fs.move(src, dst)

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        self._fs.makedirs(path, exist_ok=exist_ok)
