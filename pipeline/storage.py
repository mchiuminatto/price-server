"""
Unified storage abstraction over local filesystem, AWS S3, GCS, and
S3-compatible stores (DigitalOcean Spaces, MinIO) via fsspec.

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
            opts: dict = {
                "key": settings.aws_access_key_id or None,
                "secret": settings.aws_secret_access_key or None,
                "token": settings.aws_session_token or None,
            }
            # Custom endpoint for DigitalOcean Spaces, MinIO, etc.
            if settings.s3_endpoint_url:
                opts["client_kwargs"] = {"endpoint_url": settings.s3_endpoint_url}
            return cls(protocol="s3", storage_options=opts)

        if settings.storage_backend == "gcs":
            opts = {}
            if settings.gcs_project:
                opts["project"] = settings.gcs_project
            # gcsfs picks up GOOGLE_APPLICATION_CREDENTIALS automatically
            return cls(protocol="gcs", storage_options=opts)

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
