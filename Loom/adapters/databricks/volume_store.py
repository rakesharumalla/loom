"""Large raw files in a Unity Catalog Volume (S3 underneath): content-addressed and write-once.

https://docs.databricks.com/aws/en/volumes/  (access via READ VOLUME / WRITE VOLUME grants)
Objects live at {volume}/blobs/{sha256[:2]}/{sha256}. A blob is never overwritten, so writing it
before the Lakebase metadata transaction cannot corrupt state: if the transaction fails, the blob
is merely unreferenced (garbage-collectable), and metadata never points at a missing blob.
"""
from __future__ import annotations

import io

from databricks.sdk import WorkspaceClient

from pipelines.ingest_core import sha256


class VolumeStore:
    def __init__(self, w: WorkspaceClient, volume_path: str):
        self.w = w
        self.root = volume_path.rstrip("/")

    def blob_path(self, digest: str) -> str:
        return f"{self.root}/blobs/{digest[:2]}/{digest}"

    def exists(self, path: str) -> bool:
        try:
            self.w.files.get_metadata(path)
            return True
        except Exception:
            return False

    def put_blob(self, data: bytes) -> str:
        path = self.blob_path(sha256(data))
        if not self.exists(path):
            self.w.files.upload(path, io.BytesIO(data), overwrite=False)
        return path

    def get_blob(self, path: str) -> bytes:
        return self.w.files.download(path).contents.read()

    def delete_blob(self, path: str) -> None:
        """Only for GC of unreferenced blobs (never called on referenced content)."""
        self.w.files.delete(path)
