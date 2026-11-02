import gzip
import itertools
import threading
from base64 import b64encode
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Optional

import google_crc32c
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud.storage.constants import STANDARD_STORAGE_CLASS

from cloud_optimized_dicom.blob_store.interface import BlobInfo, BlobStore


@dataclass(frozen=True)
class _StoredObject:
    data: bytes
    content_encoding: Optional[str]
    info: BlobInfo


class FakeBlobStore(BlobStore):
    """In-memory `BlobStore` that behaves like GCS for the operations COD uses,
    so COD runs with no network or credentials.

    Objects are keyed by their full URI, so any URI scheme works. Every write
    gets a new generation, and `content_encoding="gzip"` objects are
    decompressed on read, as GCS serves them. Writes and deletes are atomic
    across threads, so a generation precondition admits one writer as GCS does.
    """

    def __init__(self):
        self._objects: dict[str, _StoredObject] = {}
        self._generations = itertools.count(1)
        self._write_lock = threading.Lock()

    def stat(self, uri: str) -> Optional[BlobInfo]:
        stored = self._objects.get(uri)
        return None if stored is None else stored.info

    def read_bytes(self, uri: str) -> bytes:
        return _decoded(self._get(uri))

    def download_to_file(
        self,
        uri: str,
        path: str,
        *,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> str:
        stored = self._get(uri)
        if start is None and end is None:
            data = _decoded(stored)
        else:
            data = stored.data[start or 0 : None if end is None else end + 1]
        with open(path, "wb") as f:
            f.write(data)
        return stored.info.storage_class

    def upload_bytes(
        self,
        uri: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        content_encoding: Optional[str] = None,
        if_generation_match: Optional[int] = None,
    ) -> BlobInfo:
        with self._write_lock:
            if if_generation_match is not None:
                current = self.stat(uri)
                current_generation = 0 if current is None else current.generation
                if current_generation != if_generation_match:
                    raise PreconditionFailed(
                        f"{uri} is at generation {current_generation}, not {if_generation_match}"
                    )
            return self._put(uri, data, content_encoding, STANDARD_STORAGE_CLASS)

    def upload_file(
        self, uri: str, path: str, *, storage_class: Optional[str] = None
    ) -> BlobInfo:
        with open(path, "rb") as f:
            data = f.read()
        with self._write_lock:
            return self._put(uri, data, None, storage_class or STANDARD_STORAGE_CLASS)

    def delete(self, uri: str) -> None:
        with self._write_lock:
            self._get(uri)
            del self._objects[uri]

    def list_uris(self, prefix: str) -> Iterator[str]:
        yield from sorted(uri for uri in self._objects if uri.startswith(prefix))

    def _get(self, uri: str) -> _StoredObject:
        if (stored := self._objects.get(uri)) is None:
            raise NotFound(f"No object at {uri}")
        return stored

    def _put(
        self,
        uri: str,
        data: bytes,
        content_encoding: Optional[str],
        storage_class: str,
    ) -> BlobInfo:
        info = BlobInfo(
            generation=next(self._generations),
            size=len(data),
            crc32c=b64encode(google_crc32c.Checksum(data).digest()).decode("utf-8"),
            storage_class=storage_class,
            updated=datetime.now(timezone.utc),
        )
        self._objects[uri] = _StoredObject(data, content_encoding, info)
        return info


def _decoded(stored: _StoredObject) -> bytes:
    if stored.content_encoding == "gzip":
        return gzip.decompress(stored.data)
    return stored.data
