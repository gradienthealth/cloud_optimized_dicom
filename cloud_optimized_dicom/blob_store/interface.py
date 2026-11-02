from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, Optional


@dataclass(frozen=True)
class BlobInfo:
    """Metadata of one stored object.

    `crc32c` is the base64-encoded CRC32C of the stored bytes, in the form GCS
    reports it. `generation` changes on every write to the object. `updated` is
    timezone-aware.
    """

    generation: int
    size: int
    crc32c: str
    storage_class: str
    updated: datetime


class BlobStore(ABC):
    """Object storage that COD reads and writes series through, addressed by URI
    (e.g. `gs://bucket/path/to/object`).

    Every implementation raises `google.api_core.exceptions.NotFound` for a
    missing object and `google.api_core.exceptions.PreconditionFailed` for a
    failed generation precondition, so callers that already catch the GCS
    errors work against any store.
    """

    @abstractmethod
    def stat(self, uri: str) -> Optional[BlobInfo]:
        """Returns the metadata of the object at `uri`, or `None` if there is none."""

    def exists(self, uri: str) -> bool:
        """Returns whether an object exists at `uri`."""
        return self.stat(uri) is not None

    @abstractmethod
    def read_bytes(self, uri: str) -> bytes:
        """Returns the contents of the object at `uri`.

        An object written with `content_encoding="gzip"` comes back decompressed,
        as GCS serves it.

        Raises:
            NotFound: if there is no object at `uri`.
        """

    @abstractmethod
    def download_to_file(
        self,
        uri: str,
        path: str,
        *,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> str:
        """Writes the contents of the object at `uri` to the local file `path`.

        `start` and `end` select an inclusive byte range of the stored bytes;
        omitting both downloads the whole object, decoded like `read_bytes`.

        Returns:
            The storage class of the object.

        Raises:
            NotFound: if there is no object at `uri`.
        """

    @abstractmethod
    def upload_bytes(
        self,
        uri: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        content_encoding: Optional[str] = None,
        if_generation_match: Optional[int] = None,
    ) -> BlobInfo:
        """Writes `data` to `uri`, replacing any object already there.

        `if_generation_match` makes the write conditional on the current
        object's generation; `0` means the write succeeds only if no object
        exists yet.

        Returns:
            The metadata of the written object.

        Raises:
            PreconditionFailed: if `if_generation_match` does not match.
        """

    @abstractmethod
    def upload_file(
        self, uri: str, path: str, *, storage_class: Optional[str] = None
    ) -> BlobInfo:
        """Writes the local file `path` to `uri`, replacing any object already there.

        `storage_class` defaults to the store's default class.

        Returns:
            The metadata of the written object.
        """

    @abstractmethod
    def delete(self, uri: str) -> None:
        """Deletes the object at `uri`.

        Raises:
            NotFound: if there is no object at `uri`.
        """

    @abstractmethod
    def list_uris(self, prefix: str) -> Iterator[str]:
        """Yields the URI of every object whose URI starts with `prefix`."""
