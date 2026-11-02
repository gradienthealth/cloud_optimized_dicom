from typing import Iterator, Optional, Union

from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

from cloud_optimized_dicom.blob_store.interface import BlobInfo, BlobStore

_GCS_SCHEME = "gs://"


def _split_gcs_uri(uri: str) -> tuple[str, str]:
    """Returns the bucket name and object name of a `gs://` URI.

    Raises:
        ValueError: if `uri` is not a `gs://` URI.
    """
    if not uri.startswith(_GCS_SCHEME):
        raise ValueError(f"Expected a {_GCS_SCHEME} URI, got: {uri}")
    bucket_name, _, object_name = uri[len(_GCS_SCHEME) :].partition("/")
    return bucket_name, object_name


def _blob_info(blob: storage.Blob) -> BlobInfo:
    return BlobInfo(
        generation=blob.generation,
        size=blob.size,
        crc32c=blob.crc32c,
        storage_class=blob.storage_class,
        updated=blob.updated,
    )


class GcsBlobStore(BlobStore):
    """`BlobStore` backed by Google Cloud Storage.

    Every request retries with the client library's `DEFAULT_RETRY`.
    """

    def __init__(self, client: storage.Client):
        self.client = client

    def _blob(self, uri: str) -> storage.Blob:
        bucket_name, object_name = _split_gcs_uri(uri)
        return self.client.bucket(bucket_name).blob(object_name)

    def stat(self, uri: str) -> Optional[BlobInfo]:
        bucket_name, object_name = _split_gcs_uri(uri)
        blob = self.client.bucket(bucket_name).get_blob(
            object_name, retry=DEFAULT_RETRY
        )
        return None if blob is None else _blob_info(blob)

    def exists(self, uri: str) -> bool:
        return self._blob(uri).exists(retry=DEFAULT_RETRY)

    def read_bytes(self, uri: str) -> bytes:
        return self._blob(uri).download_as_bytes(retry=DEFAULT_RETRY)

    def download_to_file(
        self,
        uri: str,
        path: str,
        *,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> str:
        blob = self._blob(uri)
        blob.download_to_filename(path, start=start, end=end, retry=DEFAULT_RETRY)
        return blob.storage_class

    def upload_bytes(
        self,
        uri: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        content_encoding: Optional[str] = None,
        if_generation_match: Optional[int] = None,
    ) -> BlobInfo:
        blob = self._blob(uri)
        blob.content_encoding = content_encoding
        blob.upload_from_string(
            data,
            content_type=content_type,
            if_generation_match=if_generation_match,
            retry=DEFAULT_RETRY,
        )
        return _blob_info(blob)

    def upload_file(
        self, uri: str, path: str, *, storage_class: Optional[str] = None
    ) -> BlobInfo:
        blob = self._blob(uri)
        if storage_class is not None:
            blob.storage_class = storage_class
        blob.upload_from_filename(path, retry=DEFAULT_RETRY)
        return _blob_info(blob)

    def delete(self, uri: str) -> None:
        self._blob(uri).delete(retry=DEFAULT_RETRY)

    def list_uris(self, prefix: str) -> Iterator[str]:
        bucket_name, object_prefix = _split_gcs_uri(prefix)
        for blob in self.client.list_blobs(
            bucket_name, prefix=object_prefix, retry=DEFAULT_RETRY
        ):
            yield f"{_GCS_SCHEME}{bucket_name}/{blob.name}"


def as_blob_store(client: Union[storage.Client, BlobStore]) -> BlobStore:
    """Returns `client` if it is already a `BlobStore`, or a `GcsBlobStore` wrapping it."""
    if isinstance(client, BlobStore):
        return client
    return GcsBlobStore(client)
