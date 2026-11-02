"""Contract tests for `BlobStore`: the behaviour COD relies on from every store.

Running them against `GcsBlobStore` and `FakeBlobStore` alike keeps the fake
faithful to GCS.
"""

import gzip
import io
import os

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud.storage.constants import NEARLINE_STORAGE_CLASS

from cloud_optimized_dicom.blob_store import BlobStore, FakeBlobStore, GcsBlobStore
from cloud_optimized_dicom.tests.conftest import DATASTORE_BASE, ONLINE
from cloud_optimized_dicom.utils import delete_uploaded_blobs, generate_ptr_crc32c


@pytest.fixture(
    params=[
        "fake",
        pytest.param(
            "gcs",
            marks=pytest.mark.skipif(not ONLINE, reason="needs SISKIN_ENV_ENABLED"),
        ),
    ]
)
def store(request: pytest.FixtureRequest) -> BlobStore:
    if request.param == "fake":
        return FakeBlobStore()
    return GcsBlobStore(request.getfixturevalue("gcs_client"))


@pytest.fixture
def prefix(store: BlobStore, worker_namespace: str) -> str:
    path = f"{DATASTORE_BASE}/{worker_namespace}/blob_store"
    delete_uploaded_blobs(store, [path])
    return path


def test_upload_bytes_round_trip(store: BlobStore, prefix: str):
    uri = f"{prefix}/object.bin"
    data = b"cloud optimized dicom"

    written = store.upload_bytes(uri, data)

    assert store.read_bytes(uri) == data
    assert store.stat(uri) == written
    assert written.size == len(data)
    assert written.crc32c == generate_ptr_crc32c(io.BytesIO(data))
    assert written.updated.tzinfo is not None


def test_gzip_encoded_object_reads_back_decompressed(store: BlobStore, prefix: str):
    uri = f"{prefix}/metadata.json"
    compressed = gzip.compress(b'{"key": "value"}')

    written = store.upload_bytes(
        uri, compressed, content_type="application/json", content_encoding="gzip"
    )

    assert store.read_bytes(uri) == b'{"key": "value"}'
    assert written.size == len(compressed)


def test_missing_object(store: BlobStore, prefix: str):
    uri = f"{prefix}/missing"

    assert store.stat(uri) is None
    assert not store.exists(uri)
    with pytest.raises(NotFound):
        store.read_bytes(uri)
    with pytest.raises(NotFound):
        store.delete(uri)


def test_overwrite_changes_generation(store: BlobStore, prefix: str):
    uri = f"{prefix}/object.bin"

    first = store.upload_bytes(uri, b"first")
    second = store.upload_bytes(uri, b"second")

    assert second.generation != first.generation
    assert store.read_bytes(uri) == b"second"


def test_generation_zero_precondition_only_creates(store: BlobStore, prefix: str):
    uri = f"{prefix}/.gradient.lock"

    created = store.upload_bytes(uri, b"lock", if_generation_match=0)

    assert store.stat(uri).generation == created.generation
    with pytest.raises(PreconditionFailed):
        store.upload_bytes(uri, b"stolen", if_generation_match=0)
    assert store.read_bytes(uri) == b"lock"


def test_download_to_file(store: BlobStore, prefix: str, tmp_path):
    uri = f"{prefix}/object.bin"
    store.upload_bytes(uri, b"0123456789")
    whole_path = tmp_path / "whole"
    range_path = tmp_path / "range"

    storage_class = store.download_to_file(uri, str(whole_path))
    store.download_to_file(uri, str(range_path), start=2, end=5)

    assert storage_class == store.stat(uri).storage_class
    assert whole_path.read_bytes() == b"0123456789"
    assert range_path.read_bytes() == b"2345"


def test_upload_file_sets_storage_class(store: BlobStore, prefix: str, tmp_path):
    uri = f"{prefix}/series.tar"
    local_path = tmp_path / "series.tar"
    local_path.write_bytes(b"tar bytes")

    written = store.upload_file(
        uri, str(local_path), storage_class=NEARLINE_STORAGE_CLASS
    )

    assert written.storage_class == NEARLINE_STORAGE_CLASS
    assert store.read_bytes(uri) == b"tar bytes"


def test_delete(store: BlobStore, prefix: str):
    uri = f"{prefix}/object.bin"
    store.upload_bytes(uri, b"data")

    store.delete(uri)

    assert not store.exists(uri)


def test_list_uris_filters_by_prefix(store: BlobStore, prefix: str):
    store.upload_bytes(f"{prefix}/studies/a/series/1.tar", b"")
    store.upload_bytes(f"{prefix}/studies/a/series/2.tar", b"")
    store.upload_bytes(f"{prefix}/studies/b/series/3.tar", b"")

    listed = set(store.list_uris(os.path.join(prefix, "studies", "a")))

    assert listed == {
        f"{prefix}/studies/a/series/1.tar",
        f"{prefix}/studies/a/series/2.tar",
    }
