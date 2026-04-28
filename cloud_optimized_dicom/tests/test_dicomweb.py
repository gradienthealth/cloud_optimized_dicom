import os

import pytest
from google.cloud import storage

from cloud_optimized_dicom.dicomweb import (
    STUDY_LEVEL_TAGS,
    _get_series_uid_from_blob_iterator,
    handle_request,
    is_valid_uid,
)

DICOMWEB_DATASTORE_PATH = "gs://siskin-172863-pacs/v1.0/dicomweb"


@pytest.fixture(scope="module")
def dicomweb_datastore_path() -> str:
    return DICOMWEB_DATASTORE_PATH


def test_get_series_uid_from_blob_iterator(dicomweb_datastore_path: str):
    """
    Test that the series uid can be extracted from all standard COD blobs
    """
    # simulate iterators returning every possible blob first
    series_uid = "1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150"
    series_uri = os.path.join(
        dicomweb_datastore_path,
        "studies",
        "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
        "series",
        series_uid,
    )
    tar_iter = iter([storage.Blob.from_string(f"{series_uri}.tar")])
    metadata_iter = iter(
        [storage.Blob.from_string(os.path.join(series_uri, "metadata.json"))]
    )
    index_iter = iter(
        [storage.Blob.from_string(os.path.join(series_uri, "index.sqlite"))]
    )
    thumbnail_iter = iter(
        [storage.Blob.from_string(os.path.join(series_uri, "thumbnail.mp4"))]
    )
    lock_iter = iter(
        [storage.Blob.from_string(os.path.join(series_uri, ".gradient.lock"))]
    )
    junk_iter = iter([storage.Blob.from_string(f"gs://this/is/a/junk/blob")])

    # make sure that no matter which blob the iterator returns, the series uid is extracted correctly
    assert _get_series_uid_from_blob_iterator(tar_iter) == series_uid
    assert _get_series_uid_from_blob_iterator(metadata_iter) == series_uid
    assert _get_series_uid_from_blob_iterator(index_iter) == series_uid
    assert _get_series_uid_from_blob_iterator(thumbnail_iter) == series_uid
    assert _get_series_uid_from_blob_iterator(lock_iter) == series_uid

    # expect a ValueError if the iterator has junk
    with pytest.raises(ValueError):
        _get_series_uid_from_blob_iterator(junk_iter)

    # expect a ValueError if the iterator is empty
    with pytest.raises(ValueError):
        _get_series_uid_from_blob_iterator(iter([]))


def test_get_study(gcs_client: storage.Client, dicomweb_datastore_path: str):
    """
    Test retrieving the metadata for a study
    """
    study_uri = os.path.join(
        dicomweb_datastore_path,
        "studies",
        "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
        "metadata",
    )
    result = handle_request(study_uri, gcs_client)
    # we expect a dictionary of metadata
    assert isinstance(result, dict)
    # expect all study level tags to be present, and not None
    for tag in STUDY_LEVEL_TAGS:
        assert result[tag]["Value"][0] is not None


def test_get_series(gcs_client: storage.Client, dicomweb_datastore_path: str):
    """
    Test retrieving the metadata for a series
    """
    series_uri = os.path.join(
        dicomweb_datastore_path,
        "studies",
        "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
        "series",
        "1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150",
        "metadata",
    )
    result = handle_request(series_uri, gcs_client)
    # we expect a list of instance metadata dictionaries
    assert isinstance(result, list)
    # there happen to be 82 instances in this series
    assert len(result) == 82
    # check something in each instance (e.g. series uid)
    series_uid = result[0]["0020000D"]["Value"][0]
    assert is_valid_uid(series_uid)
    for instance in result:
        assert instance["0020000D"]["Value"][0] == series_uid


def test_get_instance(gcs_client: storage.Client, dicomweb_datastore_path: str):
    """
    Test retrieving the metadata for an instance
    """
    instance_uri = os.path.join(
        dicomweb_datastore_path,
        "studies",
        "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
        "series",
        "1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150",
        "instances",
        "1.2.826.0.1.3680043.8.498.10368404844741579486264078308290534273",
        "metadata",
    )
    result = handle_request(instance_uri, gcs_client)
    # we expect a dictionary of metadata
    assert isinstance(result, dict)
    # check something in the metadata (e.g. series uid)
    series_uid = result["0020000D"]["Value"][0]
    assert is_valid_uid(series_uid)


def test_get_single_frame(gcs_client: storage.Client, dicomweb_datastore_path: str):
    """
    Test retrieving a single frame from an instance
    """
    frame_uri = os.path.join(
        dicomweb_datastore_path,
        "studies",
        "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
        "series",
        "1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150",
        "instances",
        "1.2.826.0.1.3680043.8.498.10368404844741579486264078308290534273",
        "frames",
        "1",
    )
    result = handle_request(frame_uri, gcs_client)
    # we expect a non-empty list of bytes
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], bytes)
    assert len(result[0]) > 0


def test_get_too_many_frames_errors(
    gcs_client: storage.Client, dicomweb_datastore_path: str
):
    """
    Test retrieving multiple frames from an instance with only one frame raises an error
    """
    frame_uri = os.path.join(
        dicomweb_datastore_path,
        "studies",
        "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
        "series",
        "1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150",
        "instances",
        "1.2.826.0.1.3680043.8.498.10368404844741579486264078308290534273",
        "frames",
        "1,2",
    )
    with pytest.raises(AssertionError):
        handle_request(frame_uri, gcs_client)


def test_non_metadata_requests_raise_error(
    gcs_client: storage.Client, dicomweb_datastore_path: str
):
    """
    Test that non-metadata requests raise an error
    """
    request_uri = os.path.join(
        dicomweb_datastore_path,
        "studies",
        "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
        "series",
        "1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150",
        "instances",
        "1.2.826.0.1.3680043.8.498.10368404844741579486264078308290534273",
    )
    with pytest.raises(AssertionError):
        handle_request(request_uri, gcs_client)
