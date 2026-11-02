import os

import pytest

from cloud_optimized_dicom.dicomweb import (
    STUDY_LEVEL_TAGS,
    _get_series_uid_from_uris,
    handle_request,
    is_valid_uid,
)
from cloud_optimized_dicom.tests.conftest import SeriesHandle

DICOMWEB_DATASTORE_PATH = "gs://siskin-172863-pacs/v1.0/dicomweb"


@pytest.fixture(scope="module")
def dicomweb_datastore_path() -> str:
    return DICOMWEB_DATASTORE_PATH


def test_get_series_uid_from_uris(dicomweb_datastore_path: str):
    """
    Test that the series uid can be extracted from the URIs of all standard COD blobs
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
    tar_iter = iter([f"{series_uri}.tar"])
    metadata_iter = iter([os.path.join(series_uri, "metadata.json")])
    index_iter = iter([os.path.join(series_uri, "index.sqlite")])
    thumbnail_iter = iter([os.path.join(series_uri, "thumbnail.mp4")])
    lock_iter = iter([os.path.join(series_uri, ".gradient.lock")])
    junk_iter = iter([f"gs://this/is/a/junk/blob"])

    # make sure that no matter which blob the iterator returns, the series uid is extracted correctly
    assert _get_series_uid_from_uris(tar_iter) == series_uid
    assert _get_series_uid_from_uris(metadata_iter) == series_uid
    assert _get_series_uid_from_uris(index_iter) == series_uid
    assert _get_series_uid_from_uris(thumbnail_iter) == series_uid
    assert _get_series_uid_from_uris(lock_iter) == series_uid

    # expect a ValueError if the iterator has junk
    with pytest.raises(ValueError):
        _get_series_uid_from_uris(junk_iter)

    # expect a ValueError if the iterator is empty
    with pytest.raises(ValueError):
        _get_series_uid_from_uris(iter([]))


@pytest.fixture
def instance_uid(seeded_series: SeriesHandle) -> str:
    """The uid of the first instance in the seeded series."""
    with seeded_series.open(mode="r") as cod:
        return next(iter(cod.get_instances(strict_sorting=False)))


def _series_uri(series: SeriesHandle) -> str:
    return os.path.join(
        series.datastore_path,
        "studies",
        series.study_uid,
        "series",
        series.series_uid,
    )


def test_get_study(seeded_series: SeriesHandle):
    """
    Test retrieving the metadata for a study
    """
    study_uri = os.path.join(
        seeded_series.datastore_path, "studies", seeded_series.study_uid, "metadata"
    )
    result = handle_request(study_uri, seeded_series.client)
    # we expect a dictionary of metadata
    assert isinstance(result, dict)
    # expect all study level tags to be present, and not None
    for tag in STUDY_LEVEL_TAGS:
        assert result[tag]["Value"][0] is not None


def test_get_series(seeded_series: SeriesHandle):
    """
    Test retrieving the metadata for a series
    """
    series_uri = os.path.join(_series_uri(seeded_series), "metadata")
    result = handle_request(series_uri, seeded_series.client)
    # we expect a list of instance metadata dictionaries, one per seeded instance
    assert isinstance(result, list)
    assert len(result) == 2
    for instance in result:
        assert instance["0020000D"]["Value"][0] == seeded_series.study_uid


def test_get_instance(seeded_series: SeriesHandle, instance_uid: str):
    """
    Test retrieving the metadata for an instance
    """
    instance_uri = os.path.join(
        _series_uri(seeded_series), "instances", instance_uid, "metadata"
    )
    result = handle_request(instance_uri, seeded_series.client)
    # we expect a dictionary of metadata
    assert isinstance(result, dict)
    assert result["00080018"]["Value"][0] == instance_uid


def test_get_single_frame(seeded_series: SeriesHandle, instance_uid: str):
    """
    Test retrieving a single frame from an instance
    """
    frame_uri = os.path.join(
        _series_uri(seeded_series), "instances", instance_uid, "frames", "1"
    )
    result = handle_request(frame_uri, seeded_series.client)
    # we expect a non-empty list of bytes
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], bytes)
    assert len(result[0]) > 0


def test_get_too_many_frames_errors(seeded_series: SeriesHandle, instance_uid: str):
    """
    Test retrieving multiple frames from an instance with only one frame raises an error
    """
    frame_uri = os.path.join(
        _series_uri(seeded_series), "instances", instance_uid, "frames", "1,2"
    )
    with pytest.raises(AssertionError):
        handle_request(frame_uri, seeded_series.client)


def test_non_metadata_requests_raise_error(
    seeded_series: SeriesHandle, instance_uid: str
):
    """
    Test that non-metadata requests raise an error
    """
    request_uri = os.path.join(_series_uri(seeded_series), "instances", instance_uid)
    with pytest.raises(AssertionError):
        handle_request(request_uri, seeded_series.client)
