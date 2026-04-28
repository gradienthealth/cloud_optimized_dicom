import warnings

import pytest
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.errors import LockAcquisitionError, LockVerificationError
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.utils import delete_uploaded_blobs

LOCK_STUDY_UID = "1.2.3.4.5.6.7.8.9.10"
LOCK_SERIES_UID = "1.2.3.4.5.6.7.8.9.10"


@pytest.fixture
def lock_study_uid() -> str:
    return LOCK_STUDY_UID


@pytest.fixture
def lock_series_uid() -> str:
    return LOCK_SERIES_UID


def test_mode_unspecified(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that not specifying mode raises an error"""
    with pytest.raises(ValueError):
        CODObject(
            datastore_path=datastore_path,
            client=gcs_client,
            study_uid=lock_study_uid,
            series_uid=lock_series_uid,
        )


def test_lock_immutability(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that the lock flag is immutable"""
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=lock_study_uid,
        series_uid=lock_series_uid,
        mode="r",
    ) as cod:
        with pytest.raises(AttributeError):
            cod.lock = True


def test_lock_uniqueness(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that you cannot have two CODObjects with the same lock"""
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=lock_study_uid,
        series_uid=lock_series_uid,
        mode="w",
    ) as cod1:
        with pytest.raises(LockAcquisitionError):
            with CODObject(
                client=gcs_client,
                datastore_path=datastore_path,
                study_uid=lock_study_uid,
                series_uid=lock_series_uid,
                mode="w",
            ) as cod2:
                pass


def test_read_mode(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that you can read metadata in read mode"""
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=lock_study_uid,
        series_uid=lock_series_uid,
        mode="r",
    ) as cod:
        metadata = cod.get_metadata()
        assert metadata.study_uid == lock_study_uid
        assert metadata.series_uid == lock_series_uid


def test_concurrent_read(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that you can read while another cod has a lock"""
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=lock_study_uid,
        series_uid=lock_series_uid,
        mode="w",
    ) as cod1:
        locked_metadata = cod1.get_metadata()
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=lock_study_uid,
            series_uid=lock_series_uid,
            mode="r",
        ) as cod2:
            read_metadata = cod2.get_metadata()
            assert read_metadata.study_uid == lock_study_uid
            assert read_metadata.series_uid == lock_series_uid
            assert locked_metadata.study_uid == lock_study_uid
            assert locked_metadata.series_uid == lock_series_uid


def test_read_mode_allows_reads(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that mode='r' allows read operations without errors"""
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=lock_study_uid,
        series_uid=lock_series_uid,
        mode="r",
    ) as cod:
        # Read operations should work without any dirty parameter
        metadata = cod.get_metadata()
        assert metadata.study_uid == lock_study_uid
        assert metadata.series_uid == lock_series_uid


def test_deprecation_warning_for_dirty_param(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that using dirty parameter emits a deprecation warning"""
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=lock_study_uid,
        series_uid=lock_series_uid,
        mode="r",
    ) as cod:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cod.get_metadata(dirty=True)
            # Check that a deprecation warning was issued for the dirty parameter
            assert any(
                "dirty" in str(warning.message) for warning in w
            ), "Expected deprecation warning for 'dirty' parameter"


def test_lock_gone_on_cleanup(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that we get an error if the lock disappears while the COD is active"""
    with pytest.raises(LockVerificationError):
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=lock_study_uid,
            series_uid=lock_series_uid,
            mode="w",
        ) as cod:
            cod._locker.get_lock_blob().delete()
        # when the with block exits, cod will attempt to release the lock and will find it missing


@pytest.mark.skip(reason="skipping until we have a sync method")
def test_lock_gone_on_sync(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that we get an error if the lock disappears while the COD is syncing"""
    # don't use a with block so we can delete the lock blob manually
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=lock_study_uid,
        series_uid=lock_series_uid,
        mode="w",
    )
    cod_obj.get_lock_blob().delete()
    with pytest.raises(LockVerificationError):
        cod_obj._sync()


def test_lock_changes(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that we get an error if the lock changes while the COD is active"""
    with pytest.raises(LockVerificationError):
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=lock_study_uid,
            series_uid=lock_series_uid,
            mode="w",
        ) as cod:
            # simulate some other cod somehow stealing the lock
            cod._locker.get_lock_blob().upload_from_string(
                "", content_type="application/octet-stream"
            )
        # when the with block exits, cod will attempt to release the lock and will find it changed
    # cod will have failed to delete the lock since it assumes it belongs to another cod, so we need to clean up after ourselves
    delete_uploaded_blobs(gcs_client, [datastore_path])


def test_lock_stolen_during_metadata_fetch(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that we get an error if another process creates the lock while we're fetching metadata"""

    original_get_metadata = CODObject._get_metadata

    def mock_get_metadata(self: CODObject, create_if_missing=True):
        # First get the metadata normally
        result = original_get_metadata(self, create_if_missing=create_if_missing)
        # Then simulate another process creating the lock file
        self._locker.get_lock_blob().upload_from_string(
            "competing lock",
            content_type="application/json",
            if_generation_match=0,
        )
        return result

    # Patch the _get_metadata method temporarily for this test.
    # Now in acquire_lock, it will now get_metadata, upload a lock, and then attempt to upload the lock again
    # We expect this to raise our assertion error about a stolen lock
    CODObject._get_metadata = mock_get_metadata

    try:
        with pytest.raises(
            LockAcquisitionError,
            match="COD:LOCK:ACQUISITION_FAILED:STOLEN_DURING_METADATA_FETCH",
        ):
            CODObject(
                client=gcs_client,
                datastore_path=datastore_path,
                study_uid=lock_study_uid,
                series_uid=lock_series_uid,
                mode="w",
            )
    finally:
        # Restore the original method
        CODObject._get_metadata = original_get_metadata
        # Clean up any locks that might have been created
        delete_uploaded_blobs(gcs_client, [datastore_path])


def test_lock_released_after_non_sync_exception(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that the lock is released after a non-sync exception (only local state is corrupt)"""
    with pytest.raises(ValueError):
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=lock_study_uid,
            series_uid=lock_series_uid,
            mode="w",
        ) as cod:
            raise ValueError("test")
    # Sync should NOT have been called -> tracker vars should be False
    assert not cod._tar_synced
    assert not cod._metadata_synced
    # The lock should have been released (only sync failures leave hanging locks)
    assert not cod._locker.get_lock_blob().exists()


def test_release_failure_preserves_original_exception(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that if release fails during cleanup of a non-sync exception, the original exception propagates"""
    with pytest.raises(ValueError):
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=lock_study_uid,
            series_uid=lock_series_uid,
            mode="w",
        ) as cod:
            # tamper with the lock so release() will fail
            cod._locker.get_lock_blob().upload_from_string(
                "", content_type="application/octet-stream"
            )
            raise ValueError("original error")
    # caller should see ValueError, not LockVerificationError
    # lock is left hanging (tampered), clean up
    delete_uploaded_blobs(gcs_client, [datastore_path])


def test_override_stale_lock(
    gcs_client: storage.Client,
    datastore_path: str,
    lock_study_uid: str,
    lock_series_uid: str,
):
    """Test that we can override a stale lock"""
    # leave a hanging lock by causing a sync failure (tamper with lock generation)
    with pytest.raises(LockVerificationError):
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=lock_study_uid,
            series_uid=lock_series_uid,
            mode="w",
        ) as cod:
            # re-upload lock with same metadata but new generation → verify fails on exit
            lock_blob = cod._locker.get_lock_blob()
            lock_blob.content_encoding = "gzip"
            lock_blob.upload_from_string(
                cod._metadata.to_gzipped_json(),
                content_type="application/json",
            )
    # because there's a hanging lock, we should get an error
    with pytest.raises(LockAcquisitionError):
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=lock_study_uid,
            series_uid=lock_series_uid,
            mode="w",
        ) as cod:
            pass

    # we should be able to override the lock with a sufficiently small age threshold
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=lock_study_uid,
        series_uid=lock_series_uid,
        mode="w",
        empty_lock_override_age=0.00000001,
    ) as cod:
        pass
    # The lock should have been overridden and released
    assert not cod._locker.get_lock_blob().exists()
    # clean up any locks that might have been created
    delete_uploaded_blobs(gcs_client, [datastore_path])


def test_cannot_override_non_empty_lock(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
):
    """Test that we cannot override a non-empty lock"""
    instance = Instance(dicom_uri=local_instance_path)
    # append an instance to the cod object so it exists in the datastore
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=instance.study_uid(),
        series_uid=instance.series_uid(),
        mode="w",
    ) as cod:
        cod.append([instance])

    # create a non-empty hanging lock by causing a sync failure
    # use mode="a" so the lock snapshot includes the existing instances
    with pytest.raises(LockVerificationError):
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=instance.study_uid(),
            series_uid=instance.series_uid(),
            mode="a",
        ) as cod:
            # re-upload lock with current metadata (has instances) but new generation
            lock_blob = cod._locker.get_lock_blob()
            lock_blob.content_encoding = "gzip"
            lock_blob.upload_from_string(
                cod._metadata.to_gzipped_json(),
                content_type="application/json",
            )
    # assert lock exists
    assert cod._locker.get_lock_blob().exists()
    # assert lock is non empty
    assert len(SeriesMetadata.from_blob(cod._locker.get_lock_blob()).instances) > 0
    # we should not be able to override the lock, even with a sufficiently small age threshold
    with pytest.raises(LockAcquisitionError):
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=instance.study_uid(),
            series_uid=instance.series_uid(),
            mode="w",
            empty_lock_override_age=0.00000001,
        ) as cod:
            pass
