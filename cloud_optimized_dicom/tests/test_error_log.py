import traceback

import pytest

from cloud_optimized_dicom.blob_store import BlobStore
from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.errors import ErrorLogExistsError
from cloud_optimized_dicom.utils import delete_uploaded_blobs

ERROR_LOG_DATASTORE_BASE = "gs://siskin-172863-temp/cod_error_log_tests"
ERROR_LOG_STUDY_UID = "1.2.3.4.5.6.7.8.9.10"
ERROR_LOG_SERIES_UID = "1.2.3.4.5.6.7.8.9.10"


@pytest.fixture
def error_log_datastore_path(blob_store: BlobStore, worker_namespace: str) -> str:
    path = f"{ERROR_LOG_DATASTORE_BASE}/{worker_namespace}/dicomweb"
    delete_uploaded_blobs(blob_store, [path])
    return path


def test_error_log_upload(blob_store: BlobStore, error_log_datastore_path: str):
    try:
        with CODObject(
            datastore_path=error_log_datastore_path,
            client=blob_store,
            study_uid=ERROR_LOG_STUDY_UID,
            series_uid=ERROR_LOG_SERIES_UID,
            mode="w",
        ) as cod_obj:
            # simulate doing something with the CODObjet and causing an error
            raise Exception("test error")
    except Exception:
        cod_obj.upload_error_log(traceback.format_exc())

    # error log should exist
    assert blob_store.exists(cod_obj.error_log_uri)
    # error log should contain the error message
    assert "test error" in blob_store.read_bytes(cod_obj.error_log_uri).decode("utf-8")
    # lock should have been released (non-sync exception doesn't leave hanging lock)
    # the error log alone is sufficient to brick the COD
    assert not cod_obj.blob_store.exists(cod_obj._locker.lock_uri)


def test_error_existence_bricks_cod_object_initialization(
    blob_store: BlobStore, error_log_datastore_path: str
):
    """Test that the error log bricks CODObject initialization"""
    # Create the error log
    with CODObject(
        datastore_path=error_log_datastore_path,
        client=blob_store,
        study_uid=ERROR_LOG_STUDY_UID,
        series_uid=ERROR_LOG_SERIES_UID,
        mode="w",
    ) as cod_obj:
        cod_obj.upload_error_log("test error")

    # Try to initialize the CODObject again and expect error
    with pytest.raises(ErrorLogExistsError):
        with CODObject(
            datastore_path=error_log_datastore_path,
            client=blob_store,
            study_uid=ERROR_LOG_STUDY_UID,
            series_uid=ERROR_LOG_SERIES_UID,
            mode="w",
        ) as cod_obj:
            pass


def test_error_log_override(blob_store: BlobStore, error_log_datastore_path: str):
    """Test that the error log can be overridden"""
    # Create the error log
    with CODObject(
        datastore_path=error_log_datastore_path,
        client=blob_store,
        study_uid=ERROR_LOG_STUDY_UID,
        series_uid=ERROR_LOG_SERIES_UID,
        mode="w",
    ) as cod_obj:
        cod_obj.upload_error_log("test error")

    # override the error log
    with CODObject(
        datastore_path=error_log_datastore_path,
        client=blob_store,
        study_uid=ERROR_LOG_STUDY_UID,
        series_uid=ERROR_LOG_SERIES_UID,
        mode="w",
        override_errors=True,
    ) as cod_obj:
        pass

    assert not blob_store.exists(cod_obj.error_log_uri)
