import os
import traceback

import pytest
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.errors import ErrorLogExistsError
from cloud_optimized_dicom.utils import delete_uploaded_blobs

ERROR_LOG_DATASTORE_BASE = "gs://siskin-172863-temp/cod_error_log_tests"
ERROR_LOG_STUDY_UID = "1.2.3.4.5.6.7.8.9.10"
ERROR_LOG_SERIES_UID = "1.2.3.4.5.6.7.8.9.10"

pytestmark = pytest.mark.skipif(
    "SKIP_NETWORK_TESTS" in os.environ, reason="network tests disabled"
)


@pytest.fixture
def error_log_datastore_path(gcs_client: storage.Client) -> str:
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    path = f"{ERROR_LOG_DATASTORE_BASE}/{worker}/dicomweb"
    delete_uploaded_blobs(gcs_client, [path])
    return path


def test_error_log_upload(gcs_client: storage.Client, error_log_datastore_path: str):
    try:
        with CODObject(
            datastore_path=error_log_datastore_path,
            client=gcs_client,
            study_uid=ERROR_LOG_STUDY_UID,
            series_uid=ERROR_LOG_SERIES_UID,
            mode="w",
        ) as cod_obj:
            # simulate doing something with the CODObjet and causing an error
            raise Exception("test error")
    except Exception:
        cod_obj.upload_error_log(traceback.format_exc())

    # error log should exist
    error_blob = storage.Blob.from_string(cod_obj.error_log_uri, client=gcs_client)
    assert error_blob.exists()
    # error log should contain the error message
    assert "test error" in error_blob.download_as_bytes().decode("utf-8")
    # lock should have been released (non-sync exception doesn't leave hanging lock)
    # the error log alone is sufficient to brick the COD
    assert not cod_obj._locker.get_lock_blob().exists()


def test_error_existence_bricks_cod_object_initialization(
    gcs_client: storage.Client, error_log_datastore_path: str
):
    """Test that the error log bricks CODObject initialization"""
    # Create the error log
    with CODObject(
        datastore_path=error_log_datastore_path,
        client=gcs_client,
        study_uid=ERROR_LOG_STUDY_UID,
        series_uid=ERROR_LOG_SERIES_UID,
        mode="w",
    ) as cod_obj:
        cod_obj.upload_error_log("test error")

    # Try to initialize the CODObject again and expect error
    with pytest.raises(ErrorLogExistsError):
        with CODObject(
            datastore_path=error_log_datastore_path,
            client=gcs_client,
            study_uid=ERROR_LOG_STUDY_UID,
            series_uid=ERROR_LOG_SERIES_UID,
            mode="w",
        ) as cod_obj:
            pass


def test_error_log_override(gcs_client: storage.Client, error_log_datastore_path: str):
    """Test that the error log can be overridden"""
    # Create the error log
    with CODObject(
        datastore_path=error_log_datastore_path,
        client=gcs_client,
        study_uid=ERROR_LOG_STUDY_UID,
        series_uid=ERROR_LOG_SERIES_UID,
        mode="w",
    ) as cod_obj:
        cod_obj.upload_error_log("test error")

    # override the error log
    with CODObject(
        datastore_path=error_log_datastore_path,
        client=gcs_client,
        study_uid=ERROR_LOG_STUDY_UID,
        series_uid=ERROR_LOG_SERIES_UID,
        mode="w",
        override_errors=True,
    ) as cod_obj:
        pass

    assert not storage.Blob.from_string(
        cod_obj.error_log_uri, client=gcs_client
    ).exists()
