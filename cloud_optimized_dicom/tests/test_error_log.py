import os
import traceback
import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.errors import ErrorLogExistsError
from cloud_optimized_dicom.utils import delete_uploaded_blobs


@unittest.skipIf("SKIP_NETWORK_TESTS" in os.environ, reason="network tests disabled")
class TestErrorLog(unittest.TestCase):
    # python -m unittest components.cloud_optimized_dicom.tests.test_error_log.TestErrorLog

    @classmethod
    def setUpClass(cls):
        cls.client = storage.Client(
            project="gradient-pacs-siskin-172863",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-siskin-172863"
            ),
        )
        cls.datastore_path = "gs://siskin-172863-temp/cod_error_log_tests/dicomweb"
        cls.study_uid = "1.2.3.4.5.6.7.8.9.10"
        cls.series_uid = "1.2.3.4.5.6.7.8.9.10"

    def setUp(self):
        delete_uploaded_blobs(self.client, [self.datastore_path])

    def test_error_log_upload(self):
        try:
            with CODObject(
                datastore_path=self.datastore_path,
                client=self.client,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                lock=True,
            ) as cod_obj:
                # simulate doing something with the CODObjet and causing an error
                raise Exception("test error")
        except Exception:
            cod_obj.upload_error_log(traceback.format_exc())

        # error log should exist
        error_blob = storage.Blob.from_string(cod_obj.error_log_uri, client=self.client)
        self.assertTrue(error_blob.exists())
        # error log should contain the error message
        self.assertIn("test error", error_blob.download_as_bytes().decode("utf-8"))
        # lock should have been left hanging
        self.assertTrue(cod_obj._locker.get_lock_blob().exists())

    def test_error_existence_bricks_cod_object_initialization(self):
        """Test that the error log bricks CODObject initialization"""
        # Create the error log
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            lock=True,
        ) as cod_obj:
            cod_obj.upload_error_log("test error")

        # Try to initialize the CODObject again and expect error
        with self.assertRaises(ErrorLogExistsError):
            with CODObject(
                datastore_path=self.datastore_path,
                client=self.client,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                lock=True,
            ) as cod_obj:
                pass

    def test_error_log_override(self):
        """Test that the error log can be overridden"""
        # Create the error log
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            lock=True,
        ) as cod_obj:
            cod_obj.upload_error_log("test error")

        # override the error log
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            lock=True,
            override_errors=True,
        ) as cod_obj:
            pass

        self.assertFalse(
            storage.Blob.from_string(cod_obj.error_log_uri, client=self.client).exists()
        )


if __name__ == "__main__":
    unittest.main()
