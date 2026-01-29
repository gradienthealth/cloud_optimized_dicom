import logging
import os
import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.errors import LockAcquisitionError, LockVerificationError
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.utils import delete_uploaded_blobs


class TestLocking(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)
        logging.getLogger().setLevel(logging.INFO)
        cls.client = storage.Client(
            project="gradient-pacs-siskin-172863",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-siskin-172863"
            ),
        )
        cls.datastore_path = "gs://siskin-172863-temp/cod_tests/dicomweb"
        cls.study_uid = "1.2.3.4.5.6.7.8.9.10"
        cls.series_uid = "1.2.3.4.5.6.7.8.9.10"
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.local_instance_path = os.path.join(cls.test_data_dir, "monochrome2.dcm")
        delete_uploaded_blobs(cls.client, [cls.datastore_path])

    def test_mode_unspecified(self):
        """Test that not specifying mode raises an error"""
        with self.assertRaises(ValueError):
            CODObject(
                datastore_path=self.datastore_path,
                client=self.client,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
            )

    def test_lock_immutability(self):
        """Test that the lock flag is immutable"""
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="r",
        ) as cod:
            with self.assertRaises(AttributeError):
                cod.lock = True

    def test_lock_uniqueness(self):
        """Test that you cannot have two CODObjects with the same lock"""
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="w",
        ) as cod1:
            with self.assertRaises(LockAcquisitionError):
                with CODObject(
                    client=self.client,
                    datastore_path=self.datastore_path,
                    study_uid=self.study_uid,
                    series_uid=self.series_uid,
                    mode="w",
                ) as cod2:
                    pass

    def test_read_mode(self):
        """Test that you can read metadata in read mode"""
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="r",
        ) as cod:
            metadata = cod.get_metadata()
            self.assertEqual(metadata.study_uid, self.study_uid)
            self.assertEqual(metadata.series_uid, self.series_uid)

    def test_concurrent_read(self):
        """Test that you can read while another cod has a lock"""
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="w",
        ) as cod1:
            locked_metadata = cod1.get_metadata()
            with CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                mode="r",
            ) as cod2:
                read_metadata = cod2.get_metadata()
                self.assertEqual(read_metadata.study_uid, self.study_uid)
                self.assertEqual(read_metadata.series_uid, self.series_uid)
                self.assertEqual(locked_metadata.study_uid, self.study_uid)
                self.assertEqual(locked_metadata.series_uid, self.series_uid)

    def test_read_mode_allows_reads(self):
        """Test that mode='r' allows read operations without errors"""
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="r",
        ) as cod:
            # Read operations should work without any dirty parameter
            metadata = cod.get_metadata()
            self.assertEqual(metadata.study_uid, self.study_uid)
            self.assertEqual(metadata.series_uid, self.series_uid)

    def test_deprecation_warning_for_dirty_param(self):
        """Test that using dirty parameter emits a deprecation warning"""
        import warnings

        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="r",
        ) as cod:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                cod.get_metadata(dirty=True)
                # Check that a deprecation warning was issued for the dirty parameter
                self.assertTrue(
                    any("dirty" in str(warning.message) for warning in w),
                    "Expected deprecation warning for 'dirty' parameter",
                )

    def test_lock_gone_on_cleanup(self):
        """Test that we get an error if the lock disappears while the COD is active"""
        with self.assertRaises(LockVerificationError):
            with CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                mode="w",
            ) as cod:
                cod._locker.get_lock_blob().delete()
            # when the with block exits, cod will attempt to release the lock and will find it missing

    @unittest.skip("skipping until we have a sync method")
    def test_lock_gone_on_sync(self):
        """Test that we get an error if the lock disappears while the COD is syncing"""
        # don't use a with block so we can delete the lock blob manually
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="w",
        )
        cod_obj.get_lock_blob().delete()
        with self.assertRaises(LockVerificationError):
            cod_obj._sync()

    def test_lock_changes(self):
        """Test that we get an error if the lock changes while the COD is active"""
        with self.assertRaises(LockVerificationError):
            with CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                mode="w",
            ) as cod:
                # simulate some other cod somehow stealing the lock
                cod._locker.get_lock_blob().upload_from_string(
                    "", content_type="application/octet-stream"
                )
            # when the with block exits, cod will attempt to release the lock and will find it changed
        # cod will have failed to delete the lock since it assumes it belongs to another cod, so we need to clean up after ourselves
        delete_uploaded_blobs(self.client, [self.datastore_path])

    def test_lock_stolen_during_metadata_fetch(self):
        """Test that we get an error if another process creates the lock while we're fetching metadata"""

        original_get_metadata = CODObject._get_metadata

        def mock_get_metadata(self: CODObject, create_if_missing=True):
            # First get the metadata normally
            result = original_get_metadata(self, create_if_missing=create_if_missing)
            # Then simulate another process creating the lock file
            self._locker.get_lock_blob().upload_from_string(
                "competing lock", content_type="application/json", if_generation_match=0
            )
            return result

        # Patch the _get_metadata method temporarily for this test.
        # Now in acquire_lock, it will now get_metadata, upload a lock, and then attempt to upload the lock again
        # We expect this to raise our assertion error about a stolen lock
        CODObject._get_metadata = mock_get_metadata

        try:
            with self.assertRaisesRegex(
                LockAcquisitionError,
                "COD:LOCK:ACQUISITION_FAILED:STOLEN_DURING_METADATA_FETCH",
            ):
                CODObject(
                    client=self.client,
                    datastore_path=self.datastore_path,
                    study_uid=self.study_uid,
                    series_uid=self.series_uid,
                    mode="w",
                )
        finally:
            # Restore the original method
            CODObject._get_metadata = original_get_metadata
            # Clean up any locks that might have been created
            delete_uploaded_blobs(self.client, [self.datastore_path])

    def test_lock_persists_after_exception(self):
        """Test that the lock persists after an exception is raised"""
        with self.assertRaises(ValueError):
            with CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                mode="w",
            ) as cod:
                raise ValueError("test")
        # The lock should still exist
        self.assertTrue(cod._locker.get_lock_blob().exists())
        # Clean up any locks that might have been created
        delete_uploaded_blobs(self.client, [self.datastore_path])

    def test_override_stale_lock(self):
        """Test that we can override a stale lock"""
        # leave a hanging lock
        with self.assertRaises(ValueError):
            with CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                mode="w",
            ) as cod:
                raise ValueError("test")
        # because there's a hanging lock, we should get an error
        with self.assertRaises(LockAcquisitionError):
            with CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                mode="w",
            ) as cod:
                pass

        # we should be able to override the lock with a sufficiently small age threshold
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="w",
            empty_lock_override_age=0.00000001,
        ) as cod:
            pass
        # The lock should have been overridden
        self.assertFalse(cod._locker.get_lock_blob().exists())
        # clean up any locks that might have been created
        delete_uploaded_blobs(self.client, [self.datastore_path])

    def test_cannot_override_non_empty_lock(self):
        """Test that we cannot override a non-empty lock"""
        instance = Instance(dicom_uri=self.local_instance_path)
        # append an instance to the cod object
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=instance.study_uid(),
            series_uid=instance.series_uid(),
            mode="w",
        ) as cod:
            cod.append([instance])
            cod._sync()

        # simulate non empty hanging lock
        with self.assertRaises(ValueError):
            with CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=instance.study_uid(),
                series_uid=instance.series_uid(),
                mode="w",
            ) as cod:
                raise ValueError("simulated error causing lock to hang")
        # assert lock exists
        self.assertTrue(cod._locker.get_lock_blob().exists())
        # assert lock is non empty
        self.assertGreater(
            len(SeriesMetadata.from_blob(cod._locker.get_lock_blob()).instances), 0
        )
        # we should not be able to override the lock, even with a sufficiently small age threshold
        with self.assertRaises(LockAcquisitionError):
            with CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=instance.study_uid(),
                series_uid=instance.series_uid(),
                mode="w",
                empty_lock_override_age=0.00000001,
            ) as cod:
                pass
