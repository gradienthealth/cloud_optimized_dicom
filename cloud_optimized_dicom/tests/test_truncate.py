import logging
import os
import tarfile
import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import delete_uploaded_blobs


class TestTruncate(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.client = storage.Client(
            project="gradient-pacs-siskin-172863",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-siskin-172863"
            ),
        )
        cls.datastore_path = "gs://siskin-172863-temp/cod_tests/dicomweb"
        logging.basicConfig(level=logging.INFO)

    def setUp(self):
        # ensure clean test directory prior to test start
        delete_uploaded_blobs(self.client, [self.datastore_path])

    def test_truncate(self):
        """
        Test that a cod object can be successfully truncated using mode="w" + append().
        """
        instance1 = Instance(
            dicom_uri=os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.22997958494980951977704130269567444795.dcm",
            )
        )
        instance2 = Instance(
            dicom_uri=os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.28109707839310833322020505651875585013.dcm",
            )
        )
        cod_obj = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance1.study_uid(),
            series_uid=instance1.series_uid(),
            mode="w",
            sync_on_exit=False,
        )
        append_result = cod_obj.append(instances=[instance1])
        self.assertEqual(len(append_result.new), 1)
        # Truncate by wiping and rebuilding with mode="w" + append()
        cod_obj._wipe_local()
        truncate_result = cod_obj.append(instances=[instance2])
        self.assertEqual(len(truncate_result.new), 1)
        self.assertEqual(truncate_result.new[0], instance2)
        # cod object should ONLY contain the new instance
        self.assertEqual(list(cod_obj.get_metadata().instances.values()), [instance2])
        with tarfile.open(cod_obj.tar_file_path, "r") as tar:
            self.assertEqual(len(tar.getmembers()), 1)
            self.assertEqual(
                tar.getmembers()[0].name, f"instances/{instance2.instance_uid()}.dcm"
            )

    def test_truncate_remote(self):
        """
        Test that a cod object can be successfully truncated from a remote cod object using mode="w" + append().
        """
        instance1 = Instance(
            dicom_uri=os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.22997958494980951977704130269567444795.dcm",
            )
        )
        instance2 = Instance(
            dicom_uri=os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.28109707839310833322020505651875585013.dcm",
            )
        )
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance1.study_uid(),
            series_uid=instance1.series_uid(),
            mode="w",
        ) as cod_obj:
            append_result = cod_obj.append(instances=[instance1])
            self.assertEqual(len(append_result.new), 1)
            # sync happens automatically on context exit

        # Truncate by creating new CODObject with mode="w" and appending desired instances
        # mode="w" will overwrite the remote tar/metadata on sync
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance1.study_uid(),
            series_uid=instance1.series_uid(),
            mode="w",
        ) as cod_obj:
            truncate_result = cod_obj.append(instances=[instance2])
            self.assertEqual(len(truncate_result.new), 1)
            self.assertEqual(truncate_result.new[0], instance2)
            # cod object should ONLY contain the new instance
            self.assertEqual(
                list(cod_obj.get_metadata().instances.values()), [instance2]
            )

    def test_truncate_preexisting(self):
        """
        Test that a cod object can be successfully truncated with preexisting instances using mode="w" + append().
        """
        instance1 = Instance(
            dicom_uri=os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.22997958494980951977704130269567444795.dcm",
            )
        )
        instance2 = Instance(
            dicom_uri=os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.28109707839310833322020505651875585013.dcm",
            )
        )
        cod_obj = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance1.study_uid(),
            series_uid=instance1.series_uid(),
            mode="w",
            sync_on_exit=False,
        )
        append_result = cod_obj.append(instances=[instance1, instance2])
        self.assertEqual(len(append_result.new), 2)
        # Truncate by wiping and rebuilding with mode="w" + append()
        # Create a fresh instance2 from the original file path since the old one
        # now points to the tar location which will be deleted by _wipe_local()
        cod_obj._wipe_local()
        instance2_fresh = Instance(
            dicom_uri=os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.28109707839310833322020505651875585013.dcm",
            )
        )
        truncate_result = cod_obj.append(instances=[instance2_fresh])
        self.assertEqual(len(truncate_result.new), 1)
        self.assertEqual(truncate_result.new[0], instance2_fresh)
        # cod object should ONLY contain the new instance
        self.assertEqual(
            list(cod_obj.get_metadata().instances.values()), [instance2_fresh]
        )
