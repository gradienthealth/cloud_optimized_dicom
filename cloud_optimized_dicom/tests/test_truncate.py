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
        Test that a cod object can be successfully truncated.
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
            lock=False,
        )
        append_result = cod_obj.append(instances=[instance1], dirty=True)
        self.assertEqual(len(append_result.new), 1)
        truncate_result = cod_obj.truncate(instances=[instance2], dirty=True)
        self.assertEqual(len(truncate_result.new), 1)
        self.assertEqual(truncate_result.new[0], instance2)
        # cod object should ONLY contain the new instance
        self.assertEqual(
            list(cod_obj.get_metadata(dirty=True).instances.values()), [instance2]
        )
        with tarfile.open(cod_obj.tar_file_path, "r") as tar:
            self.assertEqual(len(tar.getmembers()), 1)
            self.assertEqual(
                tar.getmembers()[0].name, f"instances/{instance2.instance_uid()}.dcm"
            )

    def test_truncate_remote(self):
        """
        Test that a cod object can be successfully truncated from a remote cod object.
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
            lock=True,
        ) as cod_obj:
            append_result = cod_obj.append(instances=[instance1])
            self.assertEqual(len(append_result.new), 1)
            cod_obj.sync()

        cod_obj = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance1.study_uid(),
            series_uid=instance1.series_uid(),
            lock=False,
        )
        truncate_result = cod_obj.truncate(instances=[instance2], dirty=True)
        self.assertEqual(len(truncate_result.new), 1)
        self.assertEqual(truncate_result.new[0], instance2)
        # cod object should ONLY contain the new instance
        self.assertEqual(
            list(cod_obj.get_metadata(dirty=True).instances.values()), [instance2]
        )

    def test_truncate_preexisting(self):
        """
        Test that a cod object can be successfully truncated with preexisting instances.
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
            lock=False,
        )
        append_result = cod_obj.append(instances=[instance1, instance2], dirty=True)
        self.assertEqual(len(append_result.new), 2)
        truncate_result = cod_obj.truncate(instances=[instance2], dirty=True)
        self.assertEqual(len(truncate_result.new), 1)
        self.assertEqual(truncate_result.new[0], instance2)
        # cod object should ONLY contain the new instance
        self.assertEqual(
            list(cod_obj.get_metadata(dirty=True).instances.values()), [instance2]
        )


class TestRemove(unittest.TestCase):

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

    def test_remove(self):
        """
        Test that an instance can be successfully removed from a cod object.
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
            lock=False,
        )
        append_result = cod_obj.append(instances=[instance1, instance2], dirty=True)
        self.assertEqual(len(append_result.new), 2)

        remove_result = cod_obj.remove(instances=[instance1], dirty=True)
        # assert that there's one instance left (and its the one we didn't remove)
        self.assertEqual(len(remove_result.new), 1)
        self.assertEqual(remove_result.new[0], instance2)
        self.assertEqual(
            list(cod_obj.get_metadata(dirty=True).instances.values()), [instance2]
        )

    def test_remove_remote(self):
        """
        Test that an instance can be successfully removed from a remote cod object.
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
            lock=True,
        ) as cod_obj:
            append_result = cod_obj.append(instances=[instance1, instance2])
            self.assertEqual(len(append_result.new), 2)
            cod_obj.sync()

        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance1.study_uid(),
            series_uid=instance1.series_uid(),
            lock=True,
        ) as cod_obj:
            remove_result = cod_obj.remove(instances=[instance1], dirty=False)
            cod_obj.sync()

        cod_obj = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance1.study_uid(),
            series_uid=instance1.series_uid(),
            lock=False,
        )
        self.assertEqual(len(cod_obj.get_metadata(dirty=True).instances), 1)
        self.assertEqual(
            list(cod_obj.get_metadata(dirty=True).instances.values()), [instance2]
        )

    def test_remove_all_raises_error(self):
        """
        Test handling of all instances being removed from a cod object.
        """
        instance1 = Instance(
            dicom_uri=os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.22997958494980951977704130269567444795.dcm",
            )
        )
        cod_obj = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=instance1.study_uid(),
            series_uid=instance1.series_uid(),
            lock=False,
        )
        append_result = cod_obj.append(instances=[instance1], dirty=True)
        self.assertEqual(len(append_result.new), 1)
        with self.assertRaises(ValueError):
            cod_obj.remove(instances=[instance1], dirty=True)

    def test_remove_nonexistent(self):
        """
        Test handling of removing a nonexistent instance from a cod object.
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
            lock=False,
        )
        cod_obj.append(instances=[instance1], dirty=True)
        remove_result = cod_obj.remove(instances=[instance2], dirty=True)
        self.assertEqual(len(remove_result.new), 1)
        self.assertEqual(remove_result.new[0], instance1)
        self.assertEqual(
            list(cod_obj.get_metadata(dirty=True).instances.values()), [instance1]
        )
