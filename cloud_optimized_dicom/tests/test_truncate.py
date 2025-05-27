import os
import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import delete_uploaded_blobs


class TestTruncate(unittest.TestCase):
    def test_truncate(self):
        """
        Test that a cod object can be successfully truncated.
        """


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
        cod_obj.append(instances=[instance1, instance2], dirty=True)
        cod_obj.remove(instances=[instance1], dirty=True)

    def test_remove_remote(self):
        """
        Test that an instance can be successfully removed from a remote cod object.
        """

    def test_remove_all(self):
        """
        Test handling of all instances being removed from a cod object.
        """

    def test_remove_nonexistent(self):
        """
        Test handling of removing a nonexistent instance from a cod object.
        """
