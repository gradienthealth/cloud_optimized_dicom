import os
import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.appender import CODAppender
from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.hints import Hints
from cloud_optimized_dicom.instance import Instance


class TestAppender(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.test_instance_uid = "1.2.276.0.50.192168001092.11156604.14547392.313"
        cls.local_instance_path = os.path.join(cls.test_data_dir, "valid.dcm")
        cls.client = storage.Client(
            project="gradient-pacs-siskin-172863",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-siskin-172863"
            ),
        )
        cls.datastore_path = "gs://siskin-172863-temp/cod_tests/dicomweb"

    def test_instance_too_large(self):
        instance = Instance(self.local_instance_path, hints=Hints(size=1000000))
        self.assertEqual(instance.size(trust_hints_if_available=True), 1000000)
        cod_object = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid="test_study_uid",
            series_uid="test_series_uid",
            lock=False,
        )
        cod_appender = CODAppender(cod_object)
        # test instance of acceptable size is not filtered
        filtered_instances = cod_appender._assert_not_too_large(
            instances=[instance], max_instance_size=1, max_series_size=100
        )
        self.assertEqual(len(filtered_instances), 1)
        # test instance of unacceptable size is filtered
        filtered_instances = cod_appender._assert_not_too_large(
            instances=[instance], max_instance_size=0.0001, max_series_size=100
        )
        self.assertEqual(len(filtered_instances), 0)
        self.assertEqual(len(cod_appender.append_result.errors), 1)
        # test series being too large raises an error
        with self.assertRaises(ValueError):
            cod_appender._assert_not_too_large(
                instances=[instance], max_instance_size=1, max_series_size=0.0001
            )

    def test_append(self):
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid="test_study_uid",
            series_uid="test_series_uid",
            lock=False,
        )
        instance = Instance(dicom_uri=self.local_instance_path)
        new, same, conflict, errors = cod_obj.append([instance], dirty=True)
        self.assertEqual(len(errors), 0)
        self.assertEqual(len(new), 1)
        self.assertEqual(len(same), 0)
        self.assertEqual(len(conflict), 0)
