import os
import unittest

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

    def test_instance_too_large(self):
        instance = Instance(self.local_instance_path, hints=Hints(size=1000000))
        self.assertEqual(instance.size(trust_hints_if_available=True), 1000000)
        cod_object = CODObject(
            datastore_path="test_datastore",
            client=None,
            study_uid="test_study_uid",
            series_uid="test_series_uid",
            lock=False,
        )
        cod_appender = CODAppender(cod_object)
        # test instance of acceptable size is not filtered
        filtered_instances, errors = cod_appender._assert_not_too_large(
            instances=[instance], max_instance_size=1, max_series_size=100
        )
        self.assertEqual(len(filtered_instances), 1)
        self.assertEqual(len(errors), 0)
        # test instance of unacceptable size is filtered
        filtered_instances, errors = cod_appender._assert_not_too_large(
            instances=[instance], max_instance_size=0.0001, max_series_size=100
        )
        self.assertEqual(len(filtered_instances), 0)
        self.assertEqual(len(errors), 1)
        # test series being too large raises an error
        with self.assertRaises(ValueError):
            cod_appender._assert_not_too_large(
                instances=[instance], max_instance_size=1, max_series_size=0.0001
            )

    def test_append(self):
        cod_object = CODObject(
            datastore_path="test_datastore",
            client=None,
            study_uid="test_study_uid",
            series_uid="test_series_uid",
            lock=False,
        )
        instance = Instance(self.local_instance_path)
        cod_object.append(instances=[instance], dirty=True)
        self.assertEqual(len(cod_object.get_metadata().instances), 1)
        self.assertEqual(
            cod_object.get_metadata().instances[instance.instance_uid], instance
        )
