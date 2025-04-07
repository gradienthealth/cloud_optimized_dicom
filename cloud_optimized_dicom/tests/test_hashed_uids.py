import unittest

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.hints import Hints
from cloud_optimized_dicom.instance import Instance


def example_hash_function(uid: str) -> str:
    """
    Example hash function that adds 1 to the last part of the uid (i.e 1.2.3.4 becomes 1.2.3.5)
    """
    split_uid = uid.split(".")
    last_part = split_uid[-1]
    new_last_part = str(int(last_part) + 1)
    split_uid[-1] = new_last_part
    return ".".join(split_uid)


class TestDeid(unittest.TestCase):
    def test_hash_func_provided(self):
        """Test the cod_object hash_func_provided property"""
        instance = Instance(
            dicom_uri="gs://bucket/path/to/file.dcm",
            hints=Hints(
                instance_uid="1.2.3.4",
                series_uid="1.2.3.4",
                study_uid="1.2.3.4",
            ),
            uid_hash_func=example_hash_function,
        )
        self.assertTrue(instance.uid_hash_func)
        self.assertEqual(
            instance.instance_uid(trust_hints_if_available=True), "1.2.3.4"
        )
        self.assertEqual(
            instance.hashed_instance_uid(trust_hints_if_available=True), "1.2.3.5"
        )
        self.assertEqual(
            instance.hashed_series_uid(trust_hints_if_available=True), "1.2.3.5"
        )
        self.assertEqual(
            instance.hashed_study_uid(trust_hints_if_available=True), "1.2.3.5"
        )

    def test_hash_func_not_provided(self):
        """Test that trying to get a hashed uid without a hash function raises an error"""
        instance = Instance(
            dicom_uri="gs://bucket/path/to/file.dcm",
            hints=Hints(instance_uid="1.2.3.4"),
        )
        self.assertFalse(instance.uid_hash_func)
        with self.assertRaises(ValueError):
            instance.hashed_instance_uid(trust_hints_if_available=True)
        with self.assertRaises(ValueError):
            instance.hashed_series_uid(trust_hints_if_available=True)
        with self.assertRaises(ValueError):
            instance.hashed_study_uid(trust_hints_if_available=True)
