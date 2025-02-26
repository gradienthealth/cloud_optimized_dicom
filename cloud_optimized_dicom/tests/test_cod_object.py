import unittest

from cloud_optimized_dicom.cod_object import CODObject


class TestCODObject(unittest.TestCase):
    def test_properties(self):
        """Test tar_uri, metadata_uri, index_uri, and __str__"""
        cod_object = CODObject(
            datastore_path="gs://my-bucket/my-datastore",
            client=None,
            study_uid="1.2.3.4.5.6.7.8.9.0",
            series_uid="1.2.3.4.5.6.7.8.9.0",
        )
        self.assertEqual(cod_object.datastore_path, "gs://my-bucket/my-datastore")
        self.assertEqual(
            cod_object.tar_uri,
            "gs://my-bucket/my-datastore/1.2.3.4.5.6.7.8.9.0/1.2.3.4.5.6.7.8.9.0.tar",
        )
        self.assertEqual(
            cod_object.metadata_uri,
            "gs://my-bucket/my-datastore/1.2.3.4.5.6.7.8.9.0/1.2.3.4.5.6.7.8.9.0/metadata.json",
        )
        self.assertEqual(
            cod_object.index_uri,
            "gs://my-bucket/my-datastore/1.2.3.4.5.6.7.8.9.0/1.2.3.4.5.6.7.8.9.0/index.sqlite",
        )
        self.assertEqual(
            str(cod_object),
            "CODObject(gs://my-bucket/my-datastore/1.2.3.4.5.6.7.8.9.0/1.2.3.4.5.6.7.8.9.0)",
        )

    def test_lock_immutability(self):
        """Test that lock is read-only"""
        cod_object = CODObject(
            datastore_path="gs://my-bucket/my-datastore",
            client=None,
            study_uid="1.2.3.4.5.6.7.8.9.0",
            series_uid="1.2.3.4.5.6.7.8.9.0",
            lock=False,
        )
        with self.assertRaises(AttributeError):
            cod_object.lock = True

    def test_validate_uids(self):
        """Test that validate_uids raises an error if the UIDs are not valid DICOM UIDs"""
        with self.assertRaises(AssertionError):
            CODObject(
                datastore_path="gs://my-bucket/my-datastore",
                client=None,
                study_uid="1.2.3.4.5",
                series_uid="1.2.3.4.5",
            )
