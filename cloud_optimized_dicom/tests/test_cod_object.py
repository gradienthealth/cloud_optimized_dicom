import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject


class TestCODObject(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = storage.Client(
            project="gradient-pacs-siskin-172863",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-siskin-172863"
            ),
        )
        cls.datastore_path = "gs://siskin-172863-temp/cod_tests/dicomweb"

    def test_properties(self):
        """Test tar_uri, metadata_uri, index_uri, and __str__"""
        cod_object = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid="1.2.3.4.5.6.7.8.9.0",
            series_uid="1.2.3.4.5.6.7.8.9.0",
            lock=False,
        )
        self.assertEqual(cod_object.datastore_path, self.datastore_path)
        self.assertEqual(
            cod_object.tar_uri,
            f"{self.datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0.tar",
        )
        self.assertEqual(
            cod_object.metadata_uri,
            f"{self.datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0/metadata.json",
        )
        self.assertEqual(
            cod_object.index_uri,
            f"{self.datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0/index.sqlite",
        )
        self.assertEqual(
            str(cod_object),
            f"CODObject({self.datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0)",
        )

    def test_validate_uids(self):
        """Test that COD instantiation fails if UIDs are not valid"""
        with self.assertRaises(AssertionError):
            CODObject(
                datastore_path=self.datastore_path,
                client=self.client,
                study_uid="1.2.3.4.5",
                series_uid="1.2.3.4.5",
                lock=False,
            )
