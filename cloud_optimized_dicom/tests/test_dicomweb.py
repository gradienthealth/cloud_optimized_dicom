import os
import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.dicomweb import DicomwebRequest, is_valid_uid


class TestDicomweb(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.datastore_path = "gs://siskin-172863-pacs/v1.0/dicomweb"
        cls.client = storage.Client(
            project="gradient-pacs-siskin-172863",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-siskin-172863"
            ),
        )

    def test_get_study(self):
        """
        Test that study existence can be queried via dicomweb standard
        """
        study_uri = os.path.join(
            self.datastore_path,
            "studies",
            "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
        )
        request = f"GET {study_uri}"
        result = DicomwebRequest.from_request(request).handle(self.client)
        # some basic checks to make sure the result is valid
        self.assertIn(
            "1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150", result
        )
        self.assertIn(
            "instances",
            result["1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150"],
        )

    def test_get_series(self):
        """
        Test that series existence can be queried via dicomweb standard
        """
        series_uri = os.path.join(
            self.datastore_path,
            "studies",
            "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
            "series",
            "1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150",
            "metadata",
        )
        request = f"GET {series_uri}"
        result = DicomwebRequest.from_request(request).handle(self.client)
        # we expect a list of instance metadata dictionaries
        self.assertIsInstance(result, list)
        # there happen to be 82 instances in this series
        self.assertEqual(len(result), 82)
        # check something in each instance (e.g. series uid)
        series_uid = result[0]["0020000D"]["Value"][0]
        self.assertTrue(is_valid_uid(series_uid))
        for instance in result:
            self.assertEqual(instance["0020000D"]["Value"][0], series_uid)

    def test_get_instance(self):
        instance_uri = os.path.join(
            self.datastore_path,
            "studies",
            "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
            "series",
            "1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150",
            "instances",
            "1.2.826.0.1.3680043.8.498.10368404844741579486264078308290534273",
            "metadata",
        )
        request = f"GET {instance_uri}"
        result = DicomwebRequest.from_request(request).handle(self.client)
        # we expect a dictionary of metadata
        self.assertIsInstance(result, dict)
        # check something in the metadata (e.g. series uid)
        series_uid = result["0020000D"]["Value"][0]
        self.assertTrue(is_valid_uid(series_uid))


if __name__ == "__main__":
    unittest.main()
