import os
import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.dicomweb import handle_dicomweb_request


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

    def test_series_existence(self):
        """
        Test that series existence can be queried via dicomweb standard
        """
        series_uri = os.path.join(
            self.datastore_path,
            "studies",
            "1.2.826.0.1.3680043.8.498.18783474219392509401504861043428417882",
            "series",
            "1.2.826.0.1.3680043.8.498.89840699185761593370876698622882853150",
        )
        request = f"GET {series_uri}"
        handle_dicomweb_request(request)


if __name__ == "__main__":
    unittest.main()
