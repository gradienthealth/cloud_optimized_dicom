import os
import unittest

from cloud_optimized_dicom.series_metadata import SeriesMetadata


class TestMetadataLoad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def test_metadata_load(self):
        with open(os.path.join(self.test_data_dir, "valid_metadata.json"), "rb") as f:
            metadata = SeriesMetadata.from_bytes(f.read())
        self.assertEqual(
            metadata.instances[
                "1.2.826.0.1.3680043.8.498.62425593669867971606161001484111987783"
            ].metadata["00080000"]["Value"],
            [612],
        )
        self.assertEqual(
            metadata.instances[
                "1.2.826.0.1.3680043.8.498.62425593669867971606161001484111987783"
            ].metadata["00080008"]["Value"],
            ["ORIGINAL", "PRIMARY", "LOCALIZER"],
        )
        self.assertEqual(
            metadata.instances[
                "1.2.826.0.1.3680043.8.498.62425593669867971606161001484111987783"
            ].metadata["00080016"]["Value"],
            ["1.2.840.10008.5.1.4.1.1.2"],
        )
