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

        # make sure all expected cod metadata is present
        self.assertListEqual(
            list(metadata.instances.keys()),
            [
                "1.2.826.0.1.3680043.8.498.62425593669867971606161001484111987783",
                "1.2.826.0.1.3680043.8.498.50975770268315387059815637280790177891",
            ],
        )
        self.assertEqual(
            metadata.study_uid,
            "some_study_uid",
        )
        self.assertEqual(
            metadata.series_uid,
            "some_series_uid",
        )

        # check some random metadata value for thoroughness
        self.assertEqual(
            metadata.instances[
                "1.2.826.0.1.3680043.8.498.62425593669867971606161001484111987783"
            ].metadata["00080000"]["Value"],
            [612],
        )

        # make sure thumbnail custom tags are present
        self.assertListEqual(
            list(metadata.custom_tags["thumbnail"].keys()),
            ["uri", "thumbnail_index_to_instance_frame", "instances", "version"],
        )
