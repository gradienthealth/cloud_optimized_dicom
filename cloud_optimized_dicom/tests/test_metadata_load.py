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
        self.assertEqual(
            metadata.study_uid,
            "some_study_uid",
        )
        self.assertEqual(
            metadata.series_uid,
            "some_series_uid",
        )
        self.assertListEqual(
            list(metadata.instances.keys()),
            ["instance_uid_1", "instance_uid_2"],
        )
        # check a specific instance for thoroughness
        loaded_instance = metadata.instances["instance_uid_1"]
        self.assertEqual(
            loaded_instance.dicom_uri,
            "gs://some-hospital-pacs/v1.0/dicomweb/studies/some_study_uid/series/some_series_uid.tar://instances/instance_uid_1.dcm",
        )
        self.assertEqual(
            loaded_instance._byte_offsets,
            (1536, 393554),
        )
        self.assertEqual(loaded_instance._crc32c, "MdpbMQ==")
        self.assertEqual(loaded_instance._size, 392018)
        self.assertEqual(loaded_instance._original_path, "gs://path/to/original.dcm")
        self.assertEqual(loaded_instance.dependencies, ["gs://path/to/original.dcm"])
        self.assertEqual(loaded_instance._diff_hash_dupe_paths, [])
        self.assertEqual(
            loaded_instance._modified_datetime, "2025-02-26T01:25:49.250660"
        )
        self.assertEqual(loaded_instance._custom_offset_tables, {})
        # check some random metadata value for thoroughness
        self.assertEqual(
            metadata.instances["instance_uid_1"].metadata["00080000"]["Value"],
            [612],
        )

        # make sure thumbnail custom tags are present
        self.assertListEqual(
            list(metadata.custom_tags["thumbnail"].keys()),
            ["uri", "thumbnail_index_to_instance_frame", "instances", "version"],
        )
