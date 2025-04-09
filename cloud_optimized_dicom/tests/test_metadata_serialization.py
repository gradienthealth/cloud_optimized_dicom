import json
import os
import unittest

from cloud_optimized_dicom.series_metadata import SeriesMetadata


class TestMetadataSerialization(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def _assert_load_success(self, metadata: SeriesMetadata):
        # make sure all expected cod metadata is present
        self.assertEqual(metadata.study_uid, "some_study_uid")
        self.assertEqual(metadata.series_uid, "some_series_uid")
        self.assertListEqual(
            list(metadata.instances.keys()), ["instance_uid_1", "instance_uid_2"]
        )
        # check a specific instance for thoroughness
        loaded_instance = metadata.instances["instance_uid_1"]
        self.assertEqual(
            loaded_instance.dicom_uri,
            "gs://some-hospital-pacs/v1.0/dicomweb/studies/some_study_uid/series/some_series_uid.tar://instances/instance_uid_1.dcm",
        )
        self.assertEqual(loaded_instance._byte_offsets, (1536, 393554))
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
            metadata.instances["instance_uid_1"].metadata["00080000"]["Value"], [612]
        )

        # make sure thumbnail custom tags are present
        self.assertListEqual(list(metadata.custom_tags.keys()), ["thumbnail"])
        self.assertListEqual(
            list(metadata.custom_tags["thumbnail"].keys()),
            ["uri", "thumbnail_index_to_instance_frame", "instances", "version"],
        )

    def _assert_save_success(self, raw_dict: dict, saved_dict: dict, is_deid: bool):
        # top level key assertion first for ease of debugging
        self.assertEqual(raw_dict.keys(), saved_dict.keys())
        # uids must be equal
        if is_deid:
            self.assertEqual(
                raw_dict.pop("deid_study_uid", None),
                saved_dict.pop("deid_study_uid", None),
            )
            self.assertEqual(
                raw_dict.pop("deid_series_uid", None),
                saved_dict.pop("deid_series_uid", None),
            )
        else:
            self.assertEqual(
                raw_dict.pop("study_uid", None), saved_dict.pop("study_uid", None)
            )
            self.assertEqual(
                raw_dict.pop("series_uid", None), saved_dict.pop("series_uid", None)
            )
        # pop off cod dict for comparison later (it is the most complex)
        raw_cod = raw_dict.pop("cod")
        saved_cod = saved_dict.pop("cod")
        # check remaining dicts (custom tags) are equal
        self.assertDictEqual(raw_dict, saved_dict)
        # now do cod dict comparison
        self.assertEqual(raw_cod.keys(), saved_cod.keys())
        for instance_uid in raw_cod["instances"].keys():
            raw_instance = raw_cod["instances"][instance_uid]
            saved_instance = saved_cod["instances"][instance_uid]
            self.assertEqual(raw_instance.keys(), saved_instance.keys())
            for key in raw_instance.keys():
                self.assertEqual(raw_instance[key], saved_instance[key])

    def test_metadata_load(self):
        with open(os.path.join(self.test_data_dir, "valid_metadata.json"), "rb") as f:
            metadata = SeriesMetadata.from_bytes(f.read())
        self._assert_load_success(metadata)

    def test_deid_metadata_load(self):
        with open(
            os.path.join(self.test_data_dir, "valid_deid_metadata.json"), "rb"
        ) as f:
            metadata = SeriesMetadata.from_bytes(f.read())
        self._assert_load_success(metadata)

    def test_metadata_save(self):
        # first load the metadata
        with open(os.path.join(self.test_data_dir, "valid_metadata.json"), "rb") as f:
            raw_bytes = f.read()
            # save raw dict for comparison
            raw_dict = json.loads(raw_bytes)
            saved_dict = SeriesMetadata.from_bytes(raw_bytes).to_dict()
        self._assert_save_success(raw_dict, saved_dict, is_deid=False)

    def test_deid_metadata_save(self):
        # first load the metadata
        with open(
            os.path.join(self.test_data_dir, "valid_deid_metadata.json"), "rb"
        ) as f:
            raw_bytes = f.read()
            # save raw dict for comparison
            raw_dict = json.loads(raw_bytes)
            saved_dict = SeriesMetadata.from_bytes(raw_bytes).to_dict()
        self._assert_save_success(raw_dict, saved_dict, is_deid=True)
