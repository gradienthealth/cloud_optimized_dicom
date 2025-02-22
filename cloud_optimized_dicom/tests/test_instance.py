import os
import tarfile
import tempfile
import unittest

import pydicom

from cloud_optimized_dicom.instance import Instance


class TestInstance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.remote_dicom_uri = (
            "https://github.com/dangom/sample-dicom/raw/refs/heads/master/MR000000.dcm"
        )

    def test_remote_detection(self):
        self.assertTrue(Instance("s3://bucket/path/to/file.dcm").is_remote())
        self.assertTrue(Instance("gs://bucket/path/to/file.dcm").is_remote())
        self.assertTrue(Instance(self.remote_dicom_uri).is_remote())
        self.assertFalse(
            Instance(
                os.path.join(self.test_data_dir, "small_multiframe.dcm")
            ).is_remote()
        )

    def test_local_open(self):
        instance = Instance(os.path.join(self.test_data_dir, "small_multiframe.dcm"))
        with instance.open() as f:
            ds = pydicom.dcmread(f)
            self.assertEqual(ds.PatientName, "Rubo DEMO")

    def test_remote_open(self):
        instance = Instance(self.remote_dicom_uri)
        with instance.open() as f:
            ds = pydicom.dcmread(f)
            self.assertEqual(
                ds.SOPInstanceUID,
                "1.2.276.0.7230010.3.1.4.2927855660.2552.1497110443.461491",
            )

    def test_validate(self):
        instance = Instance(os.path.join(self.test_data_dir, "small_multiframe.dcm"))
        self.assertIsNone(instance._instance_uid)
        self.assertIsNone(instance._series_uid)
        self.assertIsNone(instance._study_uid)
        instance.validate()
        # after validation, the internal fields should be populated
        self.assertEqual(
            instance._instance_uid, "1.3.12.2.1107.5.4.3.284980.19951129.170916.11"
        )
        self.assertEqual(
            instance._series_uid, "1.3.12.2.1107.5.4.3.4975316777216.19951114.94101.17"
        )
        self.assertEqual(
            instance._study_uid, "1.3.12.2.1107.5.4.3.4975316777216.19951114.94101.16"
        )
        # getter methods should return the same values
        self.assertEqual(instance.instance_uid, instance._instance_uid)
        self.assertEqual(instance.series_uid, instance._series_uid)
        self.assertEqual(instance.study_uid, instance._study_uid)

    def test_append_to_series_tar(self):
        instance = Instance(os.path.join(self.test_data_dir, "small_multiframe.dcm"))
        with tempfile.NamedTemporaryFile(suffix=".tar") as tar_file:
            with tarfile.open(tar_file.name, "w") as tar:
                instance.append_to_series_tar(tar)
