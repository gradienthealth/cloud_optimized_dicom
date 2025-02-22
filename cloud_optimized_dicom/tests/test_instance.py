import os
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
