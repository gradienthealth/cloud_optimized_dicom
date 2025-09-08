import os
import unittest

from cloud_optimized_dicom.errors import HintMismatchError
from cloud_optimized_dicom.hints import Hints
from cloud_optimized_dicom.instance import Instance


class TestHints(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def test_empty_hints(self):
        hints = Hints()
        instance = Instance(
            dicom_uri=os.path.join(self.test_data_dir, "monochrome2.dcm"), hints=hints
        )
        self.assertTrue(instance.validate())

    def test_good_hints(self):
        hints = Hints(
            instance_uid="1.2.276.0.50.192168001092.11156604.14547392.313",
            series_uid="1.2.276.0.50.192168001092.11156604.14547392.303",
            study_uid="1.2.276.0.50.192168001092.11156604.14547392.4",
            size=527800,
            crc32c="uEaR6w==",
        )
        instance = Instance(
            dicom_uri=os.path.join(self.test_data_dir, "monochrome2.dcm"), hints=hints
        )
        self.assertTrue(instance.validate())

    def test_bad_uid(self):
        hints = Hints(instance_uid="BAD_UID")
        instance = Instance(
            dicom_uri=os.path.join(self.test_data_dir, "monochrome2.dcm"), hints=hints
        )
        with self.assertRaises(HintMismatchError):
            instance.validate()

    def test_bad_size(self):
        hints = Hints(size=1000)
        instance = Instance(
            dicom_uri=os.path.join(self.test_data_dir, "monochrome2.dcm"), hints=hints
        )
        with self.assertRaises(HintMismatchError):
            instance.validate()

    def test_bad_crc32c(self):
        hints = Hints(crc32c="BAD_CRC32C")
        instance = Instance(
            dicom_uri=os.path.join(self.test_data_dir, "monochrome2.dcm"), hints=hints
        )
        with self.assertRaises(HintMismatchError):
            instance.validate()
