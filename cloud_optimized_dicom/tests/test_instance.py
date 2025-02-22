import unittest

from cloud_optimized_dicom.instance import Instance


class TestInstance(unittest.TestCase):
    def test_is_remote(self):
        instance = Instance("s3://bucket/path/to/file.dcm")
        self.assertTrue(instance.is_remote())
