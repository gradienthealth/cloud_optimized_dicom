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
        cls.remote_dicom_uri = "https://code.oak-tree.tech/oak-tree/medical-imaging/dcmjs/-/raw/master/test/sample-dicom.dcm?ref_type=heads&inline=false"
        cls.test_instance_uid = "1.2.276.0.50.192168001092.11156604.14547392.313"
        cls.local_instance_path = os.path.join(cls.test_data_dir, "valid.dcm")

    def test_remote_detection(self):
        self.assertTrue(Instance("s3://bucket/path/to/file.dcm").is_remote())
        self.assertTrue(Instance("gs://bucket/path/to/file.dcm").is_remote())
        self.assertTrue(Instance(self.remote_dicom_uri).is_remote())
        self.assertFalse(Instance(self.local_instance_path).is_remote())

    def test_local_open(self):
        instance = Instance(self.local_instance_path)
        with instance.open() as f:
            ds = pydicom.dcmread(f)
            self.assertEqual(ds.SOPInstanceUID, self.test_instance_uid)

    def test_remote_open(self):
        instance = Instance(self.remote_dicom_uri)
        with instance.open() as f:
            ds = pydicom.dcmread(f)
            self.assertEqual(ds.SOPInstanceUID, self.test_instance_uid)

    def test_validate(self):
        instance = Instance(self.local_instance_path)
        self.assertIsNone(instance._instance_uid)
        self.assertIsNone(instance._series_uid)
        self.assertIsNone(instance._study_uid)
        instance.validate()
        # after validation, the internal fields should be populated
        self.assertEqual(instance._instance_uid, self.test_instance_uid)
        self.assertEqual(
            instance._series_uid, "1.2.276.0.50.192168001092.11156604.14547392.303"
        )
        self.assertEqual(
            instance._study_uid, "1.2.276.0.50.192168001092.11156604.14547392.4"
        )
        # getter methods should return the same values
        self.assertEqual(instance.instance_uid(), instance._instance_uid)
        self.assertEqual(instance.series_uid, instance._series_uid)
        self.assertEqual(instance.study_uid, instance._study_uid)

    def test_append_to_series_tar(self):
        instance = Instance(self.local_instance_path)
        with tempfile.TemporaryDirectory() as temp_dir:
            tar_file = os.path.join(temp_dir, "series.tar")
            with tarfile.open(tar_file, "w") as tar:
                pass
            with tarfile.open(tar_file, "a") as tar:
                instance.append_to_series_tar(tar)
            with tarfile.open(tar_file) as tar:
                self.assertEqual(len(tar.getnames()), 1)
                self.assertEqual(
                    tar.getnames()[0], f"instances/{self.test_instance_uid}.dcm"
                )
                self.assertEqual(
                    tar.getmember(f"instances/{self.test_instance_uid}.dcm").size,
                    instance.size(),
                )

    def test_extract_metadata(self):
        instance = Instance(self.local_instance_path)
        self.assertIsNone(instance._metadata)
        self.assertIsNone(instance._custom_offset_tables)
        instance.extract_metadata(
            output_uri="gs://some_series.tar://instances/some_instance.dcm"
        )
        self.assertEqual(
            instance.metadata["00080018"]["Value"][0], self.test_instance_uid
        )
        self.assertEqual(instance._custom_offset_tables, {})

    def test_delete_local_dependencies(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.assertTrue(os.path.exists(temp_file.name))
        instance = Instance(
            dicom_uri=self.local_instance_path, dependencies=[temp_file.name]
        )
        instance.delete_dependencies()
        self.assertFalse(os.path.exists(temp_file.name))

    def test_delete_remote_dependencies(self):
        # TODO: implement
        pass
