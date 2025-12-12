import os
import tarfile
import tempfile
import unittest

import pydicom3

from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import is_remote


class TestInstance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.remote_dicom_uri = "https://code.oak-tree.tech/oak-tree/medical-imaging/dcmjs/-/raw/master/test/sample-dicom.dcm?ref_type=heads&inline=false"
        cls.test_instance_uid = "1.2.276.0.50.192168001092.11156604.14547392.313"
        cls.local_instance_path = os.path.join(cls.test_data_dir, "monochrome2.dcm")

    def test_remote_detection(self):
        self.assertTrue(is_remote("s3://bucket/path/to/file.dcm"))
        self.assertTrue(is_remote("gs://bucket/path/to/file.dcm"))
        self.assertTrue(is_remote(self.remote_dicom_uri))
        self.assertFalse(is_remote(self.local_instance_path))

    def test_local_open(self):
        instance = Instance(self.local_instance_path)
        with instance.open() as f:
            ds = pydicom3.dcmread(f)
            self.assertEqual(ds.SOPInstanceUID, self.test_instance_uid)

    def test_remote_open(self):
        instance = Instance(self.remote_dicom_uri)
        with instance.open() as f:
            ds = pydicom3.dcmread(f)
            self.assertEqual(ds.SOPInstanceUID, self.test_instance_uid)

    def test_remote_tar_open_raises_error(self):
        instance = Instance(
            dicom_uri="gs://some_series.tar://instances/some_instance.dcm"
        )
        with self.assertRaises(ValueError):
            instance.open()

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
        self.assertEqual(instance.series_uid(), instance._series_uid)
        self.assertEqual(instance.study_uid(), instance._study_uid)

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
        self.assertIsNone(instance._dicom_metadata)
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

    def test_open_invalid_file(self):
        """Test that we raise an error if the file is not a dicom file"""
        instance = Instance(dicom_uri=f"{os.path.dirname(__file__)}/test_appender.py")
        with self.assertRaises(AssertionError):
            instance.open()

    def test_has_pixeldata_property(self):
        """Test that we can determine if a local dicom file has pixel data"""
        instance = Instance(dicom_uri=self.local_instance_path)
        assert instance._has_pixeldata is None  # Verify it's None before fetch
        self.assertTrue(instance.has_pixeldata)

    def test_compress(self):
        """Test that we can compress an instance to the given syntax"""
        instance = Instance(dicom_uri=self.local_instance_path)
        with instance.open() as f:
            ds = pydicom3.dcmread(f)
            self.assertEqual(
                ds.file_meta.TransferSyntaxUID, pydicom3.uid.ImplicitVRLittleEndian
            )
        uncompressed_size = instance.size()
        instance.compress()
        self.assertLess(instance.size(), uncompressed_size)
        with instance.open() as f:
            ds = pydicom3.dcmread(f)
            self.assertEqual(
                ds.file_meta.TransferSyntaxUID, pydicom3.uid.JPEG2000Lossless
            )
        # Should be pointing to the new temp file
        self.assertEqual(instance._temp_file_path, instance.dicom_uri)
        self.assertNotEqual(instance.dicom_uri, self.local_instance_path)

    def test_temp_file_cleanup(self):
        """Test that the temp file is cleaned up when the instance is deleted"""
        # make a temp file with valid dicom data
        temp_file = tempfile.NamedTemporaryFile(suffix="_TEST.dcm", delete=False)
        with open(temp_file.name, "wb") as out:
            with open(
                os.path.join(self.test_data_dir, "monochrome2.dcm"), "rb"
            ) as in_file:
                out.write(in_file.read())
        # make an instance with the temp file
        instance = Instance(dicom_uri=temp_file.name, _temp_file_path=temp_file.name)
        self.assertIsNotNone(instance._temp_file_path)
        # make sure we can read the instance
        with instance.open() as f:
            ds = pydicom3.dcmread(f)
            self.assertEqual(ds.SOPInstanceUID, self.test_instance_uid)
        # delete the instance - this should clean up the temp file
        del instance
        self.assertFalse(os.path.exists(temp_file.name))
