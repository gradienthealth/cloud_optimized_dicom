import os
import unittest

from google.api_core.client_options import ClientOptions
from google.cloud import storage
from pydicom3 import dcmread

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import delete_uploaded_blobs, is_remote


class TestCODObject(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.test_instance_uid = "1.2.276.0.50.192168001092.11156604.14547392.313"
        cls.test_series_uid = "1.2.276.0.50.192168001092.11156604.14547392.303"
        cls.test_study_uid = "1.2.276.0.50.192168001092.11156604.14547392.4"
        cls.local_instance_path = os.path.join(cls.test_data_dir, "monochrome2.dcm")
        cls.client = storage.Client(
            project="gradient-pacs-siskin-172863",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-siskin-172863"
            ),
        )
        cls.datastore_path = "gs://siskin-172863-temp/cod_tests/dicomweb"

    def test_properties(self):
        """Test tar_uri, metadata_uri, index_uri, and __str__"""
        cod_object = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid="1.2.3.4.5.6.7.8.9.0",
            series_uid="1.2.3.4.5.6.7.8.9.0",
            lock=False,
        )
        self.assertEqual(cod_object.datastore_path, self.datastore_path)
        self.assertEqual(
            cod_object.tar_uri,
            f"{self.datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0.tar",
        )
        self.assertEqual(
            cod_object.metadata_uri,
            f"{self.datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0/metadata.json",
        )
        self.assertEqual(
            cod_object.index_uri,
            f"{self.datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0/index.sqlite",
        )
        self.assertEqual(
            str(cod_object),
            f"CODObject({self.datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0)",
        )

    def test_validate_uids(self):
        """Test that COD instantiation fails if UIDs are not valid"""
        with self.assertRaises(AssertionError):
            CODObject(
                datastore_path=self.datastore_path,
                client=self.client,
                study_uid="1.2.3.4.5",
                series_uid="1.2.3.4.5",
                lock=False,
            )

    def test_pull_tar(self):
        """Test that pull_tar fetches the tar and index and updates the instance dicom_uri"""
        delete_uploaded_blobs(self.client, [self.datastore_path])
        # append and sync an instance
        instance = Instance(dicom_uri=self.local_instance_path)
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=True,
        ) as cod_obj:
            cod_obj.append([instance])
            cod_obj.sync()
        cod_obj = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        instance = cod_obj.get_metadata(dirty=True).instances[self.test_instance_uid]
        # Before we pull the tar, the instance should have a remote URI (it exists in the COD datastore)
        self.assertTrue(is_remote(instance.dicom_uri))
        cod_obj.pull_tar(dirty=True)
        # After we pull the tar, the instance should have a local URI (it exists in the local tar file)
        self.assertEqual(
            instance.dicom_uri,
            f"{cod_obj.tar_file_path}://instances/{self.test_instance_uid}.dcm",
        )
        # We should be able to open/read the instance in this state from this local tar file
        with instance.open() as f:
            ds = dcmread(f)
            self.assertEqual(ds.StudyInstanceUID, self.test_study_uid)
            self.assertEqual(ds.SeriesInstanceUID, self.test_series_uid)
            self.assertEqual(ds.SOPInstanceUID, self.test_instance_uid)

    def test_extract_locally(self):
        """Test that extract_locally extracts the tar and index to the local temp dir, and sets the dicom_uri of each instance to the local path"""
        delete_uploaded_blobs(self.client, [self.datastore_path])
        # append and sync an instance
        instance = Instance(dicom_uri=self.local_instance_path)
        with CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=True,
        ) as cod_obj:
            cod_obj.append([instance])
            cod_obj.sync()
        cod_obj = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        instance = cod_obj.get_metadata(dirty=True).instances[self.test_instance_uid]
        # Before we extract, the instance should have a remote URI (it exists in the COD datastore)
        self.assertTrue(is_remote(instance.dicom_uri) and instance.is_nested_in_tar)
        cod_obj.extract_locally(dirty=True)
        # After we extract, the instance should be local and not nested in a tar
        self.assertTrue(
            not is_remote(instance.dicom_uri) and not instance.is_nested_in_tar
        )
        # We should be able to open/read the instance in this state from this local tar file
        with instance.open() as f:
            ds = dcmread(f)
            self.assertEqual(ds.StudyInstanceUID, self.test_study_uid)
            self.assertEqual(ds.SeriesInstanceUID, self.test_series_uid)
            self.assertEqual(ds.SOPInstanceUID, self.test_instance_uid)

    def test_serialize_deserialize(self):
        """Test serialization and deserialization"""
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid="1.2.3.4.5.6.7.8.9.0",
            series_uid="1.2.3.4.5.6.7.8.9.0",
            lock=False,
        ) as cod_obj:
            serialized = cod_obj.serialize()
        with CODObject.deserialize(serialized, self.client) as deserialized:
            reserialized = deserialized.serialize()
        # Assert all public fields are equal
        for field in serialized:
            if not field.startswith("_"):
                self.assertEqual(serialized[field], reserialized[field])

    def test_instance_read_after_sync(self):
        """Test that an instance can be read after a sync"""
        delete_uploaded_blobs(self.client, [self.datastore_path])
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=True,
        ) as cod_obj:
            instance = Instance(dicom_uri=self.local_instance_path)
            cod_obj.append([instance])
            cod_obj.sync()
            with instance.open() as f:
                ds = dcmread(f)
                self.assertEqual(ds.StudyInstanceUID, self.test_study_uid)
