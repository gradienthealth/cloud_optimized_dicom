import os
import unittest
from tempfile import NamedTemporaryFile

import pydicom3
from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.errors import InstanceValidationError
from cloud_optimized_dicom.hints import Hints
from cloud_optimized_dicom.instance import Instance


def example_hash_function(uid: str) -> str:
    """
    Example hash function that adds 1 to the last part of the uid (i.e 1.2.3.4 becomes 1.2.3.5)
    """
    split_uid = uid.split(".")
    last_part = split_uid[-1]
    new_last_part = str(int(last_part) + 1)
    split_uid[-1] = new_last_part
    return ".".join(split_uid)


class TestDeid(unittest.TestCase):

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

    def test_instance_hashing(self):
        """Test the cod_object hash_func_provided property"""
        instance = Instance(
            dicom_uri="gs://bucket/path/to/file.dcm",
            hints=Hints(
                instance_uid="1.2.3.4",
                series_uid="1.2.3.4",
                study_uid="1.2.3.4",
            ),
            uid_hash_func=example_hash_function,
        )
        self.assertTrue(instance.uid_hash_func)
        self.assertEqual(
            instance.instance_uid(trust_hints_if_available=True), "1.2.3.4"
        )
        self.assertEqual(
            instance.hashed_instance_uid(trust_hints_if_available=True), "1.2.3.5"
        )
        self.assertEqual(
            instance.hashed_series_uid(trust_hints_if_available=True), "1.2.3.5"
        )
        self.assertEqual(
            instance.hashed_study_uid(trust_hints_if_available=True), "1.2.3.5"
        )

    def test_instance_no_hash_func(self):
        """Test that trying to get a hashed uid without a hash function raises an error"""
        instance = Instance(
            dicom_uri="gs://bucket/path/to/file.dcm",
            hints=Hints(instance_uid="1.2.3.4"),
        )
        self.assertFalse(instance.uid_hash_func)
        with self.assertRaises(ValueError):
            instance.hashed_instance_uid(trust_hints_if_available=True)
        with self.assertRaises(ValueError):
            instance.hashed_series_uid(trust_hints_if_available=True)
        with self.assertRaises(ValueError):
            instance.hashed_study_uid(trust_hints_if_available=True)

    def test_instance_belongs_to_cod_object(self):
        """Test validation of instance belonging to a cod_object"""
        # create cod_object with original uids
        cod_object = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        # create instance with original uids
        instance = Instance(
            dicom_uri=self.local_instance_path,
            hints=Hints(
                instance_uid=self.test_instance_uid,
                series_uid=self.test_series_uid,
                study_uid=self.test_study_uid,
            ),
        )
        # with neither cod_object nor instance having a uid hash function, the instance belongs to the cod_object
        self.assertTrue(cod_object.assert_instance_belongs_to_cod_object(instance))
        # if instead the instance had a uid hash function, it would not belong to the cod_object
        instance.uid_hash_func = example_hash_function
        with self.assertRaises(InstanceValidationError):
            cod_object.assert_instance_belongs_to_cod_object(instance)
        # if instead the cod_object had hashed_uids=True, and the instance did NOT have a uid hash function, it would not belong to the cod_object
        cod_object.hashed_uids = True
        instance.uid_hash_func = None
        with self.assertRaises(InstanceValidationError):
            cod_object.assert_instance_belongs_to_cod_object(instance)
        # if both had hashing, but the cod object was created with unhashed uids, it would not belong
        instance.uid_hash_func = example_hash_function
        with self.assertRaises(InstanceValidationError):
            cod_object.assert_instance_belongs_to_cod_object(instance)
        # finally, if the cod_object had the hashed uids, and the instance had the hashed uids, the instance would belong
        cod_object.study_uid = example_hash_function(self.test_study_uid)
        cod_object.series_uid = example_hash_function(self.test_series_uid)
        cod_object.assert_instance_belongs_to_cod_object(instance)

    def test_accidental_double_hash(self):
        """Test that instances do not belong if cod_object accidentally hashed uids twice"""
        hashed_study_uid = example_hash_function(self.test_study_uid)
        hashed_series_uid = example_hash_function(self.test_series_uid)
        twice_hashed_study_uid = example_hash_function(hashed_study_uid)
        twice_hashed_series_uid = example_hash_function(hashed_series_uid)
        cod_object = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=twice_hashed_study_uid,
            series_uid=twice_hashed_series_uid,
            lock=False,
            hashed_uids=True,
        )
        instance = Instance(dicom_uri=self.local_instance_path)
        with self.assertRaises(InstanceValidationError):
            cod_object.assert_instance_belongs_to_cod_object(instance)

    def test_cod_obj_metadata_hashed_uids(self):
        """Test that cod_obj metadata hashed_uids property is correctly set"""
        # append a DEID instance to a cod object
        cod_object = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=example_hash_function(self.test_study_uid),
            series_uid=example_hash_function(self.test_series_uid),
            lock=False,
            hashed_uids=True,
        )
        instance = Instance(
            dicom_uri=self.local_instance_path, uid_hash_func=example_hash_function
        )
        append_result = cod_object.append([instance], dirty=True)
        # verify append success
        self.assertEqual(append_result.new[0], instance)
        metadata_dict = cod_object.get_metadata(dirty=True).to_dict()
        # because the cod_object has hashed_uids=True, the metadata should have deid_uids
        self.assertEqual(
            metadata_dict["deid_study_uid"], example_hash_function(self.test_study_uid)
        )
        self.assertEqual(
            metadata_dict["deid_series_uid"],
            example_hash_function(self.test_series_uid),
        )
        # the original uids should not be present in the metadata
        self.assertNotIn("study_uid", metadata_dict)
        self.assertNotIn("series_uid", metadata_dict)
        # the metadata should contain the single instance we appended
        instances_dict = metadata_dict["cod"]["instances"]
        self.assertEqual(len(instances_dict), 1)
        # this instance should have the hashed UID
        self.assertIn(instance.hashed_instance_uid(), instances_dict)
        # the original UID should not be present
        self.assertNotIn(instance.instance_uid(), instances_dict)

    def test_append_diff_hash_dupe_with_hashed_uids(self):
        """Test that a diff hash dupe is detected with hashed uids"""
        cod_object = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=example_hash_function(self.test_study_uid),
            series_uid=example_hash_function(self.test_series_uid),
            lock=False,
            hashed_uids=True,
        )
        instance = Instance(
            dicom_uri=self.local_instance_path, uid_hash_func=example_hash_function
        )
        append_result = cod_object.append([instance], dirty=True)
        self.assertEqual(append_result.new[0], instance)
        # make a diff hash dupe
        with NamedTemporaryFile(suffix=".dcm") as f:
            with pydicom3.dcmread(self.local_instance_path) as ds:
                ds.add_new((0x1234, 0x5678), "DS", "12345678")
                ds.save_as(f.name)
            diff_hash_dupe = Instance(
                dicom_uri=f.name, uid_hash_func=example_hash_function
            )
            append_result = cod_object.append([diff_hash_dupe], dirty=True)
            self.assertEqual(append_result.conflict[0], diff_hash_dupe)

    def test_serialize_deserialize_with_hashed_uids(self):
        """Test that serialize and deserialize work with hashed uids"""
        cod_object = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=example_hash_function(self.test_study_uid),
            series_uid=example_hash_function(self.test_series_uid),
            lock=False,
            hashed_uids=True,
        )
        instance = Instance(
            dicom_uri=self.local_instance_path, uid_hash_func=example_hash_function
        )
        cod_object.append([instance], dirty=True)
        # serialize the cod_object
        serialized_cod_object = cod_object.serialize()
        # deserialize the cod_object
        deserialized_cod_object = CODObject.deserialize(
            serialized_cod_object,
            client=self.client,
            uid_hash_func=example_hash_function,
        )
        # verify the deserialized cod_object is equal to the original cod_object
        self.assertEqual(deserialized_cod_object.study_uid, cod_object.study_uid)
        self.assertEqual(deserialized_cod_object.series_uid, cod_object.series_uid)
        self.assertEqual(len(deserialized_cod_object._metadata.instances), 1)
        self.assertEqual(
            deserialized_cod_object._metadata.instances, cod_object._metadata.instances
        )
