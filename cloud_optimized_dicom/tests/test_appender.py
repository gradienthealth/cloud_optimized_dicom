import os
import unittest
from tempfile import NamedTemporaryFile

import pydicom3
from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.append import AppendResult, _assert_not_too_large
from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.hints import Hints
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import delete_uploaded_blobs


class TestAppender(unittest.TestCase):
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

    def setUp(self):
        # before running each test, make sure datastore_path is empty
        delete_uploaded_blobs(self.client, [self.datastore_path])

    def test_instance_too_large(self):
        instance = Instance(self.local_instance_path, hints=Hints(size=1000000))
        self.assertEqual(instance.size(trust_hints_if_available=True), 1000000)
        cod_object = CODObject(
            datastore_path=self.datastore_path,
            client=self.client,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        # test instance of acceptable size is not filtered
        filtered_instances, append_result = _assert_not_too_large(
            cod_object=cod_object,
            instances=[instance],
            max_instance_size=1,
            max_series_size=100,
            append_result=AppendResult(),
        )
        self.assertEqual(len(filtered_instances), 1)
        self.assertEqual(len(append_result.errors), 0)
        # test instance of unacceptable size is filtered
        filtered_instances, append_result = _assert_not_too_large(
            cod_object=cod_object,
            instances=[instance],
            max_instance_size=0.0001,
            max_series_size=100,
            append_result=AppendResult(),
        )
        self.assertEqual(len(filtered_instances), 0)
        self.assertEqual(len(append_result.errors), 1)
        # test series being too large raises an error
        with self.assertRaises(ValueError):
            _assert_not_too_large(
                cod_object=cod_object,
                instances=[instance],
                max_instance_size=1,
                max_series_size=0.0001,
                append_result=AppendResult(),
            )

    def test_append(self):
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        instance = Instance(dicom_uri=self.local_instance_path)
        new, same, conflict, errors = cod_obj.append([instance], dirty=True)
        self.assertEqual(len(new), 1)
        self.assertEqual(len(same + conflict + errors), 0)

    def test_two_part_append(self):
        instance_a = Instance(
            os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.22997958494980951977704130269567444795.dcm",
            )
        )
        instance_b = Instance(
            os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.28109707839310833322020505651875585013.dcm",
            )
        )
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=instance_a.study_uid(),
            series_uid=instance_a.series_uid(),
            lock=False,
        )
        new, same, conflict, errors = cod_obj.append([instance_a], dirty=True)
        self.assertEqual(len(new), 1)
        self.assertEqual(len(same + conflict + errors), 0)
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=instance_a.study_uid(),
            series_uid=instance_a.series_uid(),
            lock=False,
        )
        new, same, conflict, errors = cod_obj.append([instance_b], dirty=True)
        self.assertEqual(len(new), 1)
        self.assertEqual(len(same + conflict + errors), 0)

    def test_append_true_dupe(self):
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        # start by appending instance normally
        instance = Instance(dicom_uri=self.local_instance_path)
        new, same, conflict, errors = cod_obj.append([instance], dirty=True)
        self.assertEqual(len(new), 1)
        self.assertEqual(len(same + conflict + errors), 0)
        # now append the same instance again, which should be a duplicate
        new, same, conflict, errors = cod_obj.append([instance], dirty=True)
        self.assertEqual(len(same), 1)
        self.assertEqual(len(conflict + new + errors), 0)

    def test_append_diff_hash_dupe(self):
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        # start by appending instance normally
        instance = Instance(dicom_uri=self.local_instance_path)
        new, same, conflict, errors = cod_obj.append([instance], dirty=True)
        self.assertEqual(len(new), 1)
        self.assertEqual(len(same + conflict + errors), 0)
        self.assertEqual(len(cod_obj._metadata.instances), 1)
        self.assertEqual(
            cod_obj._metadata.instances[instance.instance_uid()].crc32c(),
            instance.crc32c(),
        )
        # make a diff hash dupe
        with NamedTemporaryFile(suffix=".dcm") as f:
            with pydicom3.dcmread(self.local_instance_path) as ds:
                ds.add_new((0x1234, 0x5678), "DS", "12345678")
                ds.save_as(f.name)
            self.assertTrue(os.path.exists(f.name))
            diff_hash_dupe = Instance(dicom_uri=f.name)
            self.assertNotEqual(diff_hash_dupe.crc32c(), instance.crc32c())
            new, same, conflict, errors = cod_obj.append([diff_hash_dupe], dirty=True)
            self.assertEqual(len(conflict), 1)
            self.assertEqual(len(same + new + errors), 0)

    def test_append_and_sync(self):
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=True,
        )
        instance = Instance(dicom_uri=self.local_instance_path)
        new, same, conflict, errors = cod_obj.append([instance])
        self.assertEqual(len(new), 1)
        self.assertEqual(len(same + conflict + errors), 0)
        self.assertFalse(cod_obj._tar_synced)
        self.assertFalse(cod_obj._metadata_synced)
        tar_blob = storage.Blob.from_string(cod_obj.tar_uri, client=self.client)
        self.assertFalse(tar_blob.exists())
        index_blob = storage.Blob.from_string(cod_obj.index_uri, client=self.client)
        self.assertFalse(index_blob.exists())
        metadata_blob = storage.Blob.from_string(
            cod_obj.metadata_uri, client=self.client
        )
        self.assertFalse(metadata_blob.exists())
        cod_obj.sync()
        self.assertTrue(cod_obj._tar_synced)
        self.assertTrue(cod_obj._metadata_synced)
        self.assertTrue(tar_blob.exists())
        self.assertTrue(index_blob.exists())
        self.assertTrue(metadata_blob.exists())

    def test_append_and_sync_two_part(self):
        instance_a = Instance(
            os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.22997958494980951977704130269567444795.dcm",
            )
        )
        instance_b = Instance(
            os.path.join(
                self.test_data_dir,
                "series",
                "1.2.826.0.1.3680043.8.498.28109707839310833322020505651875585013.dcm",
            )
        )
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=instance_a.study_uid(),
            series_uid=instance_a.series_uid(),
            lock=True,
        ) as cod_obj:
            new, same, conflict, errors = cod_obj.append([instance_a], dirty=False)
            self.assertEqual(len(new), 1)
            self.assertEqual(len(same + conflict + errors), 0)
            cod_obj.sync()
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=instance_a.study_uid(),
            series_uid=instance_a.series_uid(),
            lock=False,
        ) as cod_obj:
            new, same, conflict, errors = cod_obj.append([instance_b], dirty=True)
            self.assertEqual(len(new), 1)
            self.assertEqual(len(same + conflict + errors), 0)

    def test_append_wrong_series(self):
        """Expect instance from different series than CODObject to error"""
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid="some_other_study_uid",
            series_uid="some_other_series_uid",
            lock=False,
        )
        bad_instance = Instance(dicom_uri=self.local_instance_path)
        new, same, conflict, errors = cod_obj.append([bad_instance], dirty=True)
        self.assertEqual(len(errors), 1)
        self.assertEqual(len(new + same + conflict), 0)
        self.assertIn("does not belong to COD object", str(errors[0][1]))

    def test_append_bad_hint(self):
        """Expect instance with bad hint to error"""
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        bad_instance = Instance(
            dicom_uri=self.local_instance_path,
            hints=Hints(study_uid="bad_study_uid"),
        )
        new, same, conflict, errors = cod_obj.append([bad_instance], dirty=True)
        self.assertEqual(len(errors), 1)
        self.assertEqual(len(new + same + conflict), 0)
        self.assertIn("Hint mismatch for field study_uid", str(errors[0][1]))

    def test_append_bad_uri_remote(self):
        """test nonexistent remote URI handling"""
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        instance = Instance(dicom_uri="gs://some-hospital/that/does/not/exist.dcm")
        new, same, conflict, errors = cod_obj.append([instance], dirty=True)
        self.assertEqual(len(errors), 1)
        self.assertEqual(len(new + same + conflict), 0)
        self.assertIn("not found", str(errors[0][1]))

    def test_append_bad_uri_local(self):
        """test nonexistent local URI handling"""
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        instance = Instance(dicom_uri="/some/local/path/that/does/not/exist.dcm")
        new, same, conflict, errors = cod_obj.append([instance], dirty=True)
        self.assertEqual(len(errors), 1)
        self.assertEqual(len(new + same + conflict), 0)
        self.assertIn("No such file or directory", str(errors[0][1]))

    def test_append_mix(self):
        """test mix of good and bad URIs"""
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        good_instance = Instance(dicom_uri=self.local_instance_path)
        bad_instance = Instance(dicom_uri="gs://some-hospital/that/does/not/exist.dcm")
        new, same, conflict, errors = cod_obj.append(
            [good_instance, bad_instance], dirty=True
        )
        self.assertEqual(len(errors), 1)
        self.assertEqual(len(new + same + conflict), 1)

    def test_append_corrupt_dicom(self):
        """test that corrupt dicom is not appended"""
        good_instance = Instance(dicom_uri=self.local_instance_path)

        # create a corrupt dicom (has proper header but then is garbage)
        with NamedTemporaryFile(suffix=".dcm") as f:
            with pydicom3.FileDataset(
                f.name, {}, is_little_endian=True, is_implicit_VR=False
            ) as ds:
                ds.StudyInstanceUID = (
                    "1.2.826.0.1.3680043.8.498.75141544885342931881503164869995724634"
                )
                ds.SeriesInstanceUID = (
                    "1.2.826.0.1.3680043.8.498.34266834008938638668629534063784433302"
                )
                ds.SOPInstanceUID = "1.2.3.4.5.6.7.8.9.0"
                ds.save_as(f.name)

            bad_instance = Instance(
                dicom_uri=f.name,
                hints=Hints(
                    size=os.path.getsize(f.name),
                    crc32c="some_crc32c",
                    instance_uid="some_instance_uid",
                    study_uid=good_instance.study_uid(),
                    series_uid=good_instance.series_uid(),
                ),
            )
            with CODObject(
                datastore_path=self.datastore_path,
                client=self.client,
                study_uid=good_instance.study_uid(),
                series_uid=good_instance.series_uid(),
                lock=True,
            ) as cod_obj:
                new, same, conflict, errors = cod_obj.append(
                    [bad_instance, good_instance]
                )
                self.assertEqual(len(new), 1)
                self.assertEqual(len(same + conflict), 0)
                self.assertEqual(len(errors), 1)
                self.assertEqual(errors[0][0], bad_instance)

    def test_append_dupe_uri_input(self):
        """test duplicate URI handling"""
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        instance = Instance(dicom_uri=self.local_instance_path)
        instance_v2 = Instance(
            dicom_uri=self.local_instance_path,
            hints=Hints(
                instance_uid=instance.instance_uid(),
                crc32c="some_other_hash",
                size=instance.size() + 1,
            ),
        )
        new, same, conflict, errors = cod_obj.append(
            [instance_v2, instance], dirty=True
        )

    def test_append_compress(self):
        """test that compressing instances works"""
        cod_obj = CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.test_study_uid,
            series_uid=self.test_series_uid,
            lock=False,
        )
        instance = Instance(dicom_uri=self.local_instance_path)
        with instance.open() as f:
            ds = pydicom3.dcmread(f)
            self.assertEqual(
                ds.file_meta.TransferSyntaxUID, pydicom3.uid.ImplicitVRLittleEndian
            )
        uncompressed_size = instance.size()
        new, same, conflict, errors = cod_obj.append(
            [instance], dirty=True, compress=True
        )
        self.assertEqual(len(new), 1)
        self.assertEqual(len(same + conflict + errors), 0)
        self.assertLess(instance.size(), uncompressed_size)
        with instance.open() as f:
            ds = pydicom3.dcmread(f)
            self.assertEqual(
                ds.file_meta.TransferSyntaxUID, pydicom3.uid.JPEG2000Lossless
            )
        self.assertLess(instance.size(), uncompressed_size)
