import os
import unittest

import pydicom3
from google.api_core.client_options import ClientOptions
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.errors import (
    CODObjectNotFoundError,
    EditSetChangedError,
    LockAcquisitionError,
)
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import delete_uploaded_blobs


class TestEditMode(unittest.TestCase):
    """Integration tests for CODObject mode='e'.

    Each test seeds a fresh series via mode='w', reopens in mode='e' to edit,
    then reopens in mode='r' to verify the edits round-tripped correctly.
    """

    @classmethod
    def setUpClass(cls):
        cls.client = storage.Client(
            project="gradient-pacs-siskin-172863",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-siskin-172863"
            ),
        )
        cls.datastore_path = "gs://siskin-172863-temp/cod_tests/edit_mode"
        cls.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.series_dir = os.path.join(cls.test_data_dir, "series")
        cls.series_files = sorted(
            os.path.join(cls.series_dir, f)
            for f in os.listdir(cls.series_dir)
            if f.endswith(".dcm")
        )[:2]
        # Probe the series/study UIDs from one of the series files
        probe = Instance(dicom_uri=cls.series_files[0])
        cls.study_uid = probe.study_uid()
        cls.series_uid = probe.series_uid()
        cls.test_instance_uid = probe.instance_uid()

    def setUp(self):
        delete_uploaded_blobs(self.client, [self.datastore_path])

    def _seed_series(self, files=None):
        """Ingest `files` (defaults to self.series_files) into a fresh series via mode='w'."""
        files = files or self.series_files
        instances = [Instance(dicom_uri=p) for p in files]
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="w",
        ) as cod:
            cod.append(instances)

    def test_edit_mode_happy_path(self):
        """Modify a tag in one instance; verify it persists and the other instance is untouched."""
        self._seed_series()
        new_patient_name = "REDACTED^EDIT^TEST"

        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="e",
        ) as cod:
            instances = list(cod._get_instances(strict_sorting=False).values())
            self.assertEqual(len(instances), 2)
            target = instances[0]
            target_uid_before = target.instance_uid()
            target_crc_before = target.crc32c()
            # Rewrite target on disk with a new PatientName
            ds = pydicom3.dcmread(target.dicom_uri)
            ds.PatientName = new_patient_name
            ds.save_as(target.dicom_uri)

        # Round-trip verification in read mode
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="r",
        ) as cod:
            cod.extract_locally()
            after = cod._get_instances(strict_sorting=False)
            self.assertEqual(len(after), 2)
            # Same UID set
            self.assertIn(target_uid_before, after)
            # crc32c of the edited instance differs from before
            self.assertNotEqual(after[target_uid_before].crc32c(), target_crc_before)
            # Tag change visible
            edited_ds = pydicom3.dcmread(after[target_uid_before].dicom_uri)
            self.assertEqual(str(edited_ds.PatientName), new_patient_name)

    def test_edit_mode_missing_series_raises(self):
        """Opening mode='e' against a never-written series raises CODObjectNotFoundError at init."""
        with self.assertRaises(CODObjectNotFoundError):
            CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                mode="e",
            )

    def test_edit_mode_append_rejected(self):
        """append() is blocked inside a mode='e' context."""
        self._seed_series()
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="e",
        ) as cod:
            new_instance = Instance(
                dicom_uri=os.path.join(
                    self.series_dir,
                    "1.2.826.0.1.3680043.8.498.33347096455284694650050230139909637623.dcm",
                )
            )
            with self.assertRaises(ValueError):
                cod.append([new_instance])

    def test_edit_mode_deleted_file_raises(self):
        """Deleting a local instance file mid-edit raises EditSetChangedError on exit."""
        self._seed_series()
        with self.assertRaises(EditSetChangedError):
            with CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                mode="e",
            ) as cod:
                first = next(iter(cod._get_instances(strict_sorting=False).values()))
                os.remove(first.dicom_uri)

    def test_edit_mode_corrupted_uid_raises(self):
        """Mutating an instance's SOPInstanceUID raises EditSetChangedError on exit."""
        self._seed_series()
        with self.assertRaises(EditSetChangedError):
            with CODObject(
                client=self.client,
                datastore_path=self.datastore_path,
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                mode="e",
            ) as cod:
                first = next(iter(cod._get_instances(strict_sorting=False).values()))
                ds = pydicom3.dcmread(first.dicom_uri)
                ds.SOPInstanceUID = "1.2.3.4.5.6.7.8.9.1234567890"
                ds.save_as(first.dicom_uri)

    def test_edit_mode_concurrent_lock(self):
        """Opening mode='e' twice concurrently: the second call raises LockAcquisitionError."""
        self._seed_series()
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="e",
        ):
            with self.assertRaises(LockAcquisitionError):
                CODObject(
                    client=self.client,
                    datastore_path=self.datastore_path,
                    study_uid=self.study_uid,
                    series_uid=self.series_uid,
                    mode="e",
                )

    def _seed_and_generate_thumbnail(self):
        """Seed the series and generate a thumbnail, returning its URI + current blob generation."""
        self._seed_series()
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="a",
        ) as cod:
            cod.get_thumbnail(generate_if_missing=True)
            thumb_uri = cod._get_metadata_field("thumbnail")["uri"]
        thumb_blob = storage.Blob.from_string(thumb_uri, client=self.client)
        thumb_blob.reload()
        return thumb_uri, thumb_blob.generation

    def test_edit_mode_thumbnail_regen_on_pd_change(self):
        """Editing an instance with pixeldata regenerates the thumbnail.

        Detection is via file-level crc32c on instances with has_pixeldata=True, so
        any edit (even a tag-only edit) to such an instance triggers regen.
        """
        thumb_uri, thumb_gen_before = self._seed_and_generate_thumbnail()
        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="e",
        ) as cod:
            target = next(iter(cod._get_instances(strict_sorting=False).values()))
            ds = pydicom3.dcmread(target.dicom_uri)
            ds.PatientName = "REDACTED^REGEN^TEST"
            ds.save_as(target.dicom_uri)

        thumb_blob = storage.Blob.from_string(thumb_uri, client=self.client)
        thumb_blob.reload()
        self.assertNotEqual(
            thumb_blob.generation,
            thumb_gen_before,
            "thumbnail blob should have been rewritten (new GCS generation)",
        )

        with CODObject(
            client=self.client,
            datastore_path=self.datastore_path,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            mode="r",
        ) as cod:
            self.assertIsNotNone(cod._get_metadata_field("thumbnail"))


if __name__ == "__main__":
    unittest.main()
