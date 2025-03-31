import io
import logging
import os
import tempfile
import unittest

import pydicom
from google.api_core.client_options import ClientOptions
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Hints, Instance
from cloud_optimized_dicom.query_parsing import query_result_to_codobjects
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.tests.vars import (
    BUCKET_NAME,
    GOLDEN_URI_PREFIX,
    GROUPING_FIRST_TWO,
    GROUPING_FULL,
    GROUPING_INCLUDING_DUPE,
    GROUPING_LAST_TWO,
    GROUPING_SINGLE,
    OUTPUT_URI,
    PLAYGROUND_URI_PREFIX,
)
from cloud_optimized_dicom.utils import delete_uploaded_blobs


def _copy_grouping_to_test_bucket(bucket: storage.Bucket, grouping: dict):
    for file in grouping["files"]:
        perm_uri = f"{GOLDEN_URI_PREFIX}/{grouping['study_uid']}/series/{grouping['series_uid']}/instances/{file['instance_uid']}.dcm"
        _copy_within_bucket(bucket, perm_uri, file["file_uri"])


def _copy_within_bucket(bucket: storage.Bucket, source_uri: str, dest_uri: str):
    _, source_blob_name = source_uri.replace("gs://", "").split("/", 1)
    _, dest_blob_name = dest_uri.replace("gs://", "").split("/", 1)
    source_blob = bucket.blob(source_blob_name)
    bucket.copy_blob(source_blob, bucket, dest_blob_name, retry=DEFAULT_RETRY)


@unittest.skipIf("SKIP_NETWORK_TESTS" in os.environ, reason="cloud storage")
class TestConcat(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)
        logging.getLogger().setLevel(logging.INFO)
        cls.client = storage.Client(
            project="gradient-pacs-siskin-172863",
            client_options=ClientOptions(
                quota_project_id="gradient-pacs-siskin-172863"
            ),
        )
        cls.bucket = cls.client.bucket(BUCKET_NAME)

    def setUp(self):
        # ensure clean test directory prior to test start
        delete_uploaded_blobs(self.client, [OUTPUT_URI, PLAYGROUND_URI_PREFIX])

    def _assert_metadata_equal(self, a: SeriesMetadata, b: SeriesMetadata):
        # same series
        self.assertEqual(a.deid_study_uid, b.deid_study_uid)
        self.assertEqual(a.deid_series_uid, b.deid_series_uid)
        # same number of instances
        self.assertEqual(len(a.instances), len(b.instances))
        # every hash, uri, and size in a exists in b as well
        # (actual byte ranges could differ)
        b_data = {"hashes": [], "uris": [], "sizes": []}
        for binstance_uid, binstance in b.instances.items():
            b_data["hashes"].append(binstance.crc32c)
            b_data["uris"].append(binstance.output_uri)
            b_data["sizes"].append(binstance.size)
        for ainstance_uid, instance in a.instances.items():
            self.assertIsNotNone(instance.crc32c)
            self.assertIsNotNone(instance.output_uri)
            self.assertIn(instance.crc32c, b_data["hashes"])
            self.assertIn(instance.output_uri, b_data["uris"])
            self.assertIn(instance.size, b_data["sizes"])

    def _assert_instances_dne(self, instances: list[Instance]):
        """assert the original uris of a group no longer exist"""
        for instance in instances:
            self.assertFalse(
                storage.Blob.from_string(
                    instance.dicom_uri, client=self.client
                ).exists()
            )

    def run_group(self, grouping: dict, dryrun=False):
        """Upload a series, confirm it uploaded, confirm it deleted original"""
        _copy_grouping_to_test_bucket(self.bucket, grouping)
        codobj_instance_pairs = query_result_to_codobjects(
            self.client, grouping, OUTPUT_URI, validate_datastore_path=False
        )
        self.assertEqual(len(codobj_instance_pairs), 1)
        cod_obj, instances = codobj_instance_pairs[0]
        new, same, conflict, errors = cod_obj.append(instances)
        cod_obj.sync()
        self.assertEqual(len(errors), 0)
        tar_blob = storage.Blob.from_string(cod_obj.tar_uri, client=self.client)
        metadata_blob = storage.Blob.from_string(
            cod_obj.metadata_uri, client=self.client
        )
        # confirm blobs that should exist, exist
        self.assertTrue(tar_blob.exists(), f"{cod_obj.tar_uri} does not exist")
        self.assertTrue(
            metadata_blob.exists(), f"{cod_obj.metadata_uri} does not exist"
        )
        if not dryrun:
            for instance in new + same:
                instance.delete_dependencies()
            self._assert_instances_dne(cod_obj._metadata.instances.values())
        return cod_obj

    def test_single_instance(self):
        """Upload single instance series, confirm it uploaded, confirm it deleted originals"""
        with self.run_group(GROUPING_SINGLE) as cod_obj:
            pass

    def test_pipeline_and_check_offsets(self):
        """Upload a 3-instance series, confirm it uploaded, confirm you can read instance from tar using byte offsets"""
        with self.run_group(GROUPING_FULL) as cod_obj:
            with open(cod_obj.tar_file_path, "rb") as tar:
                for instance in cod_obj._metadata.instances.values():
                    self.assertIsNotNone(instance.byte_offsets)
                    tar.seek(instance.byte_offsets[0])
                    data = tar.read(instance.byte_offsets[1] - instance.byte_offsets[0])
                    with pydicom.dcmread(io.BytesIO(data)) as ds:
                        self.assertEqual(ds.SOPInstanceUID, instance.instance_uid())

    def test_dupe_instance(self):
        """
        Given instanceA, instanceB, and instanceC in a series,
        upload all 3 in one run and confirm the result is the same as when they
        are uploaded in 2 steps [instanceA, instanceB]; [instanceB, instanceC]
        """
        # get expected result of full upload
        with self.run_group(GROUPING_FULL) as cod_obj:
            # download normal bytes
            tar_blob = storage.Blob.from_string(cod_obj.tar_uri, client=self.client)
            tar_blob.download_as_bytes()
            metadata_blob = storage.Blob.from_string(
                cod_obj.metadata_uri, client=self.client
            )
            metadata = SeriesMetadata.from_blob(metadata_blob)
        # wipe GCS
        delete_uploaded_blobs(self.client, [OUTPUT_URI, PLAYGROUND_URI_PREFIX])
        # copy & upload first partial series
        with self.run_group(GROUPING_FIRST_TWO) as cod_obj_first_two:
            pass
        # copy & upload second partial series
        with self.run_group(GROUPING_LAST_TWO) as cod_obj_last_two:
            # download duped bytes
            # duped_content = tar_blob.download_as_bytes()
            metadata_blob = storage.Blob.from_string(
                cod_obj_last_two.metadata_uri, client=self.client
            )
            duped_metadata = SeriesMetadata.from_blob(metadata_blob)
            # results are assumed to be identical if they are the same size and have equivalent metadata
            # self.assertEqual(len(upload_content), len(duped_content))
        self._assert_metadata_equal(metadata, duped_metadata)

    def test_dupe_group(self):
        """Upload the same group twice, confirm that instances were not fetched in second tar attempt"""
        # Run a standard upload
        with self.run_group(GROUPING_FULL) as cod_obj_full:
            pass
        # upload again, expecting 3 logs with "SKIP:DUPE_INSTANCE:SAME_HASH"
        with self.assertLogs(level="INFO") as log_capture_second:
            with self.run_group(GROUPING_FULL, dryrun=True) as cod_obj_full:
                pass
        # Filter logs that contain the text "SKIP:DUPE_INSTANCE:SAME_HASH"
        same_hash_logs = [
            log
            for log in log_capture_second.output
            if "SKIP:DUPE_INSTANCE:SAME_HASH" in log
        ]
        # Assert that 3 logs with "SKIP:DUPE_INSTANCE:SAME_HASH" were logged
        self.assertEqual(
            len(same_hash_logs),
            3,
            "There should be 3 'SKIP:DUPE_INSTANCE:SAME_HASH' logs on second run",
        )
        # we also expect a "NO NEW INSTANCES" log
        no_new_instances_logs = [
            log for log in log_capture_second.output if "NO_NEW_INSTANCES" in log
        ]
        self.assertEqual(
            len(no_new_instances_logs),
            1,
            "There should be exactly 1 'NO_NEW_INSTANCES' log on second run",
        )

    def test_diff_hash(self):
        """
        Upload an instance and test what happens when a different version
        of that same instance (same id, different hash) is uploaded subsequently.
        We would expect to see this second version NOT DELETED (flagged for manual_review).
        """
        dcm_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test_data", "valid.dcm"
        )
        # upload a version of the dicom
        v1_uri = f"{PLAYGROUND_URI_PREFIX}/version1.dcm"
        v2_uri = f"{PLAYGROUND_URI_PREFIX}/version2.dcm"
        v1_blob = storage.Blob.from_string(v1_uri, client=self.client)
        v2_blob = storage.Blob.from_string(v2_uri, client=self.client)
        v1_blob.upload_from_filename(dcm_path)
        # change something (cause hash mismatch)
        ds = pydicom.dcmread(dcm_path)
        study_uid = getattr(ds, "StudyInstanceUID")
        series_uid = getattr(ds, "SeriesInstanceUID")
        instance_uid = getattr(ds, "SOPInstanceUID")
        ds.add_new(pydicom.tag.Tag(0x6001, 0x0010), "LO", "Gradient Health")
        with tempfile.NamedTemporaryFile() as temp_file:
            ds.save_as(temp_file.name)
            v2_blob.upload_from_filename(temp_file.name)

        # blobs should have different hashes
        self.assertNotEqual(v1_blob.crc32c, v2_blob.crc32c)

        with CODObject(
            study_uid=study_uid,
            series_uid=series_uid,
            datastore_path=OUTPUT_URI,
            client=self.client,
            lock=True,
        ) as cod_obj:
            instance_v1 = Instance(
                dicom_uri=v1_uri,
                hints=Hints(instance_uid=instance_uid),
            )
            instance_v2 = Instance(
                dicom_uri=v2_uri,
                hints=Hints(instance_uid=instance_uid),
            )
            # append v1 and sync
            cod_obj.append([instance_v1])
            cod_obj.sync()
            metadata_blob = storage.Blob.from_string(
                cod_obj.metadata_uri, client=self.client
            )
            self.assertTrue(metadata_blob.exists())
            # append v2 and sync
            cod_obj.append([instance_v2])
            # metadata should be desynced (diff hash dupe found), but tar should be synced
            self.assertFalse(cod_obj._metadata_synced)
            self.assertTrue(cod_obj._tar_synced)
            cod_obj.sync()
            # file should still exist; deletion should be skipped due to same instance diff hash
            self.assertTrue(v2_blob.exists())
            # download the metadata and confirm diff hash duplicate was logged
            metadata_blob = storage.Blob.from_string(
                cod_obj.metadata_uri, client=self.client
            )
            duped_metadata = SeriesMetadata.from_blob(metadata_blob)
            populated_dupe_list = duped_metadata.instances.get(
                instance_v1.instance_uid(trust_hints_if_available=True)
            )._diff_hash_dupe_paths
            self.assertEqual(len(populated_dupe_list), 1)
            self.assertEqual(populated_dupe_list[0], v2_uri)

    def test_repeat_in_input(self):
        """
        Test behavior when the same instance is supplied multiple times.
        We expect the duplicate(s) to get skipped.
        Tests two scenarios:
         - duplicates & singles: [instanceA, instanceB, instanceB]
         - only duplicates: [instanceA, instanceA]
        """
        _copy_grouping_to_test_bucket(self.bucket, GROUPING_INCLUDING_DUPE)
        codobj_instance_pairs = query_result_to_codobjects(
            self.client,
            GROUPING_INCLUDING_DUPE,
            OUTPUT_URI,
            validate_datastore_path=False,
        )
        cod_obj, instances = codobj_instance_pairs[0]
        append_result = cod_obj.append(instances)
        # expect 1 new, 1 same, 0 other
        self.assertEqual(len(append_result.new), 1)
        self.assertEqual(len(append_result.same), 1)
        self.assertEqual(len(append_result.conflict), 0)
        self.assertEqual(len(append_result.errors), 0)

    def test_error_overlarge_instances(self):
        """Expect instances to be skipped if they are too large"""
        # set max size to 100 bytes; pipeline should raise ValueError
        _copy_grouping_to_test_bucket(self.bucket, GROUPING_FULL)
        codobj_instance_pairs = query_result_to_codobjects(
            self.client, GROUPING_FULL, OUTPUT_URI, validate_datastore_path=False
        )
        cod_obj, instances = codobj_instance_pairs[0]
        max_instance_size = 10000 / 1073741824
        new, same, conflict, errors = cod_obj.append(
            instances, max_instance_size=max_instance_size
        )
        self.assertEqual(len(new), 0)
        self.assertEqual(len(same), 0)
        self.assertEqual(len(conflict), 0)
        self.assertEqual(len(errors), 3)

    def test_error_overlarge_series(self):
        """Expect a ValueError if series is too large"""
        codobj_instance_pairs = query_result_to_codobjects(
            self.client, GROUPING_FULL, OUTPUT_URI, validate_datastore_path=False
        )
        cod_obj, instances = codobj_instance_pairs[0]
        max_series_size = 10000 / 1073741824
        with self.assertRaises(ValueError):
            cod_obj.append(instances, max_series_size=max_series_size)


if __name__ == "__main__":
    # SISKIN_ENV_ENABLED=1 python -m unittest components.cloud_optimized_dicom.tests.test_concat.TestPipelineFunctions.test_cod_obj
    unittest.main()
