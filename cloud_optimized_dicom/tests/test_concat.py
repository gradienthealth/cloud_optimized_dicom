import io
import logging
import os
import tempfile
from dataclasses import dataclass

import pydicom
import pydicom.tag
import pytest

import cloud_optimized_dicom.config as config
from cloud_optimized_dicom.blob_store import BlobStore
from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Hints, Instance
from cloud_optimized_dicom.query_parsing import query_result_to_codobjects
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.utils import delete_uploaded_blobs

BUCKET_NAME = "siskin-172863-test-data"


@dataclass(frozen=True)
class ConcatPaths:
    output_uri: str
    playground_prefix: str
    # file_uri -> the local file uploaded there by _upload_grouping
    sources: dict
    grouping_full: dict
    grouping_single: dict
    grouping_first_two: dict
    grouping_last_two: dict
    grouping_including_dupe: dict


@pytest.fixture(scope="module", autouse=True)
def _enable_logging():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)


@pytest.fixture(scope="module")
def source_instances(series_dir: str) -> list[Instance]:
    """The first three instances of the local series fixture, validated so their
    uid, size and crc32c are known."""
    paths = sorted(
        os.path.join(series_dir, f)
        for f in os.listdir(series_dir)
        if f.endswith(".dcm")
    )[:3]
    instances = [Instance(dicom_uri=path) for path in paths]
    for instance in instances:
        instance.validate()
    return instances


@pytest.fixture
def concat_paths(
    blob_store: BlobStore, worker_namespace: str, source_instances: list[Instance]
) -> ConcatPaths:
    """URIs and groupings for concat tests, namespaced per (run, worker).

    Cleans the output and playground prefixes before yielding so each test
    starts from a known-empty state.
    """
    playground = f"gs://{BUCKET_NAME}/{worker_namespace}/playground"
    output = f"gs://{BUCKET_NAME}/{worker_namespace}/concat-output"
    delete_uploaded_blobs(blob_store, [output, playground])
    study_uid = source_instances[0].study_uid()
    series_uid = source_instances[0].series_uid()
    sources = {}

    def _file(idx: int, name_suffix: str = "") -> dict:
        instance = source_instances[idx]
        file_uri = (
            f"{playground}/{study_uid}/series/{series_uid}/instances/"
            f"{instance.instance_uid()}{name_suffix}.dcm"
        )
        sources[file_uri] = instance.dicom_uri
        return {
            "file_uri": file_uri,
            "size": instance.size(),
            "crc32c": instance.crc32c(),
            "instance_uid": instance.instance_uid(),
        }

    file1 = _file(0)
    file1_v2 = _file(0, name_suffix="_v2")
    file2 = _file(1)
    file3 = _file(2)
    base = {"study_uid": study_uid, "series_uid": series_uid}
    return ConcatPaths(
        output_uri=output,
        playground_prefix=playground,
        sources=sources,
        grouping_full={**base, "files": [file1, file2, file3]},
        grouping_single={**base, "files": [file1]},
        grouping_first_two={**base, "files": [file1, file2]},
        grouping_last_two={**base, "files": [file2, file3]},
        grouping_including_dupe={**base, "files": [file1, file1_v2]},
    )


def _upload_grouping(blob_store: BlobStore, concat_paths: ConcatPaths, grouping: dict):
    for file in grouping["files"]:
        blob_store.upload_file(file["file_uri"], concat_paths.sources[file["file_uri"]])


def _read_metadata(blob_store: BlobStore, cod_obj: CODObject) -> SeriesMetadata:
    return SeriesMetadata.from_bytes(blob_store.read_bytes(cod_obj.metadata_uri))


def _assert_metadata_equal(a: SeriesMetadata, b: SeriesMetadata):
    # same series
    assert a.study_uid == b.study_uid
    assert a.series_uid == b.series_uid
    # same number of instances
    assert len(a.instances) == len(b.instances)
    # every hash, uri, and size in a exists in b as well
    # (actual byte ranges could differ)
    b_data = {"hashes": [], "uris": [], "sizes": []}
    for binstance_uid, binstance in b.instances.items():
        b_data["hashes"].append(binstance.crc32c())
        b_data["uris"].append(binstance.dicom_uri)
        b_data["sizes"].append(binstance.size())
    for ainstance_uid, ainstance in a.instances.items():
        assert ainstance.crc32c() is not None
        assert ainstance.dicom_uri is not None
        assert ainstance.crc32c() in b_data["hashes"]
        assert ainstance.dicom_uri in b_data["uris"]
        assert ainstance.size() in b_data["sizes"]


def _assert_originals_deleted(blob_store: BlobStore, grouping: dict):
    """assert the source files a grouping was ingested from no longer exist"""
    for file in grouping["files"]:
        assert not blob_store.exists(file["file_uri"]), file["file_uri"]


def run_group(
    blob_store: BlobStore,
    concat_paths: ConcatPaths,
    grouping: dict,
    dryrun: bool = False,
) -> CODObject:
    """Upload a series, confirm it uploaded, confirm it deleted original"""
    _upload_grouping(blob_store, concat_paths, grouping)
    codobj_instance_pairs = query_result_to_codobjects(
        blob_store, grouping, concat_paths.output_uri, validate_datastore_path=False
    )
    assert len(codobj_instance_pairs) == 1
    cod_obj, instances = codobj_instance_pairs[0]
    new, same, conflict, errors = cod_obj.append(instances)
    cod_obj._sync()
    assert len(errors) == 0
    # confirm blobs that should exist, exist
    assert blob_store.exists(cod_obj.tar_uri), f"{cod_obj.tar_uri} does not exist"
    assert blob_store.exists(
        cod_obj.metadata_uri
    ), f"{cod_obj.metadata_uri} does not exist"
    if not dryrun:
        for instance in new + same:
            instance.delete_dependencies()
        _assert_originals_deleted(blob_store, grouping)
    return cod_obj


def test_single_instance(
    blob_store: BlobStore,
    concat_paths: ConcatPaths,
):
    """Upload single instance series, confirm it uploaded, confirm it deleted originals"""
    config.debug()
    with run_group(
        blob_store,
        concat_paths,
        concat_paths.grouping_single,
    ) as cod_obj:
        pass


def test_pipeline_and_check_offsets(
    blob_store: BlobStore,
    concat_paths: ConcatPaths,
):
    """Upload a 3-instance series, confirm it uploaded, confirm you can read instance from tar using byte offsets"""
    with run_group(
        blob_store,
        concat_paths,
        concat_paths.grouping_full,
    ) as cod_obj:
        with open(cod_obj.tar_file_path, "rb") as tar:
            for instance in cod_obj._metadata.instances.values():
                assert instance._byte_offsets is not None
                tar.seek(instance._byte_offsets[0])
                data = tar.read(instance._byte_offsets[1] - instance._byte_offsets[0])
                with pydicom.dcmread(io.BytesIO(data)) as ds:
                    assert ds.SOPInstanceUID == instance.instance_uid()


def test_dupe_instance(
    blob_store: BlobStore,
    concat_paths: ConcatPaths,
):
    """
    Given instanceA, instanceB, and instanceC in a series,
    upload all 3 in one run and confirm the result is the same as when they
    are uploaded in 2 steps [instanceA, instanceB]; [instanceB, instanceC]
    """
    # get expected result of full upload
    with run_group(
        blob_store,
        concat_paths,
        concat_paths.grouping_full,
    ) as cod_obj:
        metadata = _read_metadata(blob_store, cod_obj)
    # wipe GCS
    delete_uploaded_blobs(
        blob_store, [concat_paths.output_uri, concat_paths.playground_prefix]
    )
    # copy & upload first partial series
    with run_group(
        blob_store,
        concat_paths,
        concat_paths.grouping_first_two,
    ) as cod_obj_first_two:
        pass
    # copy & upload second partial series
    with run_group(
        blob_store,
        concat_paths,
        concat_paths.grouping_last_two,
    ) as cod_obj_last_two:
        # download duped bytes
        # duped_content = tar_blob.download_as_bytes()
        duped_metadata = _read_metadata(blob_store, cod_obj_last_two)
        # results are assumed to be identical if they are the same size and have equivalent metadata
        # self.assertEqual(len(upload_content), len(duped_content))
    _assert_metadata_equal(metadata, duped_metadata)


def test_dupe_group(
    blob_store: BlobStore,
    concat_paths: ConcatPaths,
    caplog,
):
    """Upload the same group twice, confirm that instances were not fetched in second tar attempt"""
    # Run a standard upload
    with run_group(
        blob_store,
        concat_paths,
        concat_paths.grouping_full,
    ) as cod_obj_full:
        pass
    # upload again, expecting 3 logs with "SKIP:DUPE_INSTANCE:SAME_HASH"
    with caplog.at_level(logging.INFO):
        with run_group(
            blob_store,
            concat_paths,
            concat_paths.grouping_full,
            dryrun=True,
        ) as cod_obj_full:
            pass
    print("\n".join(caplog.messages))
    # Filter logs that contain the same hash skip message
    same_hash_skip_msg = "Skipping duplicate instance (same hash):"
    same_hash_logs = [log for log in caplog.messages if same_hash_skip_msg in log]
    # Assert that 3 logs with "SKIP:DUPE_INSTANCE:SAME_HASH" were logged
    assert (
        len(same_hash_logs) == 3
    ), f"There should be 3 '{same_hash_skip_msg}' logs on second run"
    # we also expect a "NO NEW INSTANCES" log
    no_new_instances_msg = "No new instances:"
    no_new_instances_logs = [
        log for log in caplog.messages if no_new_instances_msg in log
    ]
    assert (
        len(no_new_instances_logs) == 1
    ), f"There should be exactly 1 '{no_new_instances_msg}' log on second run"


def test_diff_hash(
    blob_store: BlobStore,
    concat_paths: ConcatPaths,
    test_data_dir: str,
):
    """
    Upload an instance and test what happens when a different version
    of that same instance (same id, different hash) is uploaded subsequently.
    We would expect to see this second version NOT DELETED (flagged for manual_review).
    """
    dcm_path = os.path.join(test_data_dir, "monochrome2.dcm")
    # upload a version of the dicom
    v1_uri = f"{concat_paths.playground_prefix}/version1.dcm"
    v2_uri = f"{concat_paths.playground_prefix}/version2.dcm"
    v1_blob = blob_store.upload_file(v1_uri, dcm_path)
    # change something (cause hash mismatch)
    ds = pydicom.dcmread(dcm_path)
    study_uid = getattr(ds, "StudyInstanceUID")
    series_uid = getattr(ds, "SeriesInstanceUID")
    instance_uid = getattr(ds, "SOPInstanceUID")
    ds.add_new(pydicom.tag.Tag(0x6001, 0x0010), "LO", "Gradient Health")
    with tempfile.NamedTemporaryFile() as temp_file:
        ds.save_as(temp_file.name)
        v2_blob = blob_store.upload_file(v2_uri, temp_file.name)

    # blobs should have different hashes
    assert v1_blob.crc32c != v2_blob.crc32c

    with CODObject(
        study_uid=study_uid,
        series_uid=series_uid,
        datastore_path=concat_paths.output_uri,
        client=blob_store,
        mode="w",
    ) as cod_obj:
        instance_v1 = Instance(
            dicom_uri=v1_uri,
            hints=Hints(instance_uid=instance_uid),
            blob_store=blob_store,
        )
        instance_v2 = Instance(
            dicom_uri=v2_uri,
            hints=Hints(instance_uid=instance_uid),
            blob_store=blob_store,
        )
        # append v1 and sync
        cod_obj.append([instance_v1])
        cod_obj._sync()
        assert blob_store.exists(cod_obj.metadata_uri)
        # append v2 and sync
        cod_obj.append([instance_v2])
        # metadata should be desynced (diff hash dupe found), but tar should be synced
        assert not cod_obj._metadata_synced
        assert cod_obj._tar_synced
        cod_obj._sync()
        # file should still exist; deletion should be skipped due to same instance diff hash
        assert blob_store.exists(v2_uri)
        # download the metadata and confirm diff hash duplicate was logged
        duped_metadata = _read_metadata(blob_store, cod_obj)
        populated_dupe_list = duped_metadata.instances.get(
            instance_v1.instance_uid(trust_hints_if_available=True)
        )._diff_hash_dupe_paths
        assert len(populated_dupe_list) == 1
        assert populated_dupe_list[0] == v2_uri


def test_diff_hash_pixeldata(
    blob_store: BlobStore,
    concat_paths: ConcatPaths,
    test_data_dir: str,
):
    """
    Upload an instance and test what happens when a different version
    of that same instance (same id, different hash - PIXELDATA) is uploaded subsequently.
    We would expect to see this second version NOT DELETED (flagged for manual_review).
    """
    dcm_path = os.path.join(test_data_dir, "monochrome1.dcm")
    # upload a version of the dicom
    v1_uri = f"{concat_paths.playground_prefix}/version1.dcm"
    v2_uri = f"{concat_paths.playground_prefix}/version2.dcm"
    v1_blob = blob_store.upload_file(v1_uri, dcm_path)
    # load ds & cause hash mismatch in pixel data
    ds = pydicom.dcmread(dcm_path)
    study_uid = getattr(ds, "StudyInstanceUID")
    series_uid = getattr(ds, "SeriesInstanceUID")
    getattr(ds, "SOPInstanceUID")
    # flip all bits in 1000th byte (to cause hash mismatch)
    pixel_bytes = bytearray(ds.PixelData)
    pixel_bytes[1000] ^= 0xFF
    ds.PixelData = bytes(pixel_bytes)
    with tempfile.NamedTemporaryFile() as temp_file:
        ds.save_as(temp_file.name)
        v2_blob = blob_store.upload_file(v2_uri, temp_file.name)

    # blobs should have different hashes
    assert v1_blob.crc32c != v2_blob.crc32c

    with CODObject(
        datastore_path=concat_paths.output_uri,
        client=blob_store,
        study_uid=study_uid,
        series_uid=series_uid,
        mode="w",
    ) as cod_obj:
        instance_v1 = Instance(v1_uri, blob_store=blob_store)
        instance_v2 = Instance(v2_uri, blob_store=blob_store)
        # append v1 and sync
        cod_obj.append([instance_v1])
        cod_obj._sync()
        # append v2 and sync
        cod_obj.append([instance_v2], treat_metadata_diffs_as_same=True)
        # metadata should be desynced (diff hash dupe found), but tar should be synced
        assert not cod_obj._metadata_synced
        assert cod_obj._tar_synced
        cod_obj._sync()
        # file should still exist; deletion should be skipped due to same instance diff hash
        assert blob_store.exists(v2_uri)
        # download the metadata and confirm diff hash duplicate was logged
        duped_metadata = _read_metadata(blob_store, cod_obj)
        populated_dupe_list = duped_metadata.instances.get(
            instance_v1.instance_uid(trust_hints_if_available=True)
        )._diff_hash_dupe_paths
        assert len(populated_dupe_list) == 1
        assert populated_dupe_list[0] == v2_uri


def test_repeat_in_input(
    blob_store: BlobStore,
    concat_paths: ConcatPaths,
):
    """
    Test behavior when the same instance is supplied multiple times.
    We expect the duplicate(s) to get skipped.
    Tests two scenarios:
     - duplicates & singles: [instanceA, instanceB, instanceB]
     - only duplicates: [instanceA, instanceA]
    """
    _upload_grouping(blob_store, concat_paths, concat_paths.grouping_including_dupe)
    codobj_instance_pairs = query_result_to_codobjects(
        blob_store,
        concat_paths.grouping_including_dupe,
        concat_paths.output_uri,
        validate_datastore_path=False,
    )
    cod_obj, instances = codobj_instance_pairs[0]
    append_result = cod_obj.append(instances)
    # expect 1 new, 1 same, 0 other
    assert len(append_result.new) == 1
    assert len(append_result.same) == 1
    assert len(append_result.conflict) == 0
    assert len(append_result.errors) == 0


def test_error_overlarge_instances(
    blob_store: BlobStore,
    concat_paths: ConcatPaths,
):
    """Expect instances to be skipped if they are too large"""
    # set max size to 100 bytes; pipeline should raise ValueError
    _upload_grouping(blob_store, concat_paths, concat_paths.grouping_full)
    codobj_instance_pairs = query_result_to_codobjects(
        blob_store,
        concat_paths.grouping_full,
        concat_paths.output_uri,
        validate_datastore_path=False,
    )
    cod_obj, instances = codobj_instance_pairs[0]
    max_instance_size = 10000 / 1073741824
    new, same, conflict, errors = cod_obj.append(
        instances, max_instance_size=max_instance_size
    )
    assert len(new) == 0
    assert len(same) == 0
    assert len(conflict) == 0
    assert len(errors) == 3


def test_error_overlarge_series(
    blob_store: BlobStore,
    concat_paths: ConcatPaths,
):
    """Expect a ValueError if series is too large"""
    codobj_instance_pairs = query_result_to_codobjects(
        blob_store,
        concat_paths.grouping_full,
        concat_paths.output_uri,
        validate_datastore_path=False,
    )
    cod_obj, instances = codobj_instance_pairs[0]
    max_series_size = 10000 / 1073741824
    with pytest.raises(ValueError):
        cod_obj.append(instances, max_series_size=max_series_size)
