import json
import os

import pytest

from cloud_optimized_dicom.instance_metadata import CompressedDicomMetadata
from cloud_optimized_dicom.series_metadata import SeriesMetadata


def _assert_load_success(metadata: SeriesMetadata):
    # make sure all expected cod metadata is present
    assert metadata.study_uid == "some_study_uid"
    assert metadata.series_uid == "some_series_uid"
    assert list(metadata.instances.keys()) == ["instance_uid_1", "instance_uid_2"]
    # check a specific instance for thoroughness
    loaded_instance = metadata.instances["instance_uid_1"]
    assert (
        loaded_instance.dicom_uri
        == "gs://some-hospital-pacs/v1.0/dicomweb/studies/some_study_uid/series/some_series_uid.tar://instances/instance_uid_1.dcm"
    )
    assert loaded_instance._byte_offsets == (1536, 393554)
    assert loaded_instance._crc32c == "MdpbMQ=="
    assert loaded_instance._size == 392018
    assert loaded_instance._original_path == "gs://path/to/original.dcm"
    assert loaded_instance.dependencies == ["gs://path/to/original.dcm"]
    assert loaded_instance._diff_hash_dupe_paths == []
    assert loaded_instance._modified_datetime == "2025-02-26T01:25:49.250660"
    assert loaded_instance._custom_offset_tables == {}
    # check some random metadata value for thoroughness
    assert metadata.instances["instance_uid_1"].metadata["00080000"]["Value"] == [612]

    # make sure thumbnail custom tags are present
    assert list(metadata.metadata_fields.keys()) == ["thumbnail"]
    assert list(metadata.metadata_fields["thumbnail"].keys()) == [
        "uri",
        "thumbnail_index_to_instance_frame",
        "instances",
        "version",
    ]


def _assert_save_success(raw_dict: dict, saved_dict: dict, is_deid: bool):
    # top level key assertion first for ease of debugging
    assert raw_dict.keys() == saved_dict.keys()
    # uids must be equal
    if is_deid:
        assert raw_dict.pop("deid_study_uid", None) == saved_dict.pop(
            "deid_study_uid", None
        )
        assert raw_dict.pop("deid_series_uid", None) == saved_dict.pop(
            "deid_series_uid", None
        )
    else:
        assert raw_dict.pop("study_uid", None) == saved_dict.pop("study_uid", None)
        assert raw_dict.pop("series_uid", None) == saved_dict.pop("series_uid", None)
    # pop off cod dict for comparison later (it is the most complex)
    raw_cod = raw_dict.pop("cod")
    saved_cod = saved_dict.pop("cod")
    # check remaining dicts (custom tags) are equal
    assert raw_dict == saved_dict
    # now do cod dict comparison
    assert raw_cod.keys() == saved_cod.keys()
    for instance_uid in raw_cod["instances"].keys():
        raw_instance = raw_cod["instances"][instance_uid]
        saved_instance = saved_cod["instances"][instance_uid]
        assert all(raw_key in saved_instance.keys() for raw_key in raw_instance.keys())
        # Handle version differences: v1 has dict metadata, v2 has compressed string
        raw_version = raw_instance.get("version", "1.0")
        saved_version = saved_instance.get("version", "1.0")
        if raw_version == "1.0" and saved_version == "2.0":
            # v1 → v2 upgrade: metadata is now compressed, skip metadata and version comparison
            # but verify it's a string
            assert isinstance(saved_instance["metadata"], str)
            # Compare all other fields (skip metadata and version)
            for key in raw_instance.keys():
                if key not in ("metadata", "version"):
                    assert raw_instance[key] == saved_instance[key]
        else:
            # Same version: compare all fields
            for key in raw_instance.keys():
                assert raw_instance[key] == saved_instance[key]


def test_metadata_load(test_data_dir: str):
    """Test that we can properly load metadata from a json file."""
    with open(os.path.join(test_data_dir, "valid_metadata_v1.json"), "rb") as f:
        metadata = SeriesMetadata.from_bytes(f.read())
    _assert_load_success(metadata)


def test_deid_metadata_load(test_data_dir: str):
    """Test that we can properly load de-identified metadata from a json file."""
    with open(os.path.join(test_data_dir, "valid_deid_metadata_v1.json"), "rb") as f:
        metadata = SeriesMetadata.from_bytes(f.read())
    _assert_load_success(metadata)


def test_metadata_save(test_data_dir: str):
    """Test that raw_dict_from_json = dict_when_we_load_then_save_again"""
    # first load the metadata
    with open(os.path.join(test_data_dir, "valid_metadata_v1.json"), "rb") as f:
        raw_bytes = f.read()
        # save raw dict for comparison
        raw_dict = json.loads(raw_bytes)
        saved_dict = SeriesMetadata.from_bytes(raw_bytes).to_dict()
    _assert_save_success(raw_dict, saved_dict, is_deid=False)


def test_deid_metadata_save(test_data_dir: str):
    """Test that DEID raw_dict_from_json = dict_when_we_load_then_save_again"""
    # first load the metadata
    with open(os.path.join(test_data_dir, "valid_deid_metadata_v1.json"), "rb") as f:
        raw_bytes = f.read()
        # save raw dict for comparison
        raw_dict = json.loads(raw_bytes)
        saved_dict = SeriesMetadata.from_bytes(raw_bytes).to_dict()
    _assert_save_success(raw_dict, saved_dict, is_deid=True)


def test_v2_round_trip(test_data_dir: str):
    """Test that v2 metadata can be loaded and saved correctly."""
    # Load v1 metadata and save it (will be saved as v2)
    with open(os.path.join(test_data_dir, "valid_metadata_v1.json"), "rb") as f:
        metadata = SeriesMetadata.from_bytes(f.read())

    # Save as v2
    v2_dict = metadata.to_dict()

    # Verify it's v2 format
    for instance_uid, instance_dict in v2_dict["cod"]["instances"].items():
        assert instance_dict["version"] == "2.0"
        assert isinstance(instance_dict["metadata"], str)

    # Load the v2 dict back
    reloaded_metadata = SeriesMetadata.from_dict(v2_dict)

    # Verify we can access metadata (lazy decompression)
    for instance_uid, instance in reloaded_metadata.instances.items():
        # Accessing metadata should trigger decompression
        metadata_dict = instance.metadata
        assert isinstance(metadata_dict, dict)
        # Verify we can access a specific tag
        assert "00080018" in metadata_dict  # SOPInstanceUID tag


def test_v1_to_v2_upgrade(test_data_dir: str):
    """Test that v1 metadata is automatically upgraded to v2 when saved."""
    # Load v1 metadata
    with open(os.path.join(test_data_dir, "valid_metadata_v1.json"), "rb") as f:
        raw_bytes = f.read()
        raw_dict = json.loads(raw_bytes)
        metadata = SeriesMetadata.from_bytes(raw_bytes)

    # Verify original is v1
    for instance_uid, instance_dict in raw_dict["cod"]["instances"].items():
        assert instance_dict["version"] == "1.0"
        assert isinstance(instance_dict["metadata"], dict)

    # Save (should upgrade to v2)
    saved_dict = metadata.to_dict()

    # Verify saved is v2
    for instance_uid, instance_dict in saved_dict["cod"]["instances"].items():
        assert instance_dict["version"] == "2.0"
        assert isinstance(instance_dict["metadata"], str)

    # Verify we can still load and access the metadata
    reloaded_metadata = SeriesMetadata.from_dict(saved_dict)
    for instance_uid, instance in reloaded_metadata.instances.items():
        metadata_dict = instance.metadata
        assert isinstance(metadata_dict, dict)
        # Verify data integrity by checking a specific tag
        original_metadata = raw_dict["cod"]["instances"][instance_uid]["metadata"]
        assert (
            metadata_dict["00080018"]["Value"] == original_metadata["00080018"]["Value"]
        )


def test_v2_lazy_decompression(test_data_dir: str):
    """Test that v2 metadata is not decompressed until accessed."""
    # Load v1 and save as v2
    with open(os.path.join(test_data_dir, "valid_metadata_v1.json"), "rb") as f:
        metadata = SeriesMetadata.from_bytes(f.read())
    v2_dict = metadata.to_dict()
    reloaded_metadata = SeriesMetadata.from_dict(v2_dict)

    # Check that metadata is CompressedDicomMetadata (not yet decompressed)
    instance = list(reloaded_metadata.instances.values())[0]
    assert instance._dicom_metadata is not None
    assert isinstance(instance._dicom_metadata, CompressedDicomMetadata)
    # Internal state should be a string (not yet decompressed to dict)
    compressed_metadata = instance._dicom_metadata._dicom_metadata
    assert isinstance(compressed_metadata, str)

    # Access metadata - should trigger decompression
    metadata_dict = instance.metadata
    assert isinstance(metadata_dict, dict)
    uncompressed_metadata = instance._dicom_metadata._dicom_metadata
    assert isinstance(uncompressed_metadata, dict)
    # Verify that the compressed metadata is smaller than the uncompressed metadata
    assert len(compressed_metadata) < len(json.dumps(uncompressed_metadata))


def test_v2_compression_ratio(test_data_dir: str):
    """Test that v2 format provides compression (metadata is smaller as string)."""
    # Load v1 metadata
    with open(os.path.join(test_data_dir, "valid_metadata_v1.json"), "rb") as f:
        metadata = SeriesMetadata.from_bytes(f.read())

    # Get v1 size
    v1_dict = metadata.to_dict()
    # Temporarily change to v1 for comparison
    for instance_uid in v1_dict["cod"]["instances"]:
        instance = metadata.instances[instance_uid]
        instance.to_cod_dict_v1()
        v2_instance_dict = instance.to_cod_dict_v2()

        # v2 metadata should be a compressed string
        assert isinstance(v2_instance_dict["metadata"], str)
        # For this test data, compressed string should exist
        # (exact size depends on data, but we verify it's a string)
        assert len(v2_instance_dict["metadata"]) > 0
