import os
import tarfile
import tempfile

import pydicom
import pytest

from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import is_remote

REMOTE_DICOM_URI = "https://code.oak-tree.tech/oak-tree/medical-imaging/dcmjs/-/raw/master/test/sample-dicom.dcm?ref_type=heads&inline=false"


def test_remote_detection(local_instance_path: str):
    assert is_remote("s3://bucket/path/to/file.dcm")
    assert is_remote("gs://bucket/path/to/file.dcm")
    assert is_remote(REMOTE_DICOM_URI)
    assert not is_remote(local_instance_path)


def test_local_open(local_instance_path: str, test_instance_uid: str):
    instance = Instance(local_instance_path)
    with instance.open() as f:
        ds = pydicom.dcmread(f)
        assert ds.SOPInstanceUID == test_instance_uid


def test_remote_open(test_instance_uid: str):
    instance = Instance(REMOTE_DICOM_URI)
    with instance.open() as f:
        ds = pydicom.dcmread(f)
        assert ds.SOPInstanceUID == test_instance_uid


def test_remote_tar_open_raises_error():
    instance = Instance(dicom_uri="gs://some_series.tar://instances/some_instance.dcm")
    with pytest.raises(ValueError):
        instance.open()


def test_validate(local_instance_path: str, test_instance_uid: str):
    instance = Instance(local_instance_path)
    assert instance._instance_uid is None
    assert instance._series_uid is None
    assert instance._study_uid is None
    instance.validate()
    # after validation, the internal fields should be populated
    assert instance._instance_uid == test_instance_uid
    assert instance._series_uid == "1.2.276.0.50.192168001092.11156604.14547392.303"
    assert instance._study_uid == "1.2.276.0.50.192168001092.11156604.14547392.4"
    # getter methods should return the same values
    assert instance.instance_uid() == instance._instance_uid
    assert instance.series_uid() == instance._series_uid
    assert instance.study_uid() == instance._study_uid


def test_append_to_series_tar(local_instance_path: str, test_instance_uid: str):
    instance = Instance(local_instance_path)
    with tempfile.TemporaryDirectory() as temp_dir:
        tar_file = os.path.join(temp_dir, "series.tar")
        with tarfile.open(tar_file, "w") as tar:
            pass
        with tarfile.open(tar_file, "a") as tar:
            instance.append_to_series_tar(tar)
        with tarfile.open(tar_file) as tar:
            assert len(tar.getnames()) == 1
            assert tar.getnames()[0] == f"instances/{test_instance_uid}.dcm"
            assert (
                tar.getmember(f"instances/{test_instance_uid}.dcm").size
                == instance.size()
            )


def test_extract_metadata(local_instance_path: str, test_instance_uid: str):
    instance = Instance(local_instance_path)
    assert instance._dicom_metadata is None
    assert instance._custom_offset_tables is None
    instance.extract_metadata(
        output_uri="gs://some_series.tar://instances/some_instance.dcm"
    )
    assert instance.metadata["00080018"]["Value"][0] == test_instance_uid
    assert instance._custom_offset_tables == {}


def test_extract_metadata_backfills_invalid_uid(local_instance_path: str):
    """Non-conformant UIDs (e.g. leading-zero components, >64 chars) are
    dropped by pydicom when `to_json_dict(suppress_invalid_tags=True)`
    serializes under strict_reading. _backfill_missing_uids must reinsert
    them from the cached Instance fields populated by validate()."""
    bad_study_uid = "1.2.840.01.2.3"  # leading zero component
    bad_series_uid = "1.2.840.10008.5.1.4.1.2.A"  # non-digit component
    bad_sop_uid = "1." + "2" * 70  # >64 chars

    with tempfile.NamedTemporaryFile(suffix=".dcm") as tmp:
        ds = pydicom.dcmread(local_instance_path)
        ds.StudyInstanceUID = bad_study_uid
        ds.SeriesInstanceUID = bad_series_uid
        ds.SOPInstanceUID = bad_sop_uid
        ds.file_meta.MediaStorageSOPInstanceUID = bad_sop_uid
        ds.save_as(tmp.name, write_like_original=False)

        instance = Instance(tmp.name)
        instance.validate()
        assert instance._study_uid == bad_study_uid
        assert instance._series_uid == bad_series_uid
        assert instance._instance_uid == bad_sop_uid

        instance.extract_metadata(
            output_uri="gs://some_series.tar://instances/some_instance.dcm"
        )

        # Without the backfill, these tags would be missing from the JSON
        # dict because to_json_dict(suppress_invalid_tags=True) drops them.
        assert instance.metadata["0020000D"]["Value"][0] == bad_study_uid
        assert instance.metadata["0020000E"]["Value"][0] == bad_series_uid
        assert instance.metadata["00080018"]["Value"][0] == bad_sop_uid


def test_delete_local_dependencies(local_instance_path: str):
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    assert os.path.exists(temp_file.name)
    instance = Instance(dicom_uri=local_instance_path, dependencies=[temp_file.name])
    instance.delete_dependencies()
    assert not os.path.exists(temp_file.name)


def test_open_invalid_file():
    """Test that we raise an error if the file is not a dicom file"""
    instance = Instance(dicom_uri=f"{os.path.dirname(__file__)}/test_appender.py")
    with pytest.raises(AssertionError):
        instance.open()


def test_has_pixeldata_property(local_instance_path: str):
    """Test that we can determine if a local dicom file has pixel data"""
    instance = Instance(dicom_uri=local_instance_path)
    assert instance._has_pixeldata is None  # Verify it's None before fetch
    assert instance.has_pixeldata


def test_compress(local_instance_path: str):
    """Test that we can compress an instance to the given syntax"""
    instance = Instance(dicom_uri=local_instance_path)
    with instance.open() as f:
        ds = pydicom.dcmread(f)
        assert ds.file_meta.TransferSyntaxUID == pydicom.uid.ImplicitVRLittleEndian
    uncompressed_size = instance.size()
    instance.compress()
    assert instance.size() < uncompressed_size
    with instance.open() as f:
        ds = pydicom.dcmread(f)
        assert ds.file_meta.TransferSyntaxUID == pydicom.uid.JPEG2000Lossless
    # Should be pointing to the new temp file
    assert instance._temp_file_path == instance.dicom_uri
    assert instance.dicom_uri != local_instance_path


def test_temp_file_cleanup(test_data_dir: str, test_instance_uid: str):
    """Test that the temp file is cleaned up when the instance is deleted"""
    # make a temp file with valid dicom data
    temp_file = tempfile.NamedTemporaryFile(suffix="_TEST.dcm", delete=False)
    with open(temp_file.name, "wb") as out:
        with open(os.path.join(test_data_dir, "monochrome2.dcm"), "rb") as in_file:
            out.write(in_file.read())
    # make an instance with the temp file
    instance = Instance(dicom_uri=temp_file.name, _temp_file_path=temp_file.name)
    assert instance._temp_file_path is not None
    # make sure we can read the instance
    with instance.open() as f:
        ds = pydicom.dcmread(f)
        assert ds.SOPInstanceUID == test_instance_uid
    # delete the instance - this should clean up the temp file
    del instance
    assert not os.path.exists(temp_file.name)
