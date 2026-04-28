import pytest
from google.cloud import storage
from pydicom3 import dcmread

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import is_remote


def test_properties(gcs_client: storage.Client, datastore_path: str):
    """Test tar_uri, metadata_uri, index_uri, and __str__"""
    cod_object = CODObject(
        datastore_path=datastore_path,
        client=gcs_client,
        study_uid="1.2.3.4.5.6.7.8.9.0",
        series_uid="1.2.3.4.5.6.7.8.9.0",
        mode="r",
    )
    assert cod_object.datastore_path == datastore_path
    assert (
        cod_object.tar_uri
        == f"{datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0.tar"
    )
    assert (
        cod_object.metadata_uri
        == f"{datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0/metadata.json"
    )
    assert (
        cod_object.index_uri
        == f"{datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0/index.sqlite"
    )
    assert (
        str(cod_object)
        == f"CODObject({datastore_path}/studies/1.2.3.4.5.6.7.8.9.0/series/1.2.3.4.5.6.7.8.9.0)"
    )


def test_validate_uids(gcs_client: storage.Client, datastore_path: str):
    """Test that COD instantiation fails if UIDs are not valid"""
    with pytest.raises(AssertionError):
        CODObject(
            datastore_path=datastore_path,
            client=gcs_client,
            study_uid="1.2.3.4.5",
            series_uid="1.2.3.4.5",
            mode="r",
        )


def test_pull_tar(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
    test_instance_uid: str,
):
    """Test that pull_tar fetches the tar and index and updates the instance dicom_uri"""
    # append and sync an instance
    instance = Instance(dicom_uri=local_instance_path)
    with CODObject(
        datastore_path=datastore_path,
        client=gcs_client,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
    ) as cod_obj:
        cod_obj.append([instance])
        # sync happens automatically on context exit
    cod_obj = CODObject(
        datastore_path=datastore_path,
        client=gcs_client,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="r",
    )
    instance = cod_obj.get_metadata().instances[test_instance_uid]
    # Before we pull the tar, the instance should have a remote URI (it exists in the COD datastore)
    assert is_remote(instance.dicom_uri)
    cod_obj.pull_tar()
    # After we pull the tar, the instance should have a local URI (it exists in the local tar file)
    assert (
        instance.dicom_uri
        == f"{cod_obj.tar_file_path}://instances/{test_instance_uid}.dcm"
    )
    # We should be able to open/read the instance in this state from this local tar file
    with instance.open() as f:
        ds = dcmread(f)
        assert ds.StudyInstanceUID == test_study_uid
        assert ds.SeriesInstanceUID == test_series_uid
        assert ds.SOPInstanceUID == test_instance_uid


def test_extract_locally(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
    test_instance_uid: str,
):
    """Test that extract_locally extracts the tar and index to the local temp dir, and sets the dicom_uri of each instance to the local path"""
    # append and sync an instance
    instance = Instance(dicom_uri=local_instance_path)
    with CODObject(
        datastore_path=datastore_path,
        client=gcs_client,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
    ) as cod_obj:
        cod_obj.append([instance])
        # sync happens automatically on context exit
    cod_obj = CODObject(
        datastore_path=datastore_path,
        client=gcs_client,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="r",
    )
    instance = cod_obj.get_metadata().instances[test_instance_uid]
    # Before we extract, the instance should have a remote URI (it exists in the COD datastore)
    assert is_remote(instance.dicom_uri) and instance.is_nested_in_tar
    cod_obj.extract_locally()
    # After we extract, the instance should be local and not nested in a tar
    assert not is_remote(instance.dicom_uri) and not instance.is_nested_in_tar
    # We should be able to open/read the instance in this state from this local tar file
    with instance.open() as f:
        ds = dcmread(f)
        assert ds.StudyInstanceUID == test_study_uid
        assert ds.SeriesInstanceUID == test_series_uid
        assert ds.SOPInstanceUID == test_instance_uid


def test_instance_read_after_sync(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """Test that an instance can be read after a sync"""
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
    ) as cod_obj:
        instance = Instance(dicom_uri=local_instance_path)
        cod_obj.append([instance])
        cod_obj._sync()
        with instance.open() as f:
            ds = dcmread(f)
            assert ds.StudyInstanceUID == test_study_uid
