from tempfile import NamedTemporaryFile

import pydicom
import pytest
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


def test_instance_hashing():
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
    assert instance.uid_hash_func
    assert instance.instance_uid(trust_hints_if_available=True) == "1.2.3.4"
    assert instance.hashed_instance_uid(trust_hints_if_available=True) == "1.2.3.5"
    assert instance.hashed_series_uid(trust_hints_if_available=True) == "1.2.3.5"
    assert instance.hashed_study_uid(trust_hints_if_available=True) == "1.2.3.5"


def test_instance_no_hash_func():
    """Test that trying to get a hashed uid without a hash function raises an error"""
    instance = Instance(
        dicom_uri="gs://bucket/path/to/file.dcm",
        hints=Hints(instance_uid="1.2.3.4"),
    )
    assert not instance.uid_hash_func
    with pytest.raises(ValueError):
        instance.hashed_instance_uid(trust_hints_if_available=True)
    with pytest.raises(ValueError):
        instance.hashed_series_uid(trust_hints_if_available=True)
    with pytest.raises(ValueError):
        instance.hashed_study_uid(trust_hints_if_available=True)


def test_instance_belongs_to_cod_object(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
    test_instance_uid: str,
):
    """Test validation of instance belonging to a cod_object"""
    # create cod_object with original uids
    cod_object = CODObject(
        datastore_path=datastore_path,
        client=gcs_client,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    # create instance with original uids
    instance = Instance(
        dicom_uri=local_instance_path,
        hints=Hints(
            instance_uid=test_instance_uid,
            series_uid=test_series_uid,
            study_uid=test_study_uid,
        ),
    )
    # with neither cod_object nor instance having a uid hash function, the instance belongs to the cod_object
    assert cod_object.assert_instance_belongs_to_cod_object(instance)
    # if instead the instance had a uid hash function, it would not belong to the cod_object
    instance.uid_hash_func = example_hash_function
    with pytest.raises(InstanceValidationError):
        cod_object.assert_instance_belongs_to_cod_object(instance)
    # if instead the cod_object had hashed_uids=True, and the instance did NOT have a uid hash function, it would not belong to the cod_object
    cod_object.hashed_uids = True
    instance.uid_hash_func = None
    with pytest.raises(InstanceValidationError):
        cod_object.assert_instance_belongs_to_cod_object(instance)
    # if both had hashing, but the cod object was created with unhashed uids, it would not belong
    instance.uid_hash_func = example_hash_function
    with pytest.raises(InstanceValidationError):
        cod_object.assert_instance_belongs_to_cod_object(instance)
    # finally, if the cod_object had the hashed uids, and the instance had the hashed uids, the instance would belong
    cod_object.study_uid = example_hash_function(test_study_uid)
    cod_object.series_uid = example_hash_function(test_series_uid)
    cod_object.assert_instance_belongs_to_cod_object(instance)


def test_accidental_double_hash(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """Test that instances do not belong if cod_object accidentally hashed uids twice"""
    hashed_study_uid = example_hash_function(test_study_uid)
    hashed_series_uid = example_hash_function(test_series_uid)
    twice_hashed_study_uid = example_hash_function(hashed_study_uid)
    twice_hashed_series_uid = example_hash_function(hashed_series_uid)
    cod_object = CODObject(
        datastore_path=datastore_path,
        client=gcs_client,
        study_uid=twice_hashed_study_uid,
        series_uid=twice_hashed_series_uid,
        mode="w",
        sync_on_exit=False,
        hashed_uids=True,
    )
    instance = Instance(dicom_uri=local_instance_path)
    with pytest.raises(InstanceValidationError):
        cod_object.assert_instance_belongs_to_cod_object(instance)


def test_cod_obj_metadata_hashed_uids(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """Test that cod_obj metadata hashed_uids property is correctly set"""
    # append a DEID instance to a cod object
    cod_object = CODObject(
        datastore_path=datastore_path,
        client=gcs_client,
        study_uid=example_hash_function(test_study_uid),
        series_uid=example_hash_function(test_series_uid),
        mode="w",
        sync_on_exit=False,
        hashed_uids=True,
    )
    instance = Instance(
        dicom_uri=local_instance_path, uid_hash_func=example_hash_function
    )
    append_result = cod_object.append([instance])
    # verify append success
    assert append_result.new[0] == instance
    metadata_dict = cod_object.get_metadata().to_dict()
    # because the cod_object has hashed_uids=True, the metadata should have deid_uids
    assert metadata_dict["deid_study_uid"] == example_hash_function(test_study_uid)
    assert metadata_dict["deid_series_uid"] == example_hash_function(test_series_uid)
    # the original uids should not be present in the metadata
    assert "study_uid" not in metadata_dict
    assert "series_uid" not in metadata_dict
    # the metadata should contain the single instance we appended
    instances_dict = metadata_dict["cod"]["instances"]
    assert len(instances_dict) == 1
    # this instance should have the hashed UID
    assert instance.hashed_instance_uid() in instances_dict
    # the original UID should not be present
    assert instance.instance_uid() not in instances_dict


def test_append_diff_hash_dupe_with_hashed_uids(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """Test that a diff hash dupe is detected with hashed uids"""
    cod_object = CODObject(
        datastore_path=datastore_path,
        client=gcs_client,
        study_uid=example_hash_function(test_study_uid),
        series_uid=example_hash_function(test_series_uid),
        mode="w",
        sync_on_exit=False,
        hashed_uids=True,
    )
    instance = Instance(
        dicom_uri=local_instance_path, uid_hash_func=example_hash_function
    )
    append_result = cod_object.append([instance])
    assert append_result.new[0] == instance
    # make a diff hash dupe
    with NamedTemporaryFile(suffix=".dcm") as f:
        with pydicom.dcmread(local_instance_path) as ds:
            ds.add_new((0x1234, 0x5678), "DS", "12345678")
            ds.save_as(f.name)
        diff_hash_dupe = Instance(dicom_uri=f.name, uid_hash_func=example_hash_function)
        append_result = cod_object.append([diff_hash_dupe])
        assert append_result.conflict[0] == diff_hash_dupe
