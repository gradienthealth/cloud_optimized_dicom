import os
from tempfile import NamedTemporaryFile

import pydicom
import pytest
from google.cloud import storage

from cloud_optimized_dicom.append import AppendResult, _assert_not_too_large
from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.hints import Hints
from cloud_optimized_dicom.instance import Instance


def test_instance_too_large(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    instance = Instance(local_instance_path, hints=Hints(size=1000000))
    assert instance.size(trust_hints_if_available=True) == 1000000
    cod_object = CODObject(
        datastore_path=datastore_path,
        client=gcs_client,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    # test instance of acceptable size is not filtered
    filtered_instances, append_result = _assert_not_too_large(
        cod_object=cod_object,
        instances=[instance],
        max_instance_size=1,
        max_series_size=100,
        append_result=AppendResult(),
    )
    assert len(filtered_instances) == 1
    assert len(append_result.errors) == 0
    # test instance of unacceptable size is filtered
    filtered_instances, append_result = _assert_not_too_large(
        cod_object=cod_object,
        instances=[instance],
        max_instance_size=0.0001,
        max_series_size=100,
        append_result=AppendResult(),
    )
    assert len(filtered_instances) == 0
    assert len(append_result.errors) == 1
    # test series being too large raises an error
    with pytest.raises(ValueError):
        _assert_not_too_large(
            cod_object=cod_object,
            instances=[instance],
            max_instance_size=1,
            max_series_size=0.0001,
            append_result=AppendResult(),
        )


def test_append(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    instance = Instance(dicom_uri=local_instance_path)
    new, same, conflict, errors = cod_obj.append([instance])
    assert len(new) == 1
    assert len(same + conflict + errors) == 0


def test_two_part_append(
    gcs_client: storage.Client, datastore_path: str, test_data_dir: str
):
    instance_a = Instance(
        os.path.join(
            test_data_dir,
            "series",
            "1.2.826.0.1.3680043.8.498.22997958494980951977704130269567444795.dcm",
        )
    )
    instance_b = Instance(
        os.path.join(
            test_data_dir,
            "series",
            "1.2.826.0.1.3680043.8.498.28109707839310833322020505651875585013.dcm",
        )
    )
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=instance_a.study_uid(),
        series_uid=instance_a.series_uid(),
        mode="w",
        sync_on_exit=False,
    )
    new, same, conflict, errors = cod_obj.append([instance_a])
    assert len(new) == 1
    assert len(same + conflict + errors) == 0
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=instance_a.study_uid(),
        series_uid=instance_a.series_uid(),
        mode="w",
        sync_on_exit=False,
    )
    new, same, conflict, errors = cod_obj.append([instance_b])
    assert len(new) == 1
    assert len(same + conflict + errors) == 0


def test_append_true_dupe(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    # start by appending instance normally
    instance = Instance(dicom_uri=local_instance_path)
    new, same, conflict, errors = cod_obj.append([instance])
    assert len(new) == 1
    assert len(same + conflict + errors) == 0
    # now append the same instance again, which should be a duplicate
    new, same, conflict, errors = cod_obj.append([instance])
    assert len(same) == 1
    assert len(conflict + new + errors) == 0


def test_append_diff_hash_dupe(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    # start by appending instance normally
    instance = Instance(dicom_uri=local_instance_path)
    new, same, conflict, errors = cod_obj.append([instance])
    assert len(new) == 1
    assert len(same + conflict + errors) == 0
    assert len(cod_obj._metadata.instances) == 1
    assert (
        cod_obj._metadata.instances[instance.instance_uid()].crc32c()
        == instance.crc32c()
    )
    # make a diff hash dupe
    with NamedTemporaryFile(suffix=".dcm") as f:
        with pydicom.dcmread(local_instance_path) as ds:
            ds.add_new((0x1234, 0x5678), "DS", "12345678")
            ds.save_as(f.name)
        assert os.path.exists(f.name)
        diff_hash_dupe = Instance(dicom_uri=f.name)
        assert diff_hash_dupe.crc32c() != instance.crc32c()
        new, same, conflict, errors = cod_obj.append([diff_hash_dupe])
        assert len(conflict) == 1
        assert len(same + new + errors) == 0


def test_append_and_sync(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
    )
    instance = Instance(dicom_uri=local_instance_path)
    new, same, conflict, errors = cod_obj.append([instance])
    assert len(new) == 1
    assert len(same + conflict + errors) == 0
    assert not cod_obj._tar_synced
    assert not cod_obj._metadata_synced
    tar_blob = storage.Blob.from_string(cod_obj.tar_uri, client=gcs_client)
    assert not tar_blob.exists()
    index_blob = storage.Blob.from_string(cod_obj.index_uri, client=gcs_client)
    assert not index_blob.exists()
    metadata_blob = storage.Blob.from_string(cod_obj.metadata_uri, client=gcs_client)
    assert not metadata_blob.exists()
    cod_obj._sync()
    assert cod_obj._tar_synced
    assert cod_obj._metadata_synced
    assert tar_blob.exists()
    assert index_blob.exists()
    assert metadata_blob.exists()


def test_append_and_sync_two_part(
    gcs_client: storage.Client, datastore_path: str, test_data_dir: str
):
    instance_a = Instance(
        os.path.join(
            test_data_dir,
            "series",
            "1.2.826.0.1.3680043.8.498.22997958494980951977704130269567444795.dcm",
        )
    )
    instance_b = Instance(
        os.path.join(
            test_data_dir,
            "series",
            "1.2.826.0.1.3680043.8.498.28109707839310833322020505651875585013.dcm",
        )
    )
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=instance_a.study_uid(),
        series_uid=instance_a.series_uid(),
        mode="w",
    ) as cod_obj:
        new, same, conflict, errors = cod_obj.append([instance_a])
        assert len(new) == 1
        assert len(same + conflict + errors) == 0
        # sync happens automatically on context exit
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=instance_a.study_uid(),
        series_uid=instance_a.series_uid(),
        mode="w",
        sync_on_exit=False,
    ) as cod_obj:
        new, same, conflict, errors = cod_obj.append([instance_b])
        assert len(new) == 1
        assert len(same + conflict + errors) == 0


def test_append_wrong_series(
    gcs_client: storage.Client, datastore_path: str, local_instance_path: str
):
    """Expect instance from different series than CODObject to error"""
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid="some_other_study_uid",
        series_uid="some_other_series_uid",
        mode="w",
        sync_on_exit=False,
    )
    bad_instance = Instance(dicom_uri=local_instance_path)
    new, same, conflict, errors = cod_obj.append([bad_instance])
    assert len(errors) == 1
    assert len(new + same + conflict) == 0
    assert "does not belong to COD object" in str(errors[0][1])


def test_append_bad_hint(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """Expect instance with bad hint to error"""
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    bad_instance = Instance(
        dicom_uri=local_instance_path,
        hints=Hints(study_uid="bad_study_uid"),
    )
    new, same, conflict, errors = cod_obj.append([bad_instance])
    assert len(errors) == 1
    assert len(new + same + conflict) == 0
    assert "Hint mismatch for field study_uid" in str(errors[0][1])


def test_append_bad_uri_remote(
    gcs_client: storage.Client,
    datastore_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """test nonexistent remote URI handling"""
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    instance = Instance(dicom_uri="gs://some-hospital/that/does/not/exist.dcm")
    new, same, conflict, errors = cod_obj.append([instance])
    assert len(errors) == 1
    assert len(new + same + conflict) == 0
    assert "not found" in str(errors[0][1])


def test_append_bad_uri_local(
    gcs_client: storage.Client,
    datastore_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """test nonexistent local URI handling"""
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    instance = Instance(dicom_uri="/some/local/path/that/does/not/exist.dcm")
    new, same, conflict, errors = cod_obj.append([instance])
    assert len(errors) == 1
    assert len(new + same + conflict) == 0
    assert "No such file or directory" in str(errors[0][1])


def test_append_mix(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """test mix of good and bad URIs"""
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    good_instance = Instance(dicom_uri=local_instance_path)
    bad_instance = Instance(dicom_uri="gs://some-hospital/that/does/not/exist.dcm")
    new, same, conflict, errors = cod_obj.append([good_instance, bad_instance])
    assert len(errors) == 1
    assert len(new + same + conflict) == 1


def test_append_corrupt_dicom(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
):
    """test that corrupt dicom is not appended"""
    good_instance = Instance(dicom_uri=local_instance_path)

    # create a corrupt dicom (has proper header but then is garbage)
    with NamedTemporaryFile(suffix=".dcm") as f:
        with pydicom.FileDataset(
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
            datastore_path=datastore_path,
            client=gcs_client,
            study_uid=good_instance.study_uid(),
            series_uid=good_instance.series_uid(),
            mode="w",
            sync_on_exit=False,
        ) as cod_obj:
            new, same, conflict, errors = cod_obj.append([bad_instance, good_instance])
            assert len(new) == 1
            assert len(same + conflict) == 0
            assert len(errors) == 1
            assert errors[0][0] == bad_instance


def test_append_dupe_uri_input(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """test duplicate URI handling"""
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    instance = Instance(dicom_uri=local_instance_path)
    instance_v2 = Instance(
        dicom_uri=local_instance_path,
        hints=Hints(
            instance_uid=instance.instance_uid(),
            crc32c="some_other_hash",
            size=instance.size() + 1,
        ),
    )
    cod_obj.append([instance_v2, instance])


def test_append_compress(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """test that compressing instances works"""
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    instance = Instance(dicom_uri=local_instance_path)
    with instance.open() as f:
        ds = pydicom.dcmread(f)
        assert ds.file_meta.TransferSyntaxUID == pydicom.uid.ImplicitVRLittleEndian
    uncompressed_size = instance.size()
    new, same, conflict, errors = cod_obj.append([instance], compress=True)
    assert len(new) == 1
    assert len(same + conflict + errors) == 0
    assert instance.size() < uncompressed_size
    with instance.open() as f:
        ds = pydicom.dcmread(f)
        assert ds.file_meta.TransferSyntaxUID == pydicom.uid.JPEG2000Lossless
    assert instance.size() < uncompressed_size


def test_append_no_compress(
    gcs_client: storage.Client,
    datastore_path: str,
    local_instance_path: str,
    test_study_uid: str,
    test_series_uid: str,
):
    """test that compress=False preserves the original transfer syntax (PROC-1927)"""
    cod_obj = CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=test_study_uid,
        series_uid=test_series_uid,
        mode="w",
        sync_on_exit=False,
    )
    instance = Instance(dicom_uri=local_instance_path)
    with instance.open() as f:
        ds = pydicom.dcmread(f)
        assert ds.file_meta.TransferSyntaxUID == pydicom.uid.ImplicitVRLittleEndian
    uncompressed_size = instance.size()
    new, same, conflict, errors = cod_obj.append([instance], compress=False)
    assert len(new) == 1
    assert len(same + conflict + errors) == 0
    # uncompressed: transfer syntax and size are unchanged
    assert instance.size() == uncompressed_size
    with instance.open() as f:
        ds = pydicom.dcmread(f)
        assert ds.file_meta.TransferSyntaxUID == pydicom.uid.ImplicitVRLittleEndian
