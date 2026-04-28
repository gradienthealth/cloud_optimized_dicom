"""Integration tests for CODObject mode='e'.

Each test seeds a fresh series via mode='w', reopens in mode='e' to edit,
then reopens in mode='r' to verify the edits round-tripped correctly.
"""

import os

import pydicom3
import pytest
from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.errors import (
    CODObjectNotFoundError,
    EditSetChangedError,
    LockAcquisitionError,
)
from cloud_optimized_dicom.instance import Instance


@pytest.fixture(scope="module")
def series_dir(test_data_dir: str) -> str:
    return os.path.join(test_data_dir, "series")


@pytest.fixture(scope="module")
def series_files(series_dir: str) -> list[str]:
    """First two .dcm files in the series fixture directory."""
    return sorted(
        os.path.join(series_dir, f)
        for f in os.listdir(series_dir)
        if f.endswith(".dcm")
    )[:2]


@pytest.fixture(scope="module")
def series_uids(series_files: list[str]) -> tuple[str, str]:
    """Probe (study_uid, series_uid) from the first series file."""
    probe = Instance(dicom_uri=series_files[0])
    return probe.study_uid(), probe.series_uid()


@pytest.fixture
def seeded_series(
    gcs_client: storage.Client,
    datastore_path: str,
    series_files: list[str],
    series_uids: tuple[str, str],
):
    """Ingest the series fixture into a fresh CODObject via mode='w'.

    Yields (study_uid, series_uid) so tests can re-open the same series.
    """
    study_uid, series_uid = series_uids
    instances = [Instance(dicom_uri=p) for p in series_files]
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=study_uid,
        series_uid=series_uid,
        mode="w",
    ) as cod:
        cod.append(instances)
    return study_uid, series_uid


def test_edit_mode_happy_path(
    gcs_client: storage.Client,
    datastore_path: str,
    seeded_series: tuple[str, str],
):
    """Modify a tag in one instance; verify it persists and the other is untouched."""
    study_uid, series_uid = seeded_series
    new_patient_name = "REDACTED^EDIT^TEST"

    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=study_uid,
        series_uid=series_uid,
        mode="e",
    ) as cod:
        instances = list(cod._get_instances(strict_sorting=False).values())
        assert len(instances) == 2
        target = instances[0]
        target_uid_before = target.instance_uid()
        target_crc_before = target.crc32c()
        ds = pydicom3.dcmread(target.dicom_uri)
        ds.PatientName = new_patient_name
        ds.save_as(target.dicom_uri)

    # Round-trip verification in read mode
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=study_uid,
        series_uid=series_uid,
        mode="r",
    ) as cod:
        cod.extract_locally()
        after = cod._get_instances(strict_sorting=False)
        assert len(after) == 2
        assert target_uid_before in after
        # crc32c of the edited instance differs from before
        assert after[target_uid_before].crc32c() != target_crc_before
        edited_ds = pydicom3.dcmread(after[target_uid_before].dicom_uri)
        assert str(edited_ds.PatientName) == new_patient_name


def test_edit_mode_missing_series_raises(
    gcs_client: storage.Client,
    datastore_path: str,
    series_uids: tuple[str, str],
):
    """Opening mode='e' against a never-written series raises CODObjectNotFoundError at init."""
    study_uid, series_uid = series_uids
    with pytest.raises(CODObjectNotFoundError):
        CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=study_uid,
            series_uid=series_uid,
            mode="e",
        )


def test_edit_mode_append_rejected(
    gcs_client: storage.Client,
    datastore_path: str,
    series_dir: str,
    seeded_series: tuple[str, str],
):
    """append() is blocked inside a mode='e' context."""
    study_uid, series_uid = seeded_series
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=study_uid,
        series_uid=series_uid,
        mode="e",
    ) as cod:
        new_instance = Instance(
            dicom_uri=os.path.join(
                series_dir,
                "1.2.826.0.1.3680043.8.498.33347096455284694650050230139909637623.dcm",
            )
        )
        with pytest.raises(ValueError):
            cod.append([new_instance])


def test_edit_mode_deleted_file_raises(
    gcs_client: storage.Client,
    datastore_path: str,
    seeded_series: tuple[str, str],
):
    """Deleting a local instance file mid-edit raises EditSetChangedError on exit."""
    study_uid, series_uid = seeded_series
    with pytest.raises(EditSetChangedError):
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=study_uid,
            series_uid=series_uid,
            mode="e",
        ) as cod:
            first = next(iter(cod._get_instances(strict_sorting=False).values()))
            os.remove(first.dicom_uri)


def test_edit_mode_corrupted_uid_raises(
    gcs_client: storage.Client,
    datastore_path: str,
    seeded_series: tuple[str, str],
):
    """Mutating an instance's SOPInstanceUID raises EditSetChangedError on exit."""
    study_uid, series_uid = seeded_series
    with pytest.raises(EditSetChangedError):
        with CODObject(
            client=gcs_client,
            datastore_path=datastore_path,
            study_uid=study_uid,
            series_uid=series_uid,
            mode="e",
        ) as cod:
            first = next(iter(cod._get_instances(strict_sorting=False).values()))
            ds = pydicom3.dcmread(first.dicom_uri)
            ds.SOPInstanceUID = "1.2.3.4.5.6.7.8.9.1234567890"
            ds.save_as(first.dicom_uri)


def test_edit_mode_concurrent_lock(
    gcs_client: storage.Client,
    datastore_path: str,
    seeded_series: tuple[str, str],
):
    """Opening mode='e' twice concurrently: the second call raises LockAcquisitionError."""
    study_uid, series_uid = seeded_series
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=study_uid,
        series_uid=series_uid,
        mode="e",
    ):
        with pytest.raises(LockAcquisitionError):
            CODObject(
                client=gcs_client,
                datastore_path=datastore_path,
                study_uid=study_uid,
                series_uid=series_uid,
                mode="e",
            )


def _generate_thumbnail_for(
    gcs_client: storage.Client,
    datastore_path: str,
    study_uid: str,
    series_uid: str,
) -> tuple[str, int]:
    """Generate a thumbnail and return (uri, blob generation) for change detection."""
    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=study_uid,
        series_uid=series_uid,
        mode="a",
    ) as cod:
        cod.get_thumbnail(generate_if_missing=True)
        thumb_uri = cod._get_metadata_field("thumbnail")["uri"]
    thumb_blob = storage.Blob.from_string(thumb_uri, client=gcs_client)
    thumb_blob.reload()
    return thumb_uri, thumb_blob.generation


def test_edit_mode_thumbnail_regen_on_pd_change(
    gcs_client: storage.Client,
    datastore_path: str,
    seeded_series: tuple[str, str],
):
    """Editing an instance with pixeldata regenerates the thumbnail.

    Detection is via file-level crc32c on instances with has_pixeldata=True, so any
    edit (even a tag-only edit) to such an instance triggers regen.
    """
    study_uid, series_uid = seeded_series
    thumb_uri, thumb_gen_before = _generate_thumbnail_for(
        gcs_client, datastore_path, study_uid, series_uid
    )

    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=study_uid,
        series_uid=series_uid,
        mode="e",
    ) as cod:
        target = next(iter(cod._get_instances(strict_sorting=False).values()))
        ds = pydicom3.dcmread(target.dicom_uri)
        ds.PatientName = "REDACTED^REGEN^TEST"
        ds.save_as(target.dicom_uri)

    thumb_blob = storage.Blob.from_string(thumb_uri, client=gcs_client)
    thumb_blob.reload()
    assert (
        thumb_blob.generation != thumb_gen_before
    ), "thumbnail blob should have been rewritten (new GCS generation)"

    with CODObject(
        client=gcs_client,
        datastore_path=datastore_path,
        study_uid=study_uid,
        series_uid=series_uid,
        mode="r",
    ) as cod:
        assert cod._get_metadata_field("thumbnail") is not None
