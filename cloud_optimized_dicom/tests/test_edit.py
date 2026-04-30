"""Integration tests for CODObject mode='e'.

Each test seeds a fresh series via mode='w', reopens in mode='e' to edit,
then reopens in mode='r' to verify the edits round-tripped correctly.
"""

import os

import pydicom3
import pytest
from google.cloud import storage

from cloud_optimized_dicom.errors import (
    CODObjectNotFoundError,
    EditSetChangedError,
    LockAcquisitionError,
)
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.tests.conftest import SeriesHandle


def test_edit_mode_happy_path(seeded_series: SeriesHandle):
    """Modify a tag in one instance; verify it persists and the other is untouched."""
    new_patient_name = "REDACTED^EDIT^TEST"

    with seeded_series.open(mode="e") as cod:
        instances = list(cod._get_instances(strict_sorting=False).values())
        assert len(instances) == 2
        target = instances[0]
        target_uid_before = target.instance_uid()
        target_crc_before = target.crc32c()
        ds = pydicom3.dcmread(target.dicom_uri)
        ds.PatientName = new_patient_name
        ds.save_as(target.dicom_uri)

    with seeded_series.open(mode="r") as cod:
        cod.extract_locally()
        after = cod._get_instances(strict_sorting=False)
        assert len(after) == 2
        assert target_uid_before in after
        # crc32c of the edited instance differs from before
        assert after[target_uid_before].crc32c() != target_crc_before
        edited_ds = pydicom3.dcmread(after[target_uid_before].dicom_uri)
        assert str(edited_ds.PatientName) == new_patient_name


def test_edit_mode_missing_series_raises(fresh_series: SeriesHandle):
    """Opening mode='e' against a never-written series raises CODObjectNotFoundError at init."""
    with pytest.raises(CODObjectNotFoundError):
        fresh_series.open(mode="e")


def test_edit_mode_append_rejected(seeded_series: SeriesHandle, series_dir: str):
    """append() is blocked inside a mode='e' context."""
    with seeded_series.open(mode="e") as cod:
        new_instance = Instance(
            dicom_uri=os.path.join(
                series_dir,
                "1.2.826.0.1.3680043.8.498.33347096455284694650050230139909637623.dcm",
            )
        )
        with pytest.raises(ValueError):
            cod.append([new_instance])


def test_edit_mode_deleted_file_raises(seeded_series: SeriesHandle):
    """Deleting a local instance file mid-edit raises EditSetChangedError on exit."""
    with pytest.raises(EditSetChangedError):
        with seeded_series.open(mode="e") as cod:
            first = next(iter(cod._get_instances(strict_sorting=False).values()))
            os.remove(first.dicom_uri)


def test_edit_mode_corrupted_uid_raises(seeded_series: SeriesHandle):
    """Mutating an instance's SOPInstanceUID raises EditSetChangedError on exit."""
    with pytest.raises(EditSetChangedError):
        with seeded_series.open(mode="e") as cod:
            first = next(iter(cod._get_instances(strict_sorting=False).values()))
            ds = pydicom3.dcmread(first.dicom_uri)
            ds.SOPInstanceUID = "1.2.3.4.5.6.7.8.9.1234567890"
            ds.save_as(first.dicom_uri)


def test_edit_mode_concurrent_lock(seeded_series: SeriesHandle):
    """Opening mode='e' twice concurrently: the second call raises LockAcquisitionError."""
    with seeded_series.open(mode="e"):
        with pytest.raises(LockAcquisitionError):
            seeded_series.open(mode="e")


def _generate_thumbnail(handle: SeriesHandle) -> tuple[str, int]:
    """Generate a thumbnail and return (uri, blob generation) for change detection."""
    with handle.open(mode="a") as cod:
        cod.get_thumbnail(generate_if_missing=True)
        thumb_uri = cod._get_metadata_field("thumbnail")["uri"]
    thumb_blob = storage.Blob.from_string(thumb_uri, client=handle.client)
    thumb_blob.reload()
    return thumb_uri, thumb_blob.generation


def test_edit_mode_thumbnail_regen_on_pd_change(seeded_series: SeriesHandle):
    """Editing an instance with pixeldata regenerates the thumbnail.

    Detection is via file-level crc32c on instances with has_pixeldata=True, so any
    edit (even a tag-only edit) to such an instance triggers regen.
    """
    thumb_uri, thumb_gen_before = _generate_thumbnail(seeded_series)

    with seeded_series.open(mode="e") as cod:
        target = next(iter(cod._get_instances(strict_sorting=False).values()))
        ds = pydicom3.dcmread(target.dicom_uri)
        ds.PatientName = "REDACTED^REGEN^TEST"
        ds.save_as(target.dicom_uri)

    thumb_blob = storage.Blob.from_string(thumb_uri, client=seeded_series.client)
    thumb_blob.reload()
    assert (
        thumb_blob.generation != thumb_gen_before
    ), "thumbnail blob should have been rewritten (new GCS generation)"

    with seeded_series.open(mode="r") as cod:
        assert cod._get_metadata_field("thumbnail") is not None
