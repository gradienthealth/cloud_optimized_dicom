"""Smoke-level integration tests for pixel-data redaction via mode='e'.

Covers: redaction works end-to-end, output is normalized to JPEG 2000
Lossless with a stable SOPInstanceUID, and the API is mode-guarded.
Comprehensive edge-case coverage (audit trail accumulation, multiframe
frame-selection, hard-error paths, free-function entry point) lives in a
follow-up PR stacked on top of this one.
"""

import numpy as np
import pydicom3
import pytest

from cloud_optimized_dicom.bounding_box import BoundingBox
from cloud_optimized_dicom.errors import WriteOperationInReadModeError
from cloud_optimized_dicom.tests.conftest import SeriesHandle


def _read_remote_pixel_array(handle: SeriesHandle, instance_uid: str) -> np.ndarray:
    """Pull the series tar fresh from GCS, return the pixel_array of the named instance."""
    with handle.open(mode="r") as cod:
        cod.extract_locally()
        instance = cod._get_instance(instance_uid)
        return pydicom3.dcmread(instance.dicom_uri).pixel_array


def _read_remote_dataset(handle: SeriesHandle, instance_uid: str) -> pydicom3.Dataset:
    with handle.open(mode="r") as cod:
        cod.extract_locally()
        instance = cod._get_instance(instance_uid)
        return pydicom3.dcmread(instance.dicom_uri)


def test_redact_single_frame_happy_path(seeded_series: SeriesHandle):
    """Redacting one box on one instance zeros that region and leaves the
    rest of the series alone."""
    with seeded_series.open(mode="r") as cod:
        uids = list(cod._get_instances(strict_sorting=False).keys())
    target_uid, untouched_uid = uids[0], uids[1]

    before_target = _read_remote_pixel_array(seeded_series, target_uid)
    before_untouched = _read_remote_pixel_array(seeded_series, untouched_uid)

    box = BoundingBox(x=10, y=20, width=30, height=40, applies_to=[target_uid])

    with seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([box], reviewer="reviewer-a@gradienthealth.io")

    after_target = _read_remote_pixel_array(seeded_series, target_uid)
    after_untouched = _read_remote_pixel_array(seeded_series, untouched_uid)

    redacted_region = after_target[20:60, 10:40]
    assert np.all(redacted_region == 0), "redacted region should be zero (MONOCHROME2)"

    # Pixels outside the redaction box must match the original exactly
    # (JPEG 2000 Lossless round-trip is bit-exact).
    mask = np.ones_like(after_target, dtype=bool)
    mask[20:60, 10:40] = False
    assert np.array_equal(after_target[mask], before_target[mask])

    assert np.array_equal(after_untouched, before_untouched)


def test_redact_outputs_j2k_lossless_with_stable_sop_uid(
    seeded_series: SeriesHandle,
):
    """Output TS is fixed at JPEG 2000 Lossless and SOPInstanceUID is stable
    across the redact (regression: pydicom auto-regenerates the SOP UID for
    lossy compress() calls, which would trip mode='e' set-changed validation;
    we pass generate_instance_uid=False to suppress that)."""
    with seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    pre = _read_remote_dataset(seeded_series, target_uid)
    sop_before = pre.SOPInstanceUID

    box = BoundingBox(x=0, y=0, width=10, height=10, applies_to=[target_uid])
    with seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([box], reviewer="ts-test@gradienthealth.io")

    post = _read_remote_dataset(seeded_series, target_uid)
    assert str(post.file_meta.TransferSyntaxUID) == "1.2.840.10008.1.2.4.90"
    assert post.SOPInstanceUID == sop_before


def test_redact_in_read_mode_raises(seeded_series: SeriesHandle):
    """The public_method(write_only=True) decorator blocks read-mode calls."""
    with seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))
        box = BoundingBox(x=0, y=0, width=5, height=5, applies_to=[target_uid])
        with pytest.raises(WriteOperationInReadModeError):
            cod.redact_pixel_data([box], reviewer="r")
