"""Integration tests for pixel-data redaction via mode='e'.

Each test seeds a series, runs `redact_pixel_data` (via free function or
`CODObject.redact_pixel_data` method), then reopens in mode='r' to verify
the pixel data, audit trail, and DICOM tags round-trip as expected.
"""

import os

import numpy as np
import pydicom3
import pytest
from google.cloud import storage

from cloud_optimized_dicom.bounding_box import BoundingBox
from cloud_optimized_dicom.errors import (
    RedactionBoxOutOfBoundsError,
    RedactionFillValueError,
    RedactionFrameOutOfRangeError,
    RedactionTargetMissingError,
    WriteOperationInReadModeError,
)
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.redact import redact_pixel_data
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


@pytest.fixture
def multiframe_path(test_data_dir: str) -> str:
    return os.path.join(test_data_dir, "ybr_rct_multiframe.dcm")


@pytest.fixture
def multiframe_seeded_series(
    gcs_client: storage.Client,
    datastore_path: str,
    multiframe_path: str,
) -> SeriesHandle:
    """A fresh series ingesting the YBR_RCT JPEG2000Lossless multiframe fixture."""
    probe = Instance(dicom_uri=multiframe_path)
    handle = SeriesHandle(
        gcs_client, datastore_path, probe.study_uid(), probe.series_uid()
    )
    with handle.open(mode="w") as cod:
        cod.append([Instance(dicom_uri=multiframe_path)])
    return handle


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


def test_redact_audit_record_round_trips(seeded_series: SeriesHandle):
    """The redactions audit list survives sync and accumulates across calls."""
    with seeded_series.open(mode="r") as cod:
        uid_a, uid_b = list(cod._get_instances(strict_sorting=False).keys())[:2]

    box_1 = BoundingBox(x=0, y=0, width=10, height=10, applies_to=[uid_a])
    with seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([box_1], reviewer="alice@gradienthealth.io")

    box_2 = BoundingBox(x=5, y=5, width=20, height=20, applies_to=[uid_b])
    with seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([box_2], reviewer="bob@gradienthealth.io")

    with seeded_series.open(mode="r") as cod:
        records = cod.get_metadata_field("redactions")

    assert isinstance(records, list)
    assert len(records) == 2
    assert records[0]["reviewer"] == "alice@gradienthealth.io"
    assert records[1]["reviewer"] == "bob@gradienthealth.io"
    assert records[0]["entries"][0]["applies_to"] == [uid_a]
    assert records[1]["entries"][0]["applies_to"] == [uid_b]
    assert records[0]["entries"][0]["frames"] is None


def test_redact_dicom_tags_stamped(seeded_series: SeriesHandle):
    """BurnedInAnnotation and DeidentificationMethodCodeSequence are written
    on every redacted instance."""
    with seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    box = BoundingBox(x=0, y=0, width=5, height=5, applies_to=[target_uid])
    with seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([box], reviewer="reviewer@gradienthealth.io")

    ds = _read_remote_dataset(seeded_series, target_uid)
    assert str(ds.BurnedInAnnotation) == "NO"
    seq = ds.DeidentificationMethodCodeSequence
    assert len(seq) >= 1
    assert seq[-1].CodeValue == "113101"
    assert seq[-1].CodingSchemeDesignator == "DCM"
    assert ds.InstanceCreationDate  # truthy 8-char DA value
    assert ds.InstanceCreationTime


def test_redact_multiframe_specific_frames(multiframe_seeded_series: SeriesHandle):
    """Redacting only certain frames blacks them and leaves other frames alone."""
    with multiframe_seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    before = _read_remote_pixel_array(multiframe_seeded_series, target_uid)
    assert before.ndim == 4, "fixture should be (frames, rows, cols, samples)"

    box = BoundingBox(
        x=100,
        y=50,
        width=40,
        height=30,
        applies_to=[target_uid],
        frames=[0, 5],
    )
    with multiframe_seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([box], reviewer="frame-redactor@gradienthealth.io")

    after = _read_remote_pixel_array(multiframe_seeded_series, target_uid)

    # Redacted frames have zeros in the box region (YBR_RCT: (0,0,0) is black).
    for f in (0, 5):
        assert np.all(after[f, 50:80, 100:140, :] == 0), f"frame {f} not zeroed"

    # An unrelated frame is bit-identical to before.
    assert np.array_equal(after[3], before[3])


def test_redact_free_function_opens_edit_mode(seeded_series: SeriesHandle):
    """The module-level entry point opens mode='e' itself."""
    with seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    box = BoundingBox(x=0, y=0, width=8, height=8, applies_to=[target_uid])
    redact_pixel_data(
        client=seeded_series.client,
        datastore_path=seeded_series.datastore_path,
        study_uid=seeded_series.study_uid,
        series_uid=seeded_series.series_uid,
        boxes=[box],
        reviewer="entry-point@gradienthealth.io",
    )

    after = _read_remote_pixel_array(seeded_series, target_uid)
    assert np.all(after[0:8, 0:8] == 0)


def test_redact_missing_uid_raises_and_skips_sync(seeded_series: SeriesHandle):
    """An applies_to UID not in the series raises and writes nothing."""
    with seeded_series.open(mode="r") as cod:
        real_uid = next(iter(cod._get_instances(strict_sorting=False)))
    before = _read_remote_pixel_array(seeded_series, real_uid)

    bad_box = BoundingBox(x=0, y=0, width=5, height=5, applies_to=["999.999.fake"])
    with pytest.raises(RedactionTargetMissingError):
        with seeded_series.open(mode="e") as cod:
            cod.redact_pixel_data([bad_box], reviewer="r")

    after = _read_remote_pixel_array(seeded_series, real_uid)
    assert np.array_equal(before, after), "no instance should have been mutated"

    with seeded_series.open(mode="r") as cod:
        assert cod.get_metadata_field("redactions") is None


def test_redact_box_out_of_bounds_raises(seeded_series: SeriesHandle):
    with seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    huge = BoundingBox(x=0, y=0, width=99999, height=99999, applies_to=[target_uid])
    with pytest.raises(RedactionBoxOutOfBoundsError):
        with seeded_series.open(mode="e") as cod:
            cod.redact_pixel_data([huge], reviewer="r")


def test_redact_frame_out_of_range_raises(multiframe_seeded_series: SeriesHandle):
    with multiframe_seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    bad = BoundingBox(
        x=0,
        y=0,
        width=5,
        height=5,
        applies_to=[target_uid],
        frames=[9999],
    )
    with pytest.raises(RedactionFrameOutOfRangeError):
        with multiframe_seeded_series.open(mode="e") as cod:
            cod.redact_pixel_data([bad], reviewer="r")


def test_redact_fill_value_type_mismatch_raises(multiframe_seeded_series: SeriesHandle):
    """SamplesPerPixel=3 (YBR_RCT) needs a 3-tuple fill_value."""
    with multiframe_seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    box = BoundingBox(x=0, y=0, width=5, height=5, applies_to=[target_uid])
    with pytest.raises(RedactionFillValueError):
        with multiframe_seeded_series.open(mode="e") as cod:
            cod.redact_pixel_data([box], reviewer="r", fill_value=0)  # int, not tuple


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
