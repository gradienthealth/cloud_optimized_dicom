"""Integration tests for pixel-data redaction via mode='e'.

Each test seeds a series, runs `cod.redact_pixel_data(...)`, then reopens in
mode='r' to verify the pixel data, audit trail, and DICOM tags round-trip
as expected.
"""

import os

import numpy as np
import pydicom
import pydicom.examples
import pytest
from google.cloud import storage

from cloud_optimized_dicom.bounding_box import BoundingBox, PixelRedaction
from cloud_optimized_dicom.errors import (
    RedactionBoxOutOfBoundsError,
    RedactionFillValueError,
    RedactionFrameOutOfRangeError,
    RedactionTargetMissingError,
    WriteOperationInReadModeError,
)
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.tests.conftest import SeriesHandle


def _read_remote_dataset(handle: SeriesHandle, instance_uid: str) -> pydicom.Dataset:
    """Pull the series tar fresh from GCS and return the named instance as a Dataset."""
    with handle.open(mode="r") as cod:
        cod.extract_locally()
        instance = cod._get_instance(instance_uid)
        return pydicom.dcmread(instance.dicom_uri)


def _read_remote_pixel_array(handle: SeriesHandle, instance_uid: str) -> np.ndarray:
    return _read_remote_dataset(handle, instance_uid).pixel_array


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


@pytest.fixture
def ybr_full_422_seeded_series(
    gcs_client: storage.Client,
    datastore_path: str,
    ybr_full_422_path: str,
) -> SeriesHandle:
    """A fresh series ingesting a YBR_FULL_422 JPEG Baseline (lossy) multiframe
    fixture. Instance.compress() never re-encodes a lossy source, so the file
    lands in COD with its original YBR_FULL_422 lossy TS, exercising the
    auto-convert path that redact.py handles."""
    probe = Instance(dicom_uri=ybr_full_422_path)
    handle = SeriesHandle(
        gcs_client, datastore_path, probe.study_uid(), probe.series_uid()
    )
    with handle.open(mode="w") as cod:
        cod.append([Instance(dicom_uri=ybr_full_422_path)])
    return handle


def test_redact_single_frame_happy_path(seeded_series: SeriesHandle):
    """Redacting one box on one instance zeros that region and leaves the
    rest of the series alone."""
    with seeded_series.open(mode="r") as cod:
        uids = list(cod._get_instances(strict_sorting=False).keys())
    target_uid, untouched_uid = uids[0], uids[1]

    before_target = _read_remote_pixel_array(seeded_series, target_uid)
    before_untouched = _read_remote_pixel_array(seeded_series, untouched_uid)

    redaction = PixelRedaction(
        box=BoundingBox(x=10, y=20, width=30, height=40),
        applies_to=[target_uid],
    )

    with seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([redaction], reviewer="reviewer-a@gradienthealth.io")

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

    redaction_1 = PixelRedaction(
        box=BoundingBox(x=0, y=0, width=10, height=10), applies_to=[uid_a]
    )
    with seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([redaction_1], reviewer="alice@gradienthealth.io")

    redaction_2 = PixelRedaction(
        box=BoundingBox(x=5, y=5, width=20, height=20), applies_to=[uid_b]
    )
    with seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([redaction_2], reviewer="bob@gradienthealth.io")

    with seeded_series.open(mode="r") as cod:
        records = cod.get_metadata_field("redactions")

    assert isinstance(records, list)
    assert len(records) == 2
    assert records[0]["reviewer"] == "alice@gradienthealth.io"
    assert records[1]["reviewer"] == "bob@gradienthealth.io"
    assert records[0]["entries"][0]["applies_to"] == [uid_a]
    assert records[1]["entries"][0]["applies_to"] == [uid_b]
    assert records[0]["entries"][0]["frames"] is None
    assert records[0]["entries"][0]["box"] == {
        "x": 0,
        "y": 0,
        "width": 10,
        "height": 10,
    }


def test_redact_dicom_tags_stamped(seeded_series: SeriesHandle):
    """BurnedInAnnotation and DeidentificationMethodCodeSequence are written
    on every redacted instance, and original creation timestamps are preserved
    (those describe acquisition/reconstruction time, not "when the redactor
    last touched it")."""
    with seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    pre = _read_remote_dataset(seeded_series, target_uid)
    creation_date_before = getattr(pre, "InstanceCreationDate", None)
    creation_time_before = getattr(pre, "InstanceCreationTime", None)

    redaction = PixelRedaction(
        box=BoundingBox(x=0, y=0, width=5, height=5), applies_to=[target_uid]
    )
    with seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([redaction], reviewer="reviewer@gradienthealth.io")

    ds = _read_remote_dataset(seeded_series, target_uid)
    assert str(ds.BurnedInAnnotation) == "NO"
    seq = ds.DeidentificationMethodCodeSequence
    assert len(seq) >= 1
    assert seq[-1].CodeValue == "113101"
    assert seq[-1].CodingSchemeDesignator == "DCM"
    assert getattr(ds, "InstanceCreationDate", None) == creation_date_before
    assert getattr(ds, "InstanceCreationTime", None) == creation_time_before


def test_redact_multiframe_specific_frames(multiframe_seeded_series: SeriesHandle):
    """Redacting only certain frames blacks them and leaves other frames alone."""
    with multiframe_seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    before = _read_remote_pixel_array(multiframe_seeded_series, target_uid)
    assert before.ndim == 4, "fixture should be (frames, rows, cols, samples)"

    redaction = PixelRedaction(
        box=BoundingBox(x=100, y=50, width=40, height=30),
        applies_to=[target_uid],
        frames=[0, 5],
    )
    with multiframe_seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([redaction], reviewer="frame-redactor@gradienthealth.io")

    after = _read_remote_pixel_array(multiframe_seeded_series, target_uid)

    # Redacted frames have zeros in the box region (YBR_RCT: (0,0,0) is black).
    for f in (0, 5):
        assert np.all(after[f, 50:80, 100:140, :] == 0), f"frame {f} not zeroed"

    # An unrelated frame is bit-identical to before.
    assert np.array_equal(after[3], before[3])


def test_redact_ybr_full_422_rewrites_photometric_to_rgb(
    ybr_full_422_seeded_series: SeriesHandle,
):
    """A YBR_FULL_422 lossy source comes out as RGB JPEG 2000 Lossless.

    pydicom's pixel_array auto-converts the YBR_FULL family to RGB on decode,
    so by the time we re-encode the array IS RGB; redact.py rewrites
    PhotometricInterpretation accordingly so the header matches the bytes.
    Without that rewrite, the file would have RGB pixel data tagged as YBR.
    """
    with ybr_full_422_seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    pre = _read_remote_dataset(ybr_full_422_seeded_series, target_uid)
    assert pre.PhotometricInterpretation == "YBR_FULL_422"
    assert not str(pre.file_meta.TransferSyntaxUID).startswith(
        "1.2.840.10008.1.2.4.90"
    ), "fixture should land in COD with its original lossy TS"
    sop_before = pre.SOPInstanceUID

    redaction = PixelRedaction(
        box=BoundingBox(x=10, y=10, width=20, height=20),
        applies_to=[target_uid],
        frames=[0],
    )
    with ybr_full_422_seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([redaction], reviewer="ybr-test@gradienthealth.io")

    post = _read_remote_dataset(ybr_full_422_seeded_series, target_uid)
    assert post.PhotometricInterpretation == "RGB"
    assert str(post.file_meta.TransferSyntaxUID) == "1.2.840.10008.1.2.4.90"
    assert post.SOPInstanceUID == sop_before
    assert np.all(post.pixel_array[0, 10:30, 10:30, :] == 0)


def test_redact_missing_uid_raises_and_skips_sync(seeded_series: SeriesHandle):
    """An applies_to UID not in the series raises and writes nothing."""
    with seeded_series.open(mode="r") as cod:
        real_uid = next(iter(cod._get_instances(strict_sorting=False)))
    before = _read_remote_pixel_array(seeded_series, real_uid)

    bad = PixelRedaction(
        box=BoundingBox(x=0, y=0, width=5, height=5), applies_to=["999.999.fake"]
    )
    with pytest.raises(RedactionTargetMissingError):
        with seeded_series.open(mode="e") as cod:
            cod.redact_pixel_data([bad], reviewer="r")

    after = _read_remote_pixel_array(seeded_series, real_uid)
    assert np.array_equal(before, after), "no instance should have been mutated"

    with seeded_series.open(mode="r") as cod:
        assert cod.get_metadata_field("redactions") is None


def test_redact_box_out_of_bounds_raises(seeded_series: SeriesHandle):
    with seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    huge = PixelRedaction(
        box=BoundingBox(x=0, y=0, width=99999, height=99999), applies_to=[target_uid]
    )
    with pytest.raises(RedactionBoxOutOfBoundsError):
        with seeded_series.open(mode="e") as cod:
            cod.redact_pixel_data([huge], reviewer="r")


def test_redact_frame_out_of_range_raises(multiframe_seeded_series: SeriesHandle):
    with multiframe_seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    bad = PixelRedaction(
        box=BoundingBox(x=0, y=0, width=5, height=5),
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

    redaction = PixelRedaction(
        box=BoundingBox(x=0, y=0, width=5, height=5), applies_to=[target_uid]
    )
    with pytest.raises(RedactionFillValueError):
        with multiframe_seeded_series.open(mode="e") as cod:
            cod.redact_pixel_data(
                [redaction], reviewer="r", fill_value=0  # int, not tuple
            )


def test_redact_outputs_j2k_lossless_with_stable_sop_uid(
    seeded_series: SeriesHandle,
):
    """Output TS is JPEG 2000 Lossless and SOPInstanceUID is stable across the
    redact. If the implementation ever switches to a lossy transfer syntax,
    pydicom would auto-regenerate the SOP UID and trip mode='e' set-changed
    validation; this test guards that invariant."""
    with seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))

    pre = _read_remote_dataset(seeded_series, target_uid)
    sop_before = pre.SOPInstanceUID

    redaction = PixelRedaction(
        box=BoundingBox(x=0, y=0, width=10, height=10), applies_to=[target_uid]
    )
    with seeded_series.open(mode="e") as cod:
        cod.redact_pixel_data([redaction], reviewer="ts-test@gradienthealth.io")

    post = _read_remote_dataset(seeded_series, target_uid)
    assert str(post.file_meta.TransferSyntaxUID) == "1.2.840.10008.1.2.4.90"
    assert post.SOPInstanceUID == sop_before


def test_redact_in_read_mode_raises(seeded_series: SeriesHandle):
    """The public_method(write_only=True) decorator blocks read-mode calls."""
    with seeded_series.open(mode="r") as cod:
        target_uid = next(iter(cod._get_instances(strict_sorting=False)))
        redaction = PixelRedaction(
            box=BoundingBox(x=0, y=0, width=5, height=5), applies_to=[target_uid]
        )
        with pytest.raises(WriteOperationInReadModeError):
            cod.redact_pixel_data([redaction], reviewer="r")
