import pydicom
import pydicom.uid
import pytest
from pydicom.pixels import pixel_array
from pydicom.uid import (
    JPEG2000Lossless,
    JPEGBaseline8Bit,
    JPEGLossless,
    JPEGLosslessSV1,
    RLELossless,
)

import cloud_optimized_dicom.transcode as transcode
from cloud_optimized_dicom.tests.conftest import synthetic_image, write_encoded_image
from cloud_optimized_dicom.transcode import (
    TranscodeOutcome,
    recompress_to_jpeg2000_lossless,
)

MONO16 = dict(photometric="MONOCHROME2", bits_allocated=16, bits_stored=12)
RGB8 = dict(photometric="RGB", bits_allocated=8, bits_stored=8)
YBR8 = dict(photometric="YBR_FULL", bits_allocated=8, bits_stored=8)


@pytest.mark.parametrize(
    "syntax, image, expected_photometric",
    [
        (JPEGLosslessSV1, MONO16, "MONOCHROME2"),
        (JPEGLossless, MONO16, "MONOCHROME2"),
        (JPEGLosslessSV1, dict(MONO16, bits_stored=16, is_signed=True), "MONOCHROME2"),
        (
            JPEGLosslessSV1,
            dict(photometric="MONOCHROME1", bits_allocated=8, bits_stored=8),
            "MONOCHROME1",
        ),
        (JPEGLosslessSV1, dict(MONO16, number_of_frames=3), "MONOCHROME2"),
        (JPEGLosslessSV1, RGB8, "YBR_RCT"),
        (JPEGLosslessSV1, YBR8, "YBR_FULL"),
        (
            JPEGLosslessSV1,
            dict(photometric="PALETTE COLOR", bits_allocated=8, bits_stored=8),
            "PALETTE COLOR",
        ),
        (RLELossless, MONO16, "MONOCHROME2"),
        (RLELossless, dict(RGB8, planar_configuration=1), "YBR_RCT"),
        (RLELossless, YBR8, "YBR_FULL"),
    ],
    ids=[
        "sv1_mono16",
        "p14_mono16",
        "sv1_signed",
        "sv1_mono1_8bit",
        "sv1_multiframe",
        "sv1_rgb",
        "sv1_ybr_full",
        "sv1_palette",
        "rle_mono16",
        "rle_rgb_planar",
        "rle_ybr_full",
    ],
)
def test_recompresses_legacy_lossless_bit_exact(
    tmp_path, syntax, image, expected_photometric
):
    source = synthetic_image(**image)
    source_pixels = pixel_array(source, raw=True)
    ds = pydicom.dcmread(write_encoded_image(tmp_path / "source.dcm", syntax, source))
    encoded_size = len(ds.PixelData)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.RECOMPRESSED
    assert ds.file_meta.TransferSyntaxUID == JPEG2000Lossless
    assert (pixel_array(ds, raw=True) == source_pixels).all()
    assert len(ds.PixelData) < encoded_size
    assert ds.SOPInstanceUID == source.SOPInstanceUID
    assert ds.PhotometricInterpretation == expected_photometric
    assert ds.get("PlanarConfiguration", 0) == 0
    assert int(ds.get("NumberOfFrames", 1)) == image.get("number_of_frames", 1)


def test_recompressed_dataset_survives_a_save_and_reread(tmp_path):
    source = synthetic_image(**MONO16)
    source_pixels = pixel_array(source, raw=True)
    ds = pydicom.dcmread(
        write_encoded_image(tmp_path / "source.dcm", JPEGLosslessSV1, source)
    )
    recompress_to_jpeg2000_lossless(ds)

    ds.save_as(tmp_path / "recompressed.dcm")
    reread = pydicom.dcmread(tmp_path / "recompressed.dcm")

    assert reread.file_meta.TransferSyntaxUID == JPEG2000Lossless
    assert (pixel_array(reread) == source_pixels).all()


@pytest.mark.parametrize(
    "syntax", [JPEGBaseline8Bit, JPEG2000Lossless], ids=["lossy", "jpeg2000"]
)
def test_passes_through_syntaxes_outside_the_recompress_set(
    tmp_path, ybr_full_422_path, syntax
):
    ds = _dataset_in(tmp_path, syntax, ybr_full_422_path)
    before = _snapshot(ds)

    assert recompress_to_jpeg2000_lossless(ds) is TranscodeOutcome.PASSTHROUGH_SYNTAX
    assert _snapshot(ds) == before


def test_passes_through_ybr_full_422(tmp_path):
    ds = _jpeg_lossless_dataset(tmp_path, **YBR8)
    ds.PhotometricInterpretation = "YBR_FULL_422"
    before = _snapshot(ds)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_PHOTOMETRIC
    assert _snapshot(ds) == before


def test_passes_through_source_without_photometric_interpretation(tmp_path):
    ds = _jpeg_lossless_dataset(tmp_path, **MONO16)
    del ds.PhotometricInterpretation
    before = _snapshot(ds)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_PHOTOMETRIC
    assert _snapshot(ds) == before


def test_keeps_original_when_not_smaller(tmp_path, monkeypatch):
    ds = _jpeg_lossless_dataset(tmp_path, **MONO16)
    before = _snapshot(ds)
    source_frame = pixel_array(ds, raw=True, decoding_plugin="gdcm")
    oversized = b"\0" * len(ds.PixelData)
    monkeypatch.setattr(transcode, "get_encoder", lambda uid: _StubEncoder(oversized))
    monkeypatch.setattr(transcode, "decode_jpeg2000", lambda stream: source_frame)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_NOT_SMALLER
    assert _snapshot(ds) == before


def test_keeps_original_on_verify_mismatch(tmp_path, monkeypatch):
    ds = _jpeg_lossless_dataset(tmp_path, **MONO16)
    before = _snapshot(ds)
    monkeypatch.setattr(
        transcode,
        "decode_jpeg2000",
        lambda stream: pixel_array(ds, raw=True, decoding_plugin="gdcm") + 1,
    )

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_VERIFY_MISMATCH
    assert _snapshot(ds) == before


def test_keeps_original_when_encoder_fails(tmp_path, monkeypatch):
    ds = _jpeg_lossless_dataset(tmp_path, **MONO16)
    before = _snapshot(ds)
    monkeypatch.setattr(
        transcode, "get_encoder", lambda uid: _StubEncoder(RuntimeError("boom"))
    )

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_FAILED
    assert _snapshot(ds) == before


class _StubEncoder:
    """Stands in for pydicom's encoder: returns `result`, or raises it."""

    def __init__(self, result):
        self._result = result

    def encode(self, frame, **options):
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


def _dataset_in(tmp_path, syntax, lossy_path: str) -> pydicom.Dataset:
    if syntax == JPEGBaseline8Bit:
        return pydicom.dcmread(lossy_path)
    return pydicom.dcmread(
        write_encoded_image(tmp_path / "source.dcm", syntax, synthetic_image(**MONO16))
    )


def _jpeg_lossless_dataset(tmp_path, **image) -> pydicom.Dataset:
    path = write_encoded_image(
        tmp_path / "source.dcm", JPEGLosslessSV1, synthetic_image(**image)
    )
    return pydicom.dcmread(path)


def _snapshot(ds: pydicom.Dataset) -> tuple:
    return (
        ds.file_meta.TransferSyntaxUID,
        ds.get("PhotometricInterpretation"),
        ds.get("PlanarConfiguration"),
        bytes(ds.PixelData),
    )
