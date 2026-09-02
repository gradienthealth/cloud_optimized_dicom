import pydicom
import pydicom.uid
import pytest
from pydicom.pixels import pixel_array
from pydicom.uid import JPEG2000Lossless, JPEGLossless, JPEGLosslessSV1, RLELossless

import cloud_optimized_dicom.transcode as transcode
from cloud_optimized_dicom.tests.conftest import synthetic_image, write_encoded_image
from cloud_optimized_dicom.transcode import (
    TranscodeOutcome,
    recompress_to_jpeg2000_lossless,
)

MONO16 = dict(photometric="MONOCHROME2", bits_allocated=16, bits_stored=12)


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
        (
            JPEGLosslessSV1,
            dict(
                photometric="RGB",
                bits_allocated=8,
                bits_stored=8,
                planar_configuration=1,
            ),
            "YBR_RCT",
        ),
        (
            JPEGLosslessSV1,
            dict(photometric="YBR_FULL", bits_allocated=8, bits_stored=8),
            "YBR_FULL",
        ),
        (
            JPEGLosslessSV1,
            dict(photometric="PALETTE COLOR", bits_allocated=8, bits_stored=8),
            "PALETTE COLOR",
        ),
        (RLELossless, MONO16, "MONOCHROME2"),
        (
            RLELossless,
            dict(photometric="YBR_FULL", bits_allocated=8, bits_stored=8),
            "YBR_FULL",
        ),
    ],
    ids=[
        "sv1_mono16",
        "p14_mono16",
        "sv1_signed",
        "sv1_mono1_8bit",
        "sv1_multiframe",
        "sv1_rgb_planar",
        "sv1_ybr_full",
        "sv1_palette",
        "rle_mono16",
        "rle_ybr_full",
    ],
)
def test_recompresses_legacy_lossless_bit_exact(
    tmp_path, syntax, image, expected_photometric
):
    path = write_encoded_image(
        tmp_path / "source.dcm", syntax, synthetic_image(**image)
    )
    ds = pydicom.dcmread(path)
    source_pixels = pixel_array(ds, raw=True, decoding_plugin=_source_plugin(syntax))
    source_size = len(ds.PixelData)
    sop_instance_uid = ds.SOPInstanceUID

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.RECOMPRESSED
    assert ds.file_meta.TransferSyntaxUID == JPEG2000Lossless
    assert (pixel_array(ds, raw=True) == source_pixels).all()
    assert len(ds.PixelData) < source_size
    assert ds.SOPInstanceUID == sop_instance_uid
    assert ds.PhotometricInterpretation == expected_photometric
    assert ds.get("PlanarConfiguration", 0) == 0
    assert int(ds.get("NumberOfFrames", 1)) == image.get("number_of_frames", 1)


def test_recompressed_dataset_survives_a_save_and_reread(tmp_path):
    path = write_encoded_image(
        tmp_path / "source.dcm", JPEGLosslessSV1, synthetic_image(**MONO16)
    )
    ds = pydicom.dcmread(path)
    source_pixels = pixel_array(ds, raw=True, decoding_plugin="gdcm")
    recompress_to_jpeg2000_lossless(ds)

    ds.save_as(tmp_path / "recompressed.dcm")
    reread = pydicom.dcmread(tmp_path / "recompressed.dcm")

    assert reread.file_meta.TransferSyntaxUID == JPEG2000Lossless
    assert (pixel_array(reread) == source_pixels).all()


def test_passes_through_lossy_source(ybr_full_422_path):
    ds = pydicom.dcmread(ybr_full_422_path)
    before = _snapshot(ds)

    assert recompress_to_jpeg2000_lossless(ds) is TranscodeOutcome.PASSTHROUGH_SYNTAX
    assert _snapshot(ds) == before


def test_passes_through_jpeg2000_source(tmp_path):
    path = write_encoded_image(
        tmp_path / "j2k.dcm", JPEG2000Lossless, synthetic_image(**MONO16)
    )
    ds = pydicom.dcmread(path)
    before = _snapshot(ds)

    assert recompress_to_jpeg2000_lossless(ds) is TranscodeOutcome.PASSTHROUGH_SYNTAX
    assert _snapshot(ds) == before


def test_passes_through_ybr_full_422(tmp_path):
    ds = _jpeg_lossless_dataset(
        tmp_path, photometric="YBR_FULL", bits_allocated=8, bits_stored=8
    )
    ds.PhotometricInterpretation = "YBR_FULL_422"
    before = _snapshot(ds)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_PHOTOMETRIC
    assert _snapshot(ds) == before


def test_keeps_original_when_not_smaller(tmp_path, monkeypatch):
    ds = _jpeg_lossless_dataset(tmp_path, **MONO16)
    before = _snapshot(ds)
    monkeypatch.setattr(
        transcode, "_encode_frames", lambda ds, pi: [b"\0" * len(ds.PixelData)]
    )

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

    def _raise(ds, pi):
        raise RuntimeError("encoder exploded")

    monkeypatch.setattr(transcode, "_encode_frames", _raise)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_FAILED
    assert _snapshot(ds) == before


def _source_plugin(syntax) -> str:
    return "gdcm" if syntax in (JPEGLossless, JPEGLosslessSV1) else ""


def _jpeg_lossless_dataset(tmp_path, **image) -> pydicom.Dataset:
    path = write_encoded_image(
        tmp_path / "source.dcm", JPEGLosslessSV1, synthetic_image(**image)
    )
    return pydicom.dcmread(path)


def _snapshot(ds: pydicom.Dataset) -> tuple:
    return (
        ds.file_meta.TransferSyntaxUID,
        ds.PhotometricInterpretation,
        ds.get("PlanarConfiguration"),
        bytes(ds.PixelData),
    )
