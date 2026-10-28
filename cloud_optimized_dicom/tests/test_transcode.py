import struct

import numpy as np
import pydicom
import pydicom.uid
import pytest
from pydicom.encaps import encapsulate
from pydicom.pixels import pixel_array
from pydicom.uid import (
    ExplicitVRLittleEndian,
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

MONO16 = {"photometric": "MONOCHROME2", "bits_allocated": 16, "bits_stored": 12}
RGB8 = {"photometric": "RGB", "bits_allocated": 8, "bits_stored": 8}
YBR8 = {"photometric": "YBR_FULL", "bits_allocated": 8, "bits_stored": 8}


@pytest.mark.parametrize(
    ("syntax", "image", "expected_photometric"),
    [
        (JPEGLosslessSV1, MONO16, "MONOCHROME2"),
        (JPEGLossless, MONO16, "MONOCHROME2"),
        (
            JPEGLosslessSV1,
            {**MONO16, "bits_stored": 16, "is_signed": True},
            "MONOCHROME2",
        ),
        (
            JPEGLosslessSV1,
            {"photometric": "MONOCHROME1", "bits_allocated": 8, "bits_stored": 8},
            "MONOCHROME1",
        ),
        (JPEGLosslessSV1, {**MONO16, "number_of_frames": 3}, "MONOCHROME2"),
        (JPEGLosslessSV1, RGB8, "YBR_RCT"),
        (JPEGLosslessSV1, YBR8, "YBR_FULL"),
        (
            JPEGLosslessSV1,
            {"photometric": "PALETTE COLOR", "bits_allocated": 8, "bits_stored": 8},
            "PALETTE COLOR",
        ),
        (RLELossless, MONO16, "MONOCHROME2"),
        (RLELossless, {**RGB8, "planar_configuration": 1}, "YBR_RCT"),
        (RLELossless, YBR8, "YBR_FULL"),
        (ExplicitVRLittleEndian, RGB8, "YBR_RCT"),
        (
            ExplicitVRLittleEndian,
            {**RGB8, "planar_configuration": 1, "number_of_frames": 3},
            "YBR_RCT",
        ),
        (ExplicitVRLittleEndian, MONO16, "MONOCHROME2"),
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
        "uncompressed_rgb",
        "uncompressed_rgb_planar_multiframe",
        "uncompressed_mono16",
    ],
)
def test_recompresses_accepted_sources_bit_exact(
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


def test_recompress_wraps_out_of_range_jpeg_lossless_predictions(tmp_path):
    """A decoder that clamps instead of wrapping saturates these pixels, and the
    re-encode then stores the saturated frame."""
    source = synthetic_image(bits_allocated=8, bits_stored=8)
    source_pixels = pixel_array(source, raw=True)
    source.file_meta.TransferSyntaxUID = JPEGLosslessSV1
    source.PixelData = encapsulate([_encode_sv1_with_wrapping_diffs(source_pixels)])
    source.save_as(tmp_path / "source.dcm", enforce_file_format=True)
    ds = pydicom.dcmread(tmp_path / "source.dcm")

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.RECOMPRESSED
    assert (pixel_array(ds, raw=True) == source_pixels).all()


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


def _encode_sv1_with_wrapping_diffs(pixels: np.ndarray) -> bytes:
    """Returns `pixels`, an 8-bit 2D array, as a JPEG Lossless SV1 codestream.

    Every positive difference is written as its modulo-256 negative, as Philips
    and Samsung ultrasound encoders do. Prediction plus difference then falls
    below zero, so only a decoder that wraps per T.81 H.2.1 recovers `pixels`.
    """
    rows, columns = pixels.shape
    samples = pixels.astype(int)
    # Predictor 1 takes the left neighbour; the first column takes the sample
    # above, and the first sample takes 2^(P-1).
    predictions = np.empty_like(samples)
    predictions[:, 1:] = samples[:, :-1]
    predictions[1:, 0] = samples[:-1, 0]
    predictions[0, 0] = 128
    diffs = samples - predictions
    diffs[diffs > 0] -= 256

    categories = [abs(int(diff)).bit_length() for diff in diffs.flat]
    huffman_values = sorted(set(categories))
    # A canonical table with one code per length gives the n-th category the
    # code of n-1 ones followed by a zero.
    codes = {
        category: ((1 << length) - 2, length)
        for length, category in enumerate(huffman_values, start=1)
    }
    bits = []
    for diff, category in zip(diffs.flat, categories):
        bits += _bits(*codes[category])
        # Every non-zero difference is negative, written as its one's complement.
        bits += _bits(int(diff) + (1 << category) - 1, category)
    bits += [1] * (-len(bits) % 8)
    scan = bytes(np.packbits(bits)).replace(b"\xff", b"\xff\x00")

    frame_header = struct.pack(">BHHB", 8, rows, columns, 1) + bytes([1, 0x11, 0])
    huffman_counts = [1] * len(huffman_values) + [0] * (16 - len(huffman_values))
    huffman_table = bytes([0, *huffman_counts, *huffman_values])
    scan_header = bytes([1, 1, 0x00, 1, 0, 0])
    return (
        b"\xff\xd8"
        + _segment(0xFFC3, frame_header)
        + _segment(0xFFC4, huffman_table)
        + _segment(0xFFDA, scan_header)
        + scan
        + b"\xff\xd9"
    )


def _bits(value: int, length: int) -> list[int]:
    return [(value >> shift) & 1 for shift in range(length - 1, -1, -1)]


def _segment(marker: int, body: bytes) -> bytes:
    return struct.pack(">HH", marker, len(body) + 2) + body


def _snapshot(ds: pydicom.Dataset) -> tuple:
    return (
        ds.file_meta.TransferSyntaxUID,
        ds.get("PhotometricInterpretation"),
        ds.get("PlanarConfiguration"),
        bytes(ds.PixelData),
    )
