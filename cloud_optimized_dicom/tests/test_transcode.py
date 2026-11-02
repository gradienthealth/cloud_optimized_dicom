import numpy as np
import pydicom
import pydicom.uid
import pytest
from pydicom.encaps import encapsulate, generate_frames
from pydicom.filewriter import dcmwrite
from pydicom.pixels import pixel_array
from pydicom.uid import (
    ExplicitVRBigEndian,
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
    CodecParameters,
    TranscodeOutcome,
    Verification,
    VerificationDecision,
    VerificationTracker,
    recompress_to_jpeg2000_lossless,
)

_PARAMETERS = CodecParameters(
    transfer_syntax=ExplicitVRLittleEndian,
    photometric_interpretation="RGB",
    samples_per_pixel="3",
    planar_configuration="0",
    bits_allocated="8",
    bits_stored="8",
    pixel_representation="0",
)

MONO16 = {"photometric": "MONOCHROME2", "bits_allocated": 16, "bits_stored": 12}
# JPEG Lossless re-encodes only when every allocated bit is stored
MONO16_FULL = {**MONO16, "bits_stored": 16}
RGB8 = {"photometric": "RGB", "bits_allocated": 8, "bits_stored": 8}
YBR8 = {"photometric": "YBR_FULL", "bits_allocated": 8, "bits_stored": 8}


@pytest.mark.parametrize(
    ("syntax", "image", "expected_photometric"),
    [
        (JPEGLosslessSV1, MONO16_FULL, "MONOCHROME2"),
        (JPEGLossless, MONO16_FULL, "MONOCHROME2"),
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
        (JPEGLosslessSV1, {**MONO16_FULL, "number_of_frames": 3}, "MONOCHROME2"),
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
    source = synthetic_image(**MONO16_FULL)
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
    ds = _jpeg_lossless_dataset(tmp_path, **MONO16_FULL)
    del ds.PhotometricInterpretation
    before = _snapshot(ds)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_PHOTOMETRIC
    assert _snapshot(ds) == before


def test_keeps_original_when_not_smaller(tmp_path, monkeypatch):
    ds = _jpeg_lossless_dataset(tmp_path, **MONO16_FULL)
    before = _snapshot(ds)
    source_frame = pixel_array(ds, raw=True, decoding_plugin="gdcm")
    oversized = b"\0" * len(ds.PixelData)
    monkeypatch.setattr(transcode, "get_encoder", lambda uid: _StubEncoder(oversized))
    monkeypatch.setattr(transcode, "decode_jpeg2000", lambda stream: source_frame)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_NOT_SMALLER
    assert _snapshot(ds) == before


def test_keeps_original_on_verify_mismatch(tmp_path, monkeypatch):
    ds = _jpeg_lossless_dataset(tmp_path, **MONO16_FULL)
    before = _snapshot(ds)
    monkeypatch.setattr(
        transcode,
        "decode_jpeg2000",
        lambda stream: pixel_array(ds, raw=True, decoding_plugin="gdcm") + 1,
    )

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_VERIFY_MISMATCH
    assert _snapshot(ds) == before


@pytest.mark.parametrize("should_verify", [True, False], ids=["verify", "off"])
@pytest.mark.parametrize(
    "syntax",
    [ExplicitVRLittleEndian, RLELossless],
    ids=["uncompressed", "rle"],
)
def test_keeps_original_when_bits_above_bits_stored_are_set(
    tmp_path, syntax, should_verify
):
    words = _ramp_words(64 * 64)
    words[::7] |= 0x8000
    ds = _mono16_dataset(tmp_path, syntax, words, is_signed=False)
    before = _snapshot(ds)

    outcome = recompress_to_jpeg2000_lossless(ds, should_verify=should_verify)

    assert outcome is TranscodeOutcome.PASSTHROUGH_HIGH_BITS
    assert _snapshot(ds) == before


@pytest.mark.parametrize("syntax", [JPEGLossless, JPEGLosslessSV1], ids=["p14", "sv1"])
def test_keeps_jpeg_lossless_coded_beyond_bits_stored_unchecked(tmp_path, syntax):
    """GDCM codes 16 bits of precision for these 12-bit samples."""
    ds = _mono16_dataset(tmp_path, syntax, _ramp_words(64 * 64), is_signed=False)
    before = _snapshot(ds)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_HIGH_BITS_UNCHECKED
    assert _snapshot(ds) == before


def test_recompresses_jpeg_lossless_coded_within_bits_stored_bit_exact(tmp_path):
    ds = _with_frame_header_bytes(_mono16_jpeg_lossless(tmp_path), {4: 12})
    source_pixels = pixel_array(ds, raw=True, decoding_plugin="gdcm")

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.RECOMPRESSED
    assert (pixel_array(ds, raw=True) == source_pixels).all()


def test_keeps_jpeg_lossless_with_a_later_frame_beyond_bits_stored(tmp_path):
    ds = _mono16_jpeg_lossless(tmp_path, number_of_frames=2)
    ds = _with_frame_header_bytes(ds, {4: 12}, frame_index=0)
    before = _snapshot(ds)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_HIGH_BITS_UNCHECKED
    assert _snapshot(ds) == before


def test_reads_the_precision_past_a_fill_byte(tmp_path):
    ds = _with_frame_header_bytes(_mono16_jpeg_lossless(tmp_path), {4: 12})
    ds = _with_bytes_after_start_of_image(ds, b"\xff")

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.RECOMPRESSED


def test_stops_reading_the_precision_at_the_start_of_scan(tmp_path):
    ds = _with_frame_header_bytes(_mono16_jpeg_lossless(tmp_path), {4: 12})
    ds = _with_bytes_after_start_of_image(ds, b"\xff\xda\x00\x02")
    before = _snapshot(ds)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_HIGH_BITS_UNCHECKED
    assert _snapshot(ds) == before


def test_keeps_jpeg_lossless_without_a_readable_precision_unchecked(tmp_path):
    ds = _with_frame_header_bytes(_mono16_jpeg_lossless(tmp_path), {1: 0xC7})
    before = _snapshot(ds)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_HIGH_BITS_UNCHECKED
    assert _snapshot(ds) == before


@pytest.mark.parametrize(
    ("overlay_bit", "expected"),
    [
        (0, TranscodeOutcome.RECOMPRESSED),
        (0x8000, TranscodeOutcome.PASSTHROUGH_HIGH_BITS),
    ],
    ids=["clean", "overlay"],
)
def test_checks_big_endian_words_in_their_own_byte_order(
    tmp_path, overlay_bit, expected
):
    words = _ramp_words(64 * 64)
    words[::7] |= overlay_bit
    image = synthetic_image(**MONO16)
    image.PixelData = words.astype(">u2").tobytes()
    image.file_meta.TransferSyntaxUID = ExplicitVRBigEndian
    path = str(tmp_path / "big_endian.dcm")
    dcmwrite(path, image, enforce_file_format=True)

    outcome = recompress_to_jpeg2000_lossless(pydicom.dcmread(path))

    assert outcome is expected


def test_recompresses_unsigned_words_with_clear_high_bits(tmp_path):
    ds = _mono16_dataset(
        tmp_path, ExplicitVRLittleEndian, _ramp_words(64 * 64), is_signed=False
    )

    assert recompress_to_jpeg2000_lossless(ds) is TranscodeOutcome.RECOMPRESSED


@pytest.mark.parametrize(
    "high_bits", ["sign_extended", "clear"], ids=["sign_extended", "clear"]
)
def test_recompresses_signed_samples_at_the_bits_stored_boundaries(tmp_path, high_bits):
    values = np.resize(np.array([-2048, -2047, -1, 0, 1, 2047], dtype="<i2"), 64 * 64)
    words = _signed_words(values, high_bits)
    ds = _mono16_dataset(tmp_path, ExplicitVRLittleEndian, words, is_signed=True)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.RECOMPRESSED
    assert (pixel_array(ds).ravel() == values).all()


def test_keeps_signed_samples_whose_high_bits_are_not_the_sign(tmp_path):
    values = np.resize(np.array([-2048, -1, 0, 2047], dtype="<i2"), 64 * 64)
    words = _signed_words(values, "clear")
    words[1] |= 0x4000
    ds = _mono16_dataset(tmp_path, ExplicitVRLittleEndian, words, is_signed=True)

    outcome = recompress_to_jpeg2000_lossless(ds)

    assert outcome is TranscodeOutcome.PASSTHROUGH_HIGH_BITS


@pytest.mark.parametrize(
    ("should_verify", "expected"),
    [
        (True, TranscodeOutcome.PASSTHROUGH_VERIFY_MISMATCH),
        (False, TranscodeOutcome.RECOMPRESSED),
    ],
    ids=["verify", "off"],
)
def test_checks_frames_with_the_injected_decoder(tmp_path, should_verify, expected):
    ds = _jpeg_lossless_dataset(tmp_path, **MONO16_FULL)

    outcome = recompress_to_jpeg2000_lossless(
        ds, should_verify=should_verify, decoder=_corrupting_decode
    )

    assert outcome is expected


@pytest.mark.parametrize(
    ("verification", "expected_before", "expected_after"),
    [
        (Verification.OFF, VerificationDecision.TRUST, VerificationDecision.TRUST),
        (
            Verification.PER_SERIES,
            VerificationDecision.VERIFY,
            VerificationDecision.TRUST,
        ),
        (Verification.FULL, VerificationDecision.VERIFY, VerificationDecision.VERIFY),
    ],
    ids=["off", "per_series", "full"],
)
def test_tracker_stops_verifying_once_a_verified_re_encode_completes(
    verification, expected_before, expected_after
):
    verification_tracker = VerificationTracker(verification)
    before = verification_tracker.decide(_PARAMETERS)

    verification_tracker.record(_PARAMETERS, TranscodeOutcome.RECOMPRESSED)

    assert before is expected_before
    assert verification_tracker.decide(_PARAMETERS) is expected_after


@pytest.mark.parametrize(
    ("verification", "expected"),
    [
        (Verification.OFF, VerificationDecision.TRUST),
        (Verification.PER_SERIES, VerificationDecision.SKIP),
        (Verification.FULL, VerificationDecision.VERIFY),
    ],
    ids=["off", "per_series", "full"],
)
def test_tracker_skips_after_a_failure_under_per_series_only(verification, expected):
    verification_tracker = VerificationTracker(verification)

    verification_tracker.record(
        _PARAMETERS, TranscodeOutcome.PASSTHROUGH_VERIFY_MISMATCH
    )

    assert verification_tracker.decide(_PARAMETERS) is expected


def test_tracker_keeps_each_set_of_codec_parameters_apart():
    """A failed RGB capture leaves the signed slices to their own check."""
    signed_slices = _PARAMETERS._replace(pixel_representation="1")
    verification_tracker = VerificationTracker(Verification.PER_SERIES)

    verification_tracker.record(
        _PARAMETERS, TranscodeOutcome.PASSTHROUGH_VERIFY_MISMATCH
    )

    assert verification_tracker.decide(_PARAMETERS) is VerificationDecision.SKIP
    assert verification_tracker.decide(signed_slices) is VerificationDecision.VERIFY


def test_codec_parameters_tolerate_a_header_without_them():
    ds = pydicom.Dataset()
    ds.file_meta = pydicom.dataset.FileMetaDataset()

    assert CodecParameters.from_dataset(ds) == CodecParameters(*[""] * 7)


def test_keeps_original_when_encoder_fails(tmp_path, monkeypatch):
    ds = _jpeg_lossless_dataset(tmp_path, **MONO16_FULL)
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


def _corrupting_decode(stream) -> np.ndarray:
    """Stands in for the JPEG 2000 decoder with a frame that never matches."""
    return np.zeros((1, 1), dtype=np.uint16)


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


def _mono16_dataset(
    tmp_path, syntax, words: np.ndarray, is_signed: bool
) -> pydicom.Dataset:
    image = synthetic_image(**MONO16, is_signed=is_signed)
    image.PixelData = words.astype("<u2").tobytes()
    return pydicom.dcmread(write_encoded_image(tmp_path / "source.dcm", syntax, image))


def _mono16_jpeg_lossless(tmp_path, number_of_frames: int = 1) -> pydicom.Dataset:
    image = synthetic_image(**MONO16, number_of_frames=number_of_frames)
    path = write_encoded_image(tmp_path / "source.dcm", JPEGLosslessSV1, image)
    return pydicom.dcmread(path)


def _ramp_words(count: int) -> np.ndarray:
    return (np.arange(count) % 4096).astype("<u2")


def _signed_words(values: np.ndarray, high_bits: str) -> np.ndarray:
    """Returns 12-bit signed `values` as 16-bit words, high bits as named."""
    words = values.view("<u2").copy()
    return words & 0x0FFF if high_bits == "clear" else words


def _with_bytes_after_start_of_image(
    ds: pydicom.Dataset, inserted: bytes
) -> pydicom.Dataset:
    """Inserts `inserted` after the SOI marker of a single-frame codestream."""
    frame = next(generate_frames(ds.PixelData, number_of_frames=1))
    ds.PixelData = encapsulate([frame[:2] + inserted + frame[2:]])
    return ds


def _with_frame_header_bytes(
    ds: pydicom.Dataset, replacements: dict[int, int], frame_index: int = 0
) -> pydicom.Dataset:
    """Rewrites bytes of one frame's SOF3 segment, which GDCM puts after SOI.

    Offset 1 is the marker code; offset 4 is the sample precision.
    """
    number_of_frames = int(ds.get("NumberOfFrames", 1))
    frames = [
        bytearray(frame)
        for frame in generate_frames(ds.PixelData, number_of_frames=number_of_frames)
    ]
    for offset, value in replacements.items():
        frames[frame_index][2 + offset] = value
    ds.PixelData = encapsulate([bytes(frame) for frame in frames])
    return ds


def _snapshot(ds: pydicom.Dataset) -> tuple:
    return (
        ds.file_meta.TransferSyntaxUID,
        ds.get("PhotometricInterpretation"),
        ds.get("PlanarConfiguration"),
        bytes(ds.PixelData),
    )
