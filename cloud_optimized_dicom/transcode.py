"""Re-encodes pixel data as JPEG 2000 Lossless, one frame at a time.

The re-encode takes these sources:

- the legacy lossless encodings in `RECOMPRESS_SOURCE_SYNTAXES`, which JPEG
  2000 stores more compactly;
- uncompressed RGB, which shrinks further once the codec's reversible colour
  transform runs;
- every other uncompressed photometric interpretation the encoder reproduces
  exactly.

Each frame is decoded and encoded again, and can be decoded once more to prove
it matches the source. The result is kept only when it comes out smaller and
every checked frame matches; otherwise the instance keeps its original bytes.
"""

import enum
import io
from itertools import islice
from typing import Callable, NamedTuple

import numpy as np
import pydicom
import pydicom.uid
from openjpeg import decode as decode_jpeg2000
from pydicom.encaps import encapsulate, encapsulate_extended, generate_frames
from pydicom.pixels import as_pixel_options, get_encoder, iter_pixels
from pydicom.valuerep import VR

from cloud_optimized_dicom.config import logger

# The compressed encodings worth re-encoding. Each is lossless, so the pixel
# values survive, and each stores them less compactly than JPEG 2000. Lossy
# encodings must never be re-encoded. JPEG 2000 Lossless (.90) is already the
# target. JPEG-LS Lossless and the reversible half of JPEG 2000 (.91) stay out
# because JPEG 2000 Lossless encodes them to about the same size (the PROC-2531
# compression study), and .91 mixes reversible and irreversible codestreams
# under one transfer syntax.
RECOMPRESS_SOURCE_SYNTAXES = frozenset(
    {
        pydicom.uid.JPEGLossless,
        pydicom.uid.JPEGLosslessSV1,
        pydicom.uid.RLELossless,
    }
)

# JPEG Lossless decodes with GDCM: pylibjpeg-libjpeg clamps out-of-range
# predictor reconstructions instead of wrapping modulo 65536 as ITU T.81 H.2.1
# requires, saturating samples in streams whose encoder relies on the wrap
# (common in Philips and Samsung ultrasound; pydicom/pylibjpeg-libjpeg#90).
_GDCM_DECODED_SYNTAXES = frozenset(
    {pydicom.uid.JPEGLossless, pydicom.uid.JPEGLosslessSV1}
)

# The photometric interpretations the JPEG 2000 Lossless encoder reproduces
# exactly from a raw decode. YBR_FULL_422 is excluded: decoders upsample its
# chroma on the way out, so no re-encode can match the stored samples.
_RECOMPRESSIBLE_PHOTOMETRIC_INTERPRETATIONS = frozenset(
    {"MONOCHROME1", "MONOCHROME2", "PALETTE COLOR", "RGB", "YBR_FULL"}
)


# The JPEG marker bytes the frame-header walk reads. Every marker opens with
# the prefix, SOF3 is the lossless frame header, and SOS starts the scan data.
_JPEG_MARKER_PREFIX = 0xFF
_JPEG_SOF3 = 0xC3
_JPEG_SOS = 0xDA

# Offsets and lengths for an extended offset table, produced only when the
# encapsulated frames outgrow the basic table's 32-bit offsets.
_ExtendedOffsets = tuple[bytes, bytes]


# A function that decodes a JPEG 2000 codestream to its pixel array
_Decoder = Callable[[io.BytesIO], np.ndarray]


class CodecParameters(NamedTuple):
    """The header values that drive the JPEG 2000 encoder, as strings.

    Missing or malformed values become strings too, so any header gives a key.
    """

    transfer_syntax: str
    photometric_interpretation: str
    samples_per_pixel: str
    planar_configuration: str
    bits_allocated: str
    bits_stored: str
    pixel_representation: str

    @classmethod
    def from_dataset(cls, ds: pydicom.Dataset) -> "CodecParameters":
        """Reads the parameters from the header of `ds`."""
        return cls(
            transfer_syntax=str(ds.file_meta.get("TransferSyntaxUID", "")),
            photometric_interpretation=str(ds.get("PhotometricInterpretation", "")),
            samples_per_pixel=str(ds.get("SamplesPerPixel", "")),
            planar_configuration=str(ds.get("PlanarConfiguration", "")),
            bits_allocated=str(ds.get("BitsAllocated", "")),
            bits_stored=str(ds.get("BitsStored", "")),
            pixel_representation=str(ds.get("PixelRepresentation", "")),
        )


class TranscodeOutcome(enum.Enum):
    """What `recompress_to_jpeg2000_lossless` did with a dataset."""

    PASSTHROUGH_FAILED = "passthrough_failed"
    PASSTHROUGH_HIGH_BITS = "passthrough_high_bits"
    PASSTHROUGH_HIGH_BITS_UNCHECKED = "passthrough_high_bits_unchecked"
    PASSTHROUGH_NOT_SMALLER = "passthrough_not_smaller"
    PASSTHROUGH_PHOTOMETRIC = "passthrough_photometric"
    PASSTHROUGH_SERIES_VERIFY_MISMATCH = "passthrough_series_verify_mismatch"
    PASSTHROUGH_SYNTAX = "passthrough_syntax"
    PASSTHROUGH_VERIFY_MISMATCH = "passthrough_verify_mismatch"
    RECOMPRESSED = "recompressed"


class Verification(enum.StrEnum):
    """How much of the re-encode `CODObject.append` decodes back to check.

    A checked frame is decoded from the new JPEG 2000 codestream and compared
    with the frame decoded from the source, so the check catches an encoder
    that loses data but not a decoder that misreads the source. Each checked
    frame costs one extra JPEG 2000 decode.

    - `FULL` checks every frame of every re-encoded instance.
    - `PER_SERIES` checks every frame of the first instance in each append
      whose re-encode completes, separately for each set of `CodecParameters`.
      If that instance does not round-trip, the later instances with its
      parameters keep their original bytes; once it passes, they are encoded
      without the check.
    - `OFF` checks nothing.

    Every level keeps the other safeguards: the high-bit guard, the rule that a
    re-encode not smaller than the source is discarded, and the fallback to the
    original bytes when a codec raises.
    """

    FULL = "full"
    OFF = "off"
    PER_SERIES = "per-series"


class VerificationDecision(enum.Enum):
    """What `VerificationTracker.decide` asks of the next re-encode."""

    SKIP = "skip"
    TRUST = "trust"
    VERIFY = "verify"


class VerificationTracker:
    """Decides how to re-encode each instance under one `Verification` level.

    Under `PER_SERIES` it remembers, for each set of `CodecParameters`, whether
    a verified re-encode completed or failed to round-trip.
    """

    def __init__(self, verification: Verification) -> None:
        self._verification = verification
        self._verified: set[CodecParameters] = set()
        self._failed: set[CodecParameters] = set()

    def decide(self, parameters: CodecParameters) -> VerificationDecision:
        """Returns whether to skip, verify or trust a re-encode."""
        if parameters in self._failed:
            return VerificationDecision.SKIP
        if self._verification is Verification.FULL or (
            self._verification is Verification.PER_SERIES
            and parameters not in self._verified
        ):
            return VerificationDecision.VERIFY
        return VerificationDecision.TRUST

    def record(self, parameters: CodecParameters, outcome: TranscodeOutcome) -> None:
        """Records the outcome of a re-encode with `parameters`."""
        if self._verification is not Verification.PER_SERIES:
            return
        if outcome is TranscodeOutcome.RECOMPRESSED:
            self._verified.add(parameters)
        elif outcome is TranscodeOutcome.PASSTHROUGH_VERIFY_MISMATCH:
            self._failed.add(parameters)


def recompress_to_jpeg2000_lossless(
    ds: pydicom.Dataset,
    *,
    should_verify: bool = True,
    decoder: _Decoder | None = None,
) -> TranscodeOutcome:
    """Re-encodes a dataset's pixel data as JPEG 2000 Lossless.

    The source must be uncompressed or in `RECOMPRESS_SOURCE_SYNTAXES`, and
    carry a photometric interpretation the encoder reproduces exactly; anything
    else passes through. Each frame is decoded and encoded. With
    `should_verify`, the new codestream is decoded again and compared with the
    decoded source frame; the comparison catches an encoder that loses data,
    not a decoder that misreads the source. The re-encode replaces the original
    only when every checked frame matches and the encapsulated pixel data is
    smaller. Any other outcome, including an exception from a codec, leaves
    `ds` untouched, so incompressible data and pixels the codec cannot encode,
    such as 1-bit and 32-bit samples, keep their bytes.

    Decoding discards the bits above `BitsStored`, and JPEG 2000 stores only
    `BitsStored` bits, so a source that uses them (overlays kept in the high
    bits, for instance) would lose them without any frame failing the check.
    Such a source passes through as `PASSTHROUGH_HIGH_BITS` whether or not
    `should_verify` is set. Signed samples whose high bits only extend the
    sign, or are zero, carry no extra information and are re-encoded.

    JPEG Lossless decodes with those bits already masked, so the check cannot
    see them there. A JPEG Lossless frame coded with no more sample precision
    than `BitsStored` cannot hold them. A source with any frame coded with more
    precision, or whose precision cannot be read, passes through as
    `PASSTHROUGH_HIGH_BITS_UNCHECKED`.

    RGB is stored as `YBR_RCT`, the interpretation under which the codec
    applies its reversible colour transform; every other interpretation is
    kept. Multi-sample output is colour-by-pixel (`PlanarConfiguration` 0),
    whatever the source's layout. The SOP Instance UID and the
    lossy-compression tags never change.

    Args:
        ds: Dataset with `PixelData`, uncompressed or encapsulated.
        should_verify: Whether to decode every re-encoded frame back and
            compare it.
        decoder: Decodes a JPEG 2000 codestream for the check. Defaults to
            openjpeg's decoder.

    Returns:
        The outcome. `ds` is modified only for `TranscodeOutcome.RECOMPRESSED`.
    """
    source_syntax = ds.file_meta.TransferSyntaxUID
    if source_syntax.is_compressed and source_syntax not in RECOMPRESS_SOURCE_SYNTAXES:
        return TranscodeOutcome.PASSTHROUGH_SYNTAX
    source_photometric = ds.get("PhotometricInterpretation")
    if source_photometric not in _RECOMPRESSIBLE_PHOTOMETRIC_INTERPRETATIONS:
        return TranscodeOutcome.PASSTHROUGH_PHOTOMETRIC
    # RGB is stored as YBR_RCT so the codec applies its reversible colour
    # transform, which openjpeg enables only for that interpretation. Every
    # other interpretation keeps its samples as decoded.
    target_photometric = (
        "YBR_RCT" if source_photometric == "RGB" else source_photometric
    )

    try:
        if _may_hide_high_bits(ds):
            return TranscodeOutcome.PASSTHROUGH_HIGH_BITS_UNCHECKED
        checking_decoder = (decoder or decode_jpeg2000) if should_verify else None
        encoded = _encode_frames(ds, target_photometric, decoder=checking_decoder)
    # Any codec or pydicom failure means the original bytes stay, so every
    # exception type ends in the same place.
    except Exception as error:
        logger.warning(
            f"Keeping {source_syntax.name} pixel data; re-encode failed: {error}"
        )
        return TranscodeOutcome.PASSTHROUGH_FAILED

    if isinstance(encoded, TranscodeOutcome):
        return encoded
    pixel_data, extended_offsets = _encapsulate(encoded)
    if len(pixel_data) >= len(ds.PixelData):
        return TranscodeOutcome.PASSTHROUGH_NOT_SMALLER
    _replace_pixel_data(ds, pixel_data, extended_offsets, target_photometric)
    return TranscodeOutcome.RECOMPRESSED


def _encapsulate(
    codestreams: list[bytes],
) -> tuple[bytes, _ExtendedOffsets | None]:
    """Encapsulates the frames; large totals get an extended offset table."""
    basic_table_span = (len(codestreams) - 1) * 8 + sum(
        len(codestream) for codestream in codestreams[:-1]
    )
    if basic_table_span > 2**32 - 1:
        pixel_data, offsets, lengths = encapsulate_extended(codestreams)
        return pixel_data, (offsets, lengths)
    return encapsulate(codestreams), None


def _encode_frames(
    ds: pydicom.Dataset, target_photometric: str, *, decoder: _Decoder | None
) -> list[bytes] | TranscodeOutcome:
    """Decodes and re-encodes every frame, checking each with `decoder`.

    A `decoder` of `None` skips the check.

    Returns the codestreams, or the outcome that stopped the loop:
    `PASSTHROUGH_HIGH_BITS` at a frame that uses bits above `BitsStored`,
    `PASSTHROUGH_VERIFY_MISMATCH` at a frame that does not decode back, or
    `PASSTHROUGH_NOT_SMALLER` once the encapsulated items reach the size of the
    source pixel data.
    """
    encoder = get_encoder(pydicom.uid.JPEG2000Lossless)
    options = as_pixel_options(ds)
    options["photometric_interpretation"] = target_photometric
    options["number_of_frames"] = 1
    if options["samples_per_pixel"] > 1:
        options["planar_configuration"] = 0
    decoding_plugin = (
        "gdcm" if ds.file_meta.TransferSyntaxUID in _GDCM_DECODED_SYNTAXES else ""
    )
    # `raw=True` skips pydicom's YBR to RGB conversion, which rounds.
    # `correct_unused_bits=False` keeps the bits above `BitsStored` for
    # `_without_unused_bits` to check. The clamp to `NumberOfFrames` drops the
    # extra fragment groups pydicom yields for producers that write more items
    # than frames.
    frames = iter_pixels(
        ds, raw=True, decoding_plugin=decoding_plugin, correct_unused_bits=False
    )
    bits_stored = int(ds.BitsStored)
    is_signed = int(ds.PixelRepresentation) == 1
    number_of_frames = int(ds.get("NumberOfFrames", 1) or 1)
    source_length = len(ds.PixelData)
    codestreams = []
    items_length = 0
    for stored_frame in islice(frames, number_of_frames):
        frame = _without_unused_bits(stored_frame, bits_stored, is_signed)
        if frame is None:
            logger.warning("Keeping pixel data; bits above BitsStored are set")
            return TranscodeOutcome.PASSTHROUGH_HIGH_BITS
        codestream = encoder.encode(frame, encoding_plugin="pylibjpeg", **options)
        if decoder is not None and not _round_trips(codestream, frame, decoder):
            return TranscodeOutcome.PASSTHROUGH_VERIFY_MISMATCH
        codestreams.append(codestream)
        # An 8-byte item header plus the codestream, padded to an even length
        items_length += 8 + len(codestream) + len(codestream) % 2
        if items_length >= source_length:
            return TranscodeOutcome.PASSTHROUGH_NOT_SMALLER
    return codestreams


def _may_hide_high_bits(ds: pydicom.Dataset) -> bool:
    """Whether a JPEG Lossless frame may hold bits that GDCM masks unseen."""
    bits_stored = int(ds.BitsStored)
    if (
        ds.file_meta.TransferSyntaxUID not in _GDCM_DECODED_SYNTAXES
        or bits_stored >= int(ds.BitsAllocated)
    ):
        return False
    number_of_frames = int(ds.get("NumberOfFrames", 1) or 1)
    frames = generate_frames(ds.PixelData, number_of_frames=number_of_frames)
    for frame in islice(frames, number_of_frames):
        precision = _jpeg_sample_precision(frame)
        if precision is None or precision > bits_stored:
            return True
    return False


def _jpeg_sample_precision(codestream: bytes) -> int | None:
    """Returns the sample precision in a JPEG Lossless frame header (SOF3).

    Returns `None` when the codestream has no frame header where one belongs.
    """
    position = 2  # past the SOI marker
    while (
        position + 5 <= len(codestream) and codestream[position] == _JPEG_MARKER_PREFIX
    ):
        marker = codestream[position + 1]
        if marker == _JPEG_MARKER_PREFIX:  # a fill byte before the marker
            position += 1
            continue
        if marker == _JPEG_SOF3:
            return codestream[position + 4]
        if marker == _JPEG_SOS:  # scan data follows, so no frame header
            return None
        segment_length = int.from_bytes(codestream[position + 2 : position + 4], "big")
        position += 2 + segment_length
    return None


def _replace_pixel_data(
    ds: pydicom.Dataset,
    pixel_data: bytes,
    extended_offsets: _ExtendedOffsets | None,
    target_photometric: str,
) -> None:
    ds.PixelData = pixel_data
    # PS3.5 Annex A.4 and Section 8.2: encapsulated pixel data is OB with
    # undefined length.
    element = ds["PixelData"]
    element.is_undefined_length = True
    element.VR = VR.OB
    for keyword in ("ExtendedOffsetTable", "ExtendedOffsetTableLengths"):
        if keyword in ds:
            delattr(ds, keyword)
    if extended_offsets is not None:
        ds.ExtendedOffsetTable, ds.ExtendedOffsetTableLengths = extended_offsets
    ds.file_meta.TransferSyntaxUID = pydicom.uid.JPEG2000Lossless
    ds.PhotometricInterpretation = target_photometric
    if ds.SamplesPerPixel > 1:
        ds.PlanarConfiguration = 0


def _round_trips(codestream: bytes, frame: np.ndarray, decoder: _Decoder) -> bool:
    """Whether `codestream` decodes back to exactly `frame`."""
    decoded = decoder(io.BytesIO(codestream))
    if decoded.shape == frame.shape and np.array_equal(decoded, frame):
        return True
    logger.warning(
        f"Keeping pixel data; decoded frame {decoded.shape} {decoded.dtype} "
        f"differs from source {frame.shape} {frame.dtype}"
    )
    return False


def _without_unused_bits(
    frame: np.ndarray, bits_stored: int, is_signed: bool
) -> np.ndarray | None:
    """Clears or sign-extends the bits above `bits_stored`, as pydicom does.

    Returns `None` when any of those bits are set, other than as the sign
    extension of a signed sample.
    """
    unused_bits = frame.dtype.itemsize * 8 - bits_stored
    if unused_bits <= 0:
        return frame
    frame = frame.astype(frame.dtype.newbyteorder("="), copy=False)
    corrected = (frame << unused_bits) >> unused_bits
    clear = (frame.view(f"u{frame.dtype.itemsize}") >> bits_stored) == 0
    if not np.all(clear | (frame == corrected) if is_signed else clear):
        return None
    return corrected
