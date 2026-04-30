"""Pixel-data redaction via mode='e'.

Reviewer-supplied redactions are blacked out in each target instance's
PixelData, then the instance is re-encoded as JPEG 2000 Lossless. An audit
record is appended to series-level metadata_fields["redactions"] so the
action is traceable later.

Output transfer syntax is fixed at JPEG 2000 Lossless regardless of the
source TS. This: (a) sidesteps pydicom auto-regenerating SOPInstanceUID for
lossy encodes (which would trip mode='e' set-changed validation), (b) lets
us assume the encoder is always available since pylibjpeg-openjpeg is a
hard dependency, and (c) matches what append.py already does on ingest, so
in practice nothing's changing format for already-stored data.
"""

import dataclasses
import datetime
from collections import defaultdict
from typing import TYPE_CHECKING, Optional, Union

import numpy as np
import pydicom3
from pydicom3.uid import JPEG2000Lossless

from cloud_optimized_dicom.bounding_box import PixelRedaction
from cloud_optimized_dicom.errors import (
    RedactionBoxOutOfBoundsError,
    RedactionFillValueError,
    RedactionFrameOutOfRangeError,
    RedactionTargetMissingError,
)

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject
    from cloud_optimized_dicom.instance import Instance

# DCM CID 7050 code for "Clean Pixel Data Option": what we record in
# DeidentificationMethodCodeSequence to flag instances whose pixels were
# scrubbed of PHI by this routine.
_DEID_METHOD_CODE = "113101"
_DEID_METHOD_DESIGNATOR = "DCM"
_DEID_METHOD_MEANING = "Clean Pixel Data Option"

# Photometric interpretations that pydicom's `pixel_array` auto-converts to
# RGB on read (the JPEG decoders return RGB for these). After decode we
# must update PhotometricInterpretation to match the decoded array, otherwise
# the re-encoded bytes would be RGB but the header would still say YBR.
_PI_AUTO_CONVERTS_TO_RGB = frozenset(
    {"YBR_FULL", "YBR_FULL_422", "YBR_ICT", "YBR_PARTIAL_420", "YBR_PARTIAL_422"}
)

FillValue = Union[int, tuple[int, ...]]


def redact_pixel_data(
    cod_object: "CODObject",
    redactions: list[PixelRedaction],
    *,
    reviewer: str,
    fill_value: Optional[FillValue] = None,
) -> None:
    """Body of `CODObject.redact_pixel_data`. See that method for docs."""
    if cod_object.mode != "e":
        raise ValueError(
            f"redact_pixel_data() requires mode='e', got mode={cod_object.mode!r}. "
            f"Edit mode is needed because redaction modifies existing pixel data in place."
        )

    instances = cod_object._get_instances(strict_sorting=False)
    redactions_by_uid = _group_by_uid(redactions, instances)

    # Pre-flight: read each affected instance's header (no pixel decode) and
    # validate every redaction against it. Raises before any pixel data is
    # decoded or written.
    for uid, uid_redactions in redactions_by_uid.items():
        ds = pydicom3.dcmread(instances[uid].dicom_uri, stop_before_pixels=True)
        _validate_redactions_against_header(uid, ds, uid_redactions, fill_value)

    # Apply pass.
    now = datetime.datetime.now(datetime.timezone.utc)
    for uid, uid_redactions in redactions_by_uid.items():
        _apply_to_instance(
            instance=instances[uid],
            redactions=uid_redactions,
            fill_value=fill_value,
            now=now,
        )

    _append_audit_record(cod_object, redactions, reviewer=reviewer, now=now)


def _group_by_uid(
    redactions: list[PixelRedaction], instances: dict
) -> dict[str, list[PixelRedaction]]:
    grouped: dict[str, list[PixelRedaction]] = defaultdict(list)
    for r in redactions:
        for uid in r.applies_to:
            if uid not in instances:
                raise RedactionTargetMissingError(
                    f"Redaction targets instance {uid!r} which is not in the series. "
                    f"(known: {sorted(instances.keys())})"
                )
            grouped[uid].append(r)
    return grouped


def _validate_redactions_against_header(
    uid: str,
    ds: pydicom3.Dataset,
    redactions: list[PixelRedaction],
    fill_value: Optional[FillValue],
) -> None:
    rows = int(ds.Rows)
    cols = int(ds.Columns)
    num_frames = int(getattr(ds, "NumberOfFrames", 1))
    samples = int(getattr(ds, "SamplesPerPixel", 1))
    photometric = str(ds.PhotometricInterpretation)

    _validate_fill_value(uid, fill_value, samples, photometric)

    for r in redactions:
        box = r.box
        if box.x < 0 or box.y < 0 or box.width <= 0 or box.height <= 0:
            raise RedactionBoxOutOfBoundsError(
                f"Instance {uid}: box {box} has non-positive width/height "
                f"or negative origin"
            )
        if box.x + box.width > cols or box.y + box.height > rows:
            raise RedactionBoxOutOfBoundsError(
                f"Instance {uid}: box {box} not contained in frame ({cols}x{rows})"
            )
        frames = r.frames if r.frames is not None else range(num_frames)
        for f in frames:
            if f < 0 or f >= num_frames:
                raise RedactionFrameOutOfRangeError(
                    f"Instance {uid}: frame index {f} out of range "
                    f"(NumberOfFrames={num_frames})"
                )


def _validate_fill_value(
    uid: str,
    fill_value: Optional[FillValue],
    samples: int,
    photometric: str,
) -> None:
    if fill_value is None:
        return
    if samples == 1:
        if not isinstance(fill_value, int):
            raise RedactionFillValueError(
                f"Instance {uid}: SamplesPerPixel=1 requires int fill_value, "
                f"got {fill_value!r}"
            )
    else:
        if not isinstance(fill_value, tuple) or len(fill_value) != samples:
            raise RedactionFillValueError(
                f"Instance {uid}: SamplesPerPixel={samples} "
                f"(PhotometricInterpretation={photometric!r}) requires a tuple "
                f"fill_value of length {samples}, got {fill_value!r}"
            )


def _derive_fill_value(samples: int, pi: str, bits_stored: int) -> FillValue:
    """Default fill is whatever displays as black for the (post-decode)
    PhotometricInterpretation. Color sources all end up as either RGB (after
    pydicom auto-converts the YBR_FULL family) or YBR_RCT, both of which
    treat (0, 0, 0) as black, so a single value covers every color path."""
    if samples == 1:
        if pi == "MONOCHROME1":
            return (1 << bits_stored) - 1
        return 0
    return (0, 0, 0)


def _apply_to_instance(
    instance: "Instance",
    redactions: list[PixelRedaction],
    fill_value: Optional[FillValue],
    now: datetime.datetime,
) -> None:
    ds = pydicom3.dcmread(instance.dicom_uri)
    original_pi = str(ds.PhotometricInterpretation)
    arr = ds.pixel_array

    num_frames = int(getattr(ds, "NumberOfFrames", 1))
    samples = int(getattr(ds, "SamplesPerPixel", 1))
    bits_stored = int(getattr(ds, "BitsStored", 8))

    # If pydicom converted YBR -> RGB on decode, the array is RGB even though
    # the header still says YBR. Reflect that on the dataset before compress
    # so the re-encoded bytes match the PhotometricInterpretation tag.
    effective_pi = "RGB" if original_pi in _PI_AUTO_CONVERTS_TO_RGB else original_pi
    if effective_pi != original_pi:
        ds.PhotometricInterpretation = effective_pi

    # pixel_array drops the leading frame axis when NumberOfFrames is 1 or
    # absent. Add a synthetic axis so the slice below works uniformly; the
    # view shares memory with arr so writes hit the original buffer.
    if num_frames == 1 and (
        (samples == 1 and arr.ndim == 2) or (samples > 1 and arr.ndim == 3)
    ):
        framed = arr[np.newaxis, ...]
    else:
        framed = arr

    fv = (
        fill_value
        if fill_value is not None
        else _derive_fill_value(samples, effective_pi, bits_stored)
    )

    for r in redactions:
        box = r.box
        frames = r.frames if r.frames is not None else range(num_frames)
        for f in frames:
            framed[f, box.y : box.y + box.height, box.x : box.x + box.width] = fv

    _stamp_deid_tags(ds, now)

    # Always normalize to JPEG 2000 Lossless (see module docstring for why).
    # Lossless leaves SOPInstanceUID alone, which mode='e' set-changed validation
    # relies on; pydicom only auto-regenerates the UID for lossy compresses.
    ds.compress(JPEG2000Lossless, arr=arr)
    ds.save_as(instance.dicom_uri)


def _stamp_deid_tags(ds: pydicom3.Dataset, now: datetime.datetime) -> None:
    """Mark the instance as having had its pixel data scrubbed of PHI.

    Three standard tags get touched. Each one is what downstream consumers
    (PACS, research collaborators, compliance audits) look at to decide
    whether the instance is safe to use.
    """

    # (0028,0301) BurnedInAnnotation: "is there PHI burned into the pixels?"
    # Downstream tools gate sharing/release on this flag. After we redact, the
    # answer is "NO"; without setting it, consumers continue to assume worst-
    # case and the redaction has no externally-observable effect.
    ds.BurnedInAnnotation = "NO"

    # (0012,0064) DeidentificationMethodCodeSequence: a list of coded entries
    # from CID 7050 documenting *which* de-id methods ran. Code 113101 ("Clean
    # Pixel Data Option") is the standard DICOM PS3.16 entry for what we just
    # did. Required to claim Basic Confidentiality Profile compliance.
    # Append rather than overwrite: upstream text/metadata de-id may have
    # already added entries we mustn't clobber.
    method_item = pydicom3.Dataset()
    method_item.CodeValue = _DEID_METHOD_CODE
    method_item.CodingSchemeDesignator = _DEID_METHOD_DESIGNATOR
    method_item.CodeMeaning = _DEID_METHOD_MEANING
    seq = list(getattr(ds, "DeidentificationMethodCodeSequence", []) or [])
    seq.append(method_item)
    ds.DeidentificationMethodCodeSequence = seq

    # (0008,0012)/(0008,0013) InstanceCreationDate/Time: stamp the instance
    # with the redaction time. Visible in any DICOM viewer (so a reviewer can
    # see "this was modified") and used by downstream caches/dedupers that
    # key on freshness.
    ds.InstanceCreationDate = now.strftime("%Y%m%d")
    ds.InstanceCreationTime = now.strftime("%H%M%S")


def _append_audit_record(
    cod_object: "CODObject",
    redactions: list[PixelRedaction],
    *,
    reviewer: str,
    now: datetime.datetime,
) -> None:
    record = {
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reviewer": reviewer,
        "entries": [dataclasses.asdict(r) for r in redactions],
    }
    existing = cod_object._get_metadata_field("redactions") or []
    cod_object.add_metadata_field(
        "redactions", existing + [record], overwrite_existing=True
    )
