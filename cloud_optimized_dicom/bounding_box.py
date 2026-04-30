"""Bounding-box dataclass for pixel-data redaction (see redact.py)."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BoundingBox:
    """Axis-aligned redaction region in raw PixelData coordinates.

    Origin top-left of the frame, x grows right, y grows down. Inclusive on
    (x, y), exclusive on (x + width, y + height). The box must be fully
    contained in (Rows, Columns) of every target frame; redact_pixel_data
    raises if not.

    Attributes:
        x, y, width, height: Pixel coordinates of the box.
        applies_to: SOP Instance UIDs (hashed if the parent CODObject uses
            hashed UIDs) the box should be applied to.
        frames: 0-indexed frame indices the box applies to. None means all
            frames of each target instance. Applies uniformly to every UID in
            applies_to; emit multiple BoundingBox objects if you need
            different frame ranges per instance.
    """

    x: int
    y: int
    width: int
    height: int
    applies_to: list[str]
    frames: Optional[list[int]] = None
