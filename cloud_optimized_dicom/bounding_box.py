"""Geometry and pixel-redaction request types (consumed by redact.py)."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BoundingBox:
    """Axis-aligned rectangle in raw PixelData coordinates.

    Origin top-left of the frame, x grows right, y grows down. Bounds are
    inclusive on (x, y) and exclusive on (x + width, y + height).
    """

    x: int
    y: int
    width: int
    height: int


@dataclass(frozen=True)
class PixelRedaction:
    """A request to black out a region of pixel data on one or more instances.

    Attributes:
        box: Rectangle to redact, in PixelData coordinates of the target frames.
        applies_to: SOP Instance UIDs (hashed if the parent CODObject uses
            hashed UIDs) the box should be applied to.
        frames: 0-indexed frame indices to apply the box to. None means all
            frames of each target instance. Applies uniformly to every UID in
            applies_to; emit multiple PixelRedaction objects if you need
            different frame ranges per instance.
    """

    box: BoundingBox
    applies_to: list[str]
    frames: Optional[list[int]] = None
