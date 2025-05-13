from typing import Tuple

import cv2
import ffmpeg
import numpy as np
from google.cloud import storage

DEFAULT_FPS = 4
DEFAULT_QUALITY = 60
DEFAULT_SIZE = 128


# Utility functions having to do with converting a numpy array of pixel data into jpgs and mp4s
def _convert_frame_to_jpg(frame: np.ndarray, output_path: str):
    print(f"converting frame to jpg at {output_path}")
    # Normalize and convert frame to uint8
    frame_uint8 = cv2.normalize(frame, None, 255, 0, cv2.NORM_MINMAX, cv2.CV_8U)
    cv2.imwrite(output_path, frame_uint8)


def _convert_frames_to_mp4(
    frames: list[np.ndarray], output_path: str, fps: int = DEFAULT_FPS
):
    """Convert `frames` to an mp4 and save to `output_path`"""
    if not frames:
        raise ValueError("Frame list is empty.")

    # Assume all frames are the same shape
    height, width = frames[0].shape[:2]
    if any(frame.shape[:2] != (height, width) for frame in frames):
        raise ValueError("All frames must have the same shape.")

    # if any frames are color, we must write a color video
    thumbnail_is_color = any(len(frame.shape) > 2 for frame in frames)

    # Initialize the video writer, using XVID codec
    out = cv2.VideoWriter(
        filename=output_path,
        fourcc=cv2.VideoWriter_fourcc(*"avc1"),
        fps=fps,
        frameSize=(width, height),
        isColor=thumbnail_is_color,
    )

    def _process_frame(frame: np.ndarray) -> np.ndarray:
        """For color thumbnails, convert frame to BGR format. No conversion is necessary for grayscale thumbnails.
        After formatting, normalize the frame (0-255), set data type to uint8, and return.
        """
        if thumbnail_is_color:
            if len(frame.shape) == 2:
                # Convert grayscale frame to BGR
                frame = cv2.cvtColor(frame, cv2.COLOR_GRAY2BGR)
            elif frame.shape[2] == 3:
                # Assume frame shape of 3 -> standard RGB
                frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
            elif frame.shape[2] == 4:
                # Assume frame shape of 4 -> RGBA
                frame = cv2.cvtColor(frame, cv2.COLOR_RGBA2BGR)
        elif len(frame.shape) > 2:
            # no conversion is necessary for grayscale frames in a grayscale thumbnail
            raise ValueError(
                f"Unsupported frame shape for grayscale thumbnail: {frame.shape}"
            )
        return cv2.normalize(frame, None, 255, 0, cv2.NORM_MINMAX, cv2.CV_8U)

    for frame in frames:
        out.write(_process_frame(frame))

    out.release()
    print(f"Saved MP4 to: {output_path}")


def _generate_thumbnail_frame_and_anchors(
    pixel_array: np.ndarray,
) -> Tuple[np.ndarray, dict]:
    """
    Given a DICOM pixel array from pydicom.pixels.iter_pixels, create a thumbnail and record
    the mapping information between original and thumbnail coordinates.

    Args:
        pixel_array: A numpy array from pydicom.pixels.iter_pixels, either (rows, columns) for
                    single sample data or (rows, columns, samples) for multi-sample data

    Returns:
        Tuple containing:
        - The thumbnail as a numpy array (always DEFAULT_SIZE x DEFAULT_SIZE)
        - A dictionary of anchor points mapping between original and thumbnail coordinates
    """
    # Get original dimensions
    height, width = pixel_array.shape[:2]

    # Calculate scaling factor to fit the longer dimension to DEFAULT_SIZE
    scale = DEFAULT_SIZE / max(height, width)

    # Calculate new dimensions while maintaining aspect ratio
    new_height = int(height * scale)
    new_width = int(width * scale)

    # Resize the image using cv2
    resized = cv2.resize(
        pixel_array, (new_width, new_height), interpolation=cv2.INTER_AREA
    )

    # Create a black square canvas of size DEFAULT_SIZE x DEFAULT_SIZE
    if len(pixel_array.shape) == 2:  # Grayscale
        thumbnail = np.zeros((DEFAULT_SIZE, DEFAULT_SIZE), dtype=pixel_array.dtype)
    else:  # Multi-sample (e.g., RGB)
        thumbnail = np.zeros(
            (DEFAULT_SIZE, DEFAULT_SIZE, pixel_array.shape[2]), dtype=pixel_array.dtype
        )

    # Calculate position to paste the resized image (centered)
    y_offset = (DEFAULT_SIZE - new_height) // 2
    x_offset = (DEFAULT_SIZE - new_width) // 2

    # Place the resized image in the center of the square
    thumbnail[y_offset : y_offset + new_height, x_offset : x_offset + new_width] = (
        resized
    )

    # Calculate the mapping between original and thumbnail coordinates
    anchors = {
        "original_size": {"width": width, "height": height},
        "thumbnail_upper_left": {"row": y_offset, "col": x_offset},
        "thumbnail_bottom_right": {
            "row": y_offset + new_height,
            "col": x_offset + new_width,
        },
        "scale_factor": scale,
    }

    return thumbnail, anchors
