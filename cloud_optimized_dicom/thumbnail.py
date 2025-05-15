import logging
import os
from typing import TYPE_CHECKING

import numpy as np
import pydicom3
from google.cloud import storage

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import upload_and_count_file

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject

logger = logging.getLogger(__name__)

SORTING_ATTRIBUTES = {"InstanceNumber": "00200013", "SliceLocation": "00201041"}


class ThumbnailError(Exception):
    """Error generating thumbnail."""


class SeriesMissingPixelDataError(ThumbnailError):
    """Series has no pixel data."""


class NoExtractablePixelDataError(ThumbnailError):
    """Series has pixel data, but we failed to extract any of it."""


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


def _sort_instances(instances: list[Instance]) -> list[Instance]:
    """Attempt to sort instances by instance_number tag. Try slice_location if that fails.
    If both fail, return the instances in the order they were fetched, and log a warning.
    """
    # if there's only one instance, return it as is
    if len(instances) <= 1:
        return instances
    # attempt to sort by by each attribute in SORTING_ATTRIBUTES
    for tag in SORTING_ATTRIBUTES.values():
        # do not attempt sorting if any instances are missing the tag
        if any(tag not in instance.metadata for instance in instances):
            continue
        # sortable attributes are expected to be stored in metadata as "tag": {"vr":"VR","Value":[some_value]}
        return sorted(instances, key=lambda x: x.metadata[tag]["Value"][0])
    # if no sorting was successful, return the instances in the order they were fetched
    logger.warning(
        f"Unable to sort instances by any known sorting attributes ({', '.join(SORTING_ATTRIBUTES.keys())})"
    )
    return instances


def _remove_instances_without_pixeldata(
    cod_obj: "CODObject", instances: list[Instance]
) -> list[Instance]:
    """Remove instances that do not have pixel data. Raise an error if no instances have pixel data."""
    num_instances = len(instances)
    instances = [instance for instance in instances if instance.has_pixeldata]
    if len(instances) == 0:
        metrics.SERIES_MISSING_PIXEL_DATA.inc()
        raise SeriesMissingPixelDataError(
            f"None of the {num_instances} instances have pixel data for cod object {cod_obj}"
        )
    return instances


def _generate_thumbnail_frames(
    cod_obj: "CODObject",
    instances: list[Instance],
    instance_to_instance_uid: dict[Instance, str],
):
    """Iterate through instances and generate thumbnail frames.

    Returns:
        all_frames: list of thumbnail frames, in the form of raw numpy ndarrays
        thumbnail_instance_metadata: dict mapping instance uids to metadata for all frames in the instance
        thumbnail_index_to_instance_frame: convenience list mapping thumbnail index to instance uid and frame index
        (i.e. `thumbnail_index_to_instance_frame[4] = (some_uid, 0)` means the 5th thumbnail frame = 1st frame of instance `some_uid`)
    """
    all_frames = []
    thumbnail_instance_metadata = {}
    thumbnail_index_to_instance_frame = []
    for instance in instances:
        with instance.open() as f:
            instance_uid = instance_to_instance_uid[instance]
            instance_frame_metadata = []
            for instance_frame_index, frame in enumerate(pydicom3.iter_pixels(f)):
                thumbnail_frame, anchors = _generate_thumbnail_frame_and_anchors(frame)
                # append thumbnail frame to list of all frames
                all_frames.append(thumbnail_frame)
                # append frame-level metadata to list of metadata for all of this instance's frames
                instance_frame_metadata.append(
                    {"thumbnail_index": len(all_frames) - 1, "anchors": anchors}
                )
                # update the list mapping index in overall thumbnail to index within instance (i.e 5th thumbnail frame = 3rd frame of instance 2)
                thumbnail_index_to_instance_frame.append(
                    (instance_uid, instance_frame_index)
                )
            thumbnail_instance_metadata[instance_uid] = {
                "frames": instance_frame_metadata
            }
    thumbnail_metadata = {
        "uri": os.path.join(
            cod_obj.datastore_series_uri,
            f"thumbnail.{'mp4' if len(all_frames) > 1 else 'jpg'}",
        ),
        "thumbnail_index_to_instance_frame": thumbnail_index_to_instance_frame,
        "instances": thumbnail_instance_metadata,
    }
    return all_frames, thumbnail_metadata


def _save_thumbnail_to_disk(cod_obj: "CODObject", all_frames: list[np.ndarray]) -> str:
    """Given the frames of a thumbnail, convert to mp4 or jpg as appropriate and upload to datastore.

    Returns:
        thumbnail_path: the path to the thumbnail on disk
    """
    if len(all_frames) == 0:
        raise NoExtractablePixelDataError(
            f"Failed to extract pixel data from all {str(len(cod_obj._metadata.instances))} instances for {cod_obj}"
        )
    thumbnail_name = "thumbnail.mp4" if len(all_frames) > 1 else "thumbnail.jpg"
    thumbnail_path = os.path.join(cod_obj.temp_dir.name, thumbnail_name)
    if len(all_frames) == 1:
        _convert_frame_to_jpg(all_frames[0], output_path=thumbnail_path)
    else:
        _convert_frames_to_mp4(all_frames, output_path=thumbnail_path)
    return thumbnail_path


def _generate_instance_lookup_dict(
    cod_obj: "CODObject", dirty: bool = False
) -> dict[Instance, str]:
    """Generate a dictionary mapping instances to their instance UIDs.
    (thumbnail metadata requires instance UIDs)
    """
    return {
        instance: instance_uid
        for instance_uid, instance in cod_obj.get_metadata(
            dirty=dirty
        ).instances.items()
    }


def generate_thumbnail(
    cod_obj: "CODObject",
    overwrite_existing: bool = False,
    dirty: bool = False,
):
    """Generate a thumbnail for a COD object.

    Args:
        cod_obj: The COD object to generate a thumbnail for.
        overwrite_existing: Whether to overwrite the existing thumbnail, if it exists.
        dirty: Whether the operation is dirty.
    """
    if (
        cod_obj.get_custom_tag("thumbnail", dirty=dirty) is not None
        and not overwrite_existing
    ):
        logger.info(f"Skipping thumbnail generation for {cod_obj} (already exists)")
        return
    # fetch the tar, if it's not already fetched
    if cod_obj.tar_is_empty:
        cod_obj.pull_tar(dirty=dirty)

    instance_to_instance_uid = _generate_instance_lookup_dict(cod_obj, dirty)
    instances = list(instance_to_instance_uid.keys())
    assert len(instances) > 0, "COD object has no instances"
    instances = _remove_instances_without_pixeldata(cod_obj, instances)
    instances = _sort_instances(instances)
    all_frames, thumbnail_metadata = _generate_thumbnail_frames(
        cod_obj, instances, instance_to_instance_uid
    )
    thumbnail_path = _save_thumbnail_to_disk(cod_obj, all_frames)
    cod_obj.add_custom_tag(
        tag_name="thumbnail",
        tag_value=thumbnail_metadata,
        overwrite_existing=True,
        dirty=dirty,
    )
    # we just generated the thumbnail, so it is not synced to the datastore
    cod_obj._thumbnail_synced = False
    return thumbnail_path
