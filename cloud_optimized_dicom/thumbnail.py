import dataclasses
import logging
import os
from typing import TYPE_CHECKING, Tuple

import cv2
import ffmpeg
import numpy as np
import pydicom3
from google.cloud import storage

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.utils import upload_and_count_file

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject

logger = logging.getLogger(__name__)

SORTING_ATTRIBUTES = {"InstanceNumber": "00200013", "SliceLocation": "00201041"}
DEFAULT_FPS = 4
DEFAULT_QUALITY = 60
DEFAULT_SIZE = 128


class ThumbnailError(Exception):
    """Error generating thumbnail."""


class SeriesMissingPixelDataError(ThumbnailError):
    """Series has no pixel data."""


class NoExtractablePixelDataError(ThumbnailError):
    """Series has pixel data, but we failed to extract any of it."""


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

    def _process_frame(frame: np.ndarray) -> bytes:
        """For color thumbnails, convert frame to BGR format. No conversion is necessary for grayscale thumbnails.
        After formatting, normalize the frame (0-255), set data type to uint8, convert to bytes, and return.
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
        return cv2.normalize(frame, None, 255, 0, cv2.NORM_MINMAX, cv2.CV_8U).tobytes()

    # Create ffmpeg process
    process = (
        ffmpeg.input(
            "pipe:",
            format="rawvideo",
            pix_fmt="bgr24" if thumbnail_is_color else "gray",
            s=f"{width}x{height}",
            r=fps,
        )
        .output(
            output_path, vcodec="libx264", pix_fmt="yuv420p", r=fps, loglevel="error"
        )
        .overwrite_output()
        .run_async(pipe_stdin=True)
    )

    try:
        # Write frames to ffmpeg process
        for frame in frames:
            process.stdin.write(_process_frame(frame))
        process.stdin.close()
        process.wait()
    except Exception as e:
        process.kill()
        raise RuntimeError(f"Failed to write video: {str(e)}")


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
    metrics.THUMBNAIL_SUCCESS_COUNTER.inc()
    metrics.THUMBNAIL_BYTES_PROCESSED.inc(os.path.getsize(thumbnail_path))
    return thumbnail_path


@dataclasses.dataclass
class ThumbnailCoordConverter:
    orig_w: int
    orig_h: int
    thmb_ul_x: int
    thmb_ul_y: int
    thmb_br_x: int
    thmb_br_y: int

    @property
    def thmb_w(self):
        return self.thmb_br_x - self.thmb_ul_x

    @property
    def thmb_h(self):
        return self.thmb_br_y - self.thmb_ul_y

    def thumbnail_to_original(
        self, thumbnail_coords: Tuple[float, float]
    ) -> Tuple[float, float]:
        """Convert a point in thumbnail space to original coordinate space"""
        # Extract coordinates from the thumbnail_coords tuple
        thmb_x, thmb_y = thumbnail_coords

        # Check if the point is outside the bounds of the original image in the thumbnail
        if not (
            self.thmb_ul_x <= thmb_x <= self.thmb_br_x
            and self.thmb_ul_y <= thmb_y <= self.thmb_br_y
        ):
            raise ValueError(
                "The given thumbnail coordinates are outside the bounds of the original image in the thumbnail."
            )

        # Calculate the scaling factors between the thumbnail and the original image
        scale_x = self.orig_w / self.thmb_w
        scale_y = self.orig_h / self.thmb_h

        # Map the thumbnail coordinates back to the original image
        orig_x = (thmb_x - self.thmb_ul_x) * scale_x
        orig_y = (thmb_y - self.thmb_ul_y) * scale_y
        return orig_x, orig_y

    def original_to_thumbnail(
        self, original_coords: Tuple[float, float]
    ) -> Tuple[float, float]:
        """Convert a point in original coordinate space to thumbnail space"""
        # Extract coordinates from the original_coords tuple
        orig_x, orig_y = original_coords

        # Check if the original coordinates are within the bounds of the original image
        if not (0 <= orig_x <= self.orig_w and 0 <= orig_y <= self.orig_h):
            raise ValueError(
                "The given original coordinates are outside the bounds of the original image."
            )

        # Calculate the scaling factors between the original image and the thumbnail
        scale_x = self.thmb_w / self.orig_w
        scale_y = self.thmb_h / self.orig_h

        # Map the original coordinates to the thumbnail
        thmb_x = orig_x * scale_x + self.thmb_ul_x
        thmb_y = orig_y * scale_y + self.thmb_ul_y
        return thmb_x, thmb_y

    @classmethod
    def from_anchors(cls, anchors: dict) -> "ThumbnailCoordConverter":
        try:
            return ThumbnailCoordConverter(
                orig_w=anchors["original_size"]["width"],
                orig_h=anchors["original_size"]["height"],
                thmb_ul_x=anchors["thumbnail_upper_left"]["col"],
                thmb_ul_y=anchors["thumbnail_upper_left"]["row"],
                thmb_br_x=anchors["thumbnail_bottom_right"]["col"],
                thmb_br_y=anchors["thumbnail_bottom_right"]["row"],
            )
        except KeyError:
            logger.exception(f"Anchors dict missing required fields: {anchors}")
