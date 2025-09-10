import dataclasses
import os
from typing import TYPE_CHECKING, Tuple

import cv2
import ffmpeg
import numpy as np
import pydicom3
from google.cloud import storage

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.instance import Instance

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject

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
    pixel_array: np.ndarray, thumbnail_size: int
) -> Tuple[np.ndarray, dict]:
    """
    Given a DICOM pixel array from pydicom.pixels.iter_pixels, create a thumbnail and record
    the mapping information between original and thumbnail coordinates.

    Args:
        pixel_array: A numpy array from pydicom.pixels.iter_pixels, either (rows, columns) for
                    single sample data or (rows, columns, samples) for multi-sample data
        thumbnail_size: The size of the thumbnail to generate.

    Returns:
        Tuple containing:
        - The thumbnail as a numpy array (always thumbnail_size x thumbnail_size)
        - A dictionary of anchor points mapping between original and thumbnail coordinates
    """
    # Get original dimensions
    height, width = pixel_array.shape[:2]

    # Calculate scaling factor to fit the longer dimension to thumbnail_size
    scale = thumbnail_size / max(height, width)

    # Calculate new dimensions while maintaining aspect ratio
    new_height = int(height * scale)
    new_width = int(width * scale)

    # Resize the image using cv2
    resized = cv2.resize(
        pixel_array, (new_width, new_height), interpolation=cv2.INTER_AREA
    )

    # Create a black square canvas of size thumbnail_size x thumbnail_size
    if len(pixel_array.shape) == 2:  # Grayscale
        thumbnail = np.zeros((thumbnail_size, thumbnail_size), dtype=pixel_array.dtype)
    else:  # Multi-sample (e.g., RGB)
        thumbnail = np.zeros(
            (thumbnail_size, thumbnail_size, pixel_array.shape[2]),
            dtype=pixel_array.dtype,
        )

    # Calculate position to paste the resized image (centered)
    y_offset = (thumbnail_size - new_height) // 2
    x_offset = (thumbnail_size - new_width) // 2

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


def _remove_instances_without_pixeldata(
    cod_obj: "CODObject", uid_to_instance: dict[str, Instance]
):
    """Remove instances that do not have pixel data. Raise an error if no instances have pixel data."""
    filtered_dict = {
        uid: instance
        for uid, instance in uid_to_instance.items()
        if instance.has_pixeldata
    }
    if len(filtered_dict) == 0:
        raise SeriesMissingPixelDataError(
            f"None of the {len(uid_to_instance)} instances have pixel data for cod object {cod_obj}"
        )
    return filtered_dict


def _generate_thumbnail_frames(
    cod_obj: "CODObject",
    uid_to_instance: dict[str, Instance],
    thumbnail_size: int,
):
    """Iterate through instances and generate thumbnail frames.

    Returns:
        all_frames: list of thumbnail frames, in the form of raw numpy ndarrays
        thumbnail_instance_metadata: dict mapping instance uids to metadata for all frames in the instance
        thumbnail_index_to_instance_frame: convenience list mapping thumbnail index to instance uid and frame index
        (i.e. `thumbnail_index_to_instance_frame[4] = (some_uid, 0)` means the 5th thumbnail frame = 1st frame of instance `some_uid`)
        thumbnail_size: The size of the thumbnail to generate.
    """
    all_frames = []
    thumbnail_instance_metadata = {}
    thumbnail_index_to_instance_frame = []
    for instance_uid, instance in uid_to_instance.items():
        with instance.open() as f:
            instance_frame_metadata = []
            for instance_frame_index, frame in enumerate(pydicom3.iter_pixels(f)):
                thumbnail_frame, anchors = _generate_thumbnail_frame_and_anchors(
                    frame, thumbnail_size
                )
                # append thumbnail frame to list of all frames
                all_frames.append(thumbnail_frame)
                # append frame-level metadata to list of metadata for all of this instance's frames
                instance_frame_metadata.append(
                    {"thumbnail_index": len(all_frames) - 1, "anchors": anchors}
                )
                # update the list mapping index in overall thumbnail to index within instance (i.e 5th thumbnail frame = 3rd frame of instance 2)
                thumbnail_index_to_instance_frame.append(
                    [instance_uid, instance_frame_index]
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
    thumbnail_path = os.path.join(cod_obj.get_temp_dir(), thumbnail_name)
    if len(all_frames) == 1:
        _convert_frame_to_jpg(all_frames[0], output_path=thumbnail_path)
    else:
        _convert_frames_to_mp4(all_frames, output_path=thumbnail_path)
    return thumbnail_path


def generate_thumbnail(
    cod_obj: "CODObject",
    overwrite_existing: bool = False,
    thumbnail_size: int = DEFAULT_SIZE,
):
    """Generate a thumbnail for a COD object.

    Args:
        cod_obj: The COD object to generate a thumbnail for.
        overwrite_existing: Whether to overwrite the existing thumbnail, if it exists.
        thumbnail_size: The size of the thumbnail to generate (default: 128px).

    Returns:
        thumbnail_path: the path to the thumbnail on disk, or None if the thumbnail was not generated
    """
    try:
        # can infer whether the operation is dirty by checking if the cod object is locked
        dirty = not cod_obj.lock
        if (
            cod_obj.get_metadata_field("thumbnail", dirty=dirty) is not None
            and not overwrite_existing
        ):
            logger.info(f"Skipping thumbnail generation for {cod_obj} (already exists)")
            return
        # fetch the tar, if it's not already fetched
        if cod_obj.tar_is_empty:
            cod_obj.pull_tar(dirty=dirty)

        # cod_obj.get_instances() sorts instances by instance number or slice location, if possible
        uid_to_instance = cod_obj.get_instances(strict_sorting=False, dirty=dirty)
        assert len(uid_to_instance) > 0, "COD object has no instances"
        uid_to_instance = _remove_instances_without_pixeldata(cod_obj, uid_to_instance)
        all_frames, thumbnail_metadata = _generate_thumbnail_frames(
            cod_obj, uid_to_instance, thumbnail_size
        )
        thumbnail_path = _save_thumbnail_to_disk(cod_obj, all_frames)
        cod_obj.add_metadata_field(
            field_name="thumbnail",
            field_value=thumbnail_metadata,
            overwrite_existing=True,
            dirty=dirty,
        )
        # we just generated the thumbnail, so it is not synced to the datastore
        cod_obj._thumbnail_synced = False
        metrics.THUMBNAIL_SUCCESSES.inc()
        metrics.THUMBNAIL_BYTES_PROCESSED.inc(os.path.getsize(thumbnail_path))
        return thumbnail_path
    except SeriesMissingPixelDataError:
        metrics.SERIES_MISSING_PIXEL_DATA.inc()
        logger.warning(
            f"Could not generate thumbnail for {cod_obj} because it has no pixel data"
        )
        return None
    except Exception as e:
        # On exception, increment failure metric, log exception, and re-raise
        metrics.THUMBNAIL_FAILS.inc()
        logger.exception(f"Error generating thumbnail for {cod_obj}: {e}")
        raise e


def fetch_thumbnail(cod_obj: "CODObject") -> str:
    """Download thumbnail from GCS for given cod object.

    Returns:
        thumbnail_path: the path to the thumbnail on disk

    Raises:
        ValueError: if the cod object has no thumbnail metadata
        NotFound: if the thumbnail blob does not exist in GCS
    """
    thumbnail_metadata = cod_obj.get_metadata_field("thumbnail", dirty=not cod_obj.lock)
    if thumbnail_metadata is None:
        raise ValueError(f"Thumbnail metadata not found for {cod_obj}")
    thumbnail_uri = thumbnail_metadata["uri"]
    logger.info(f"Fetching thumbnail from {thumbnail_uri}")
    thumbnail_blob = storage.Blob.from_string(thumbnail_uri, client=cod_obj.client)
    thumbnail_local_path = os.path.join(
        cod_obj.get_temp_dir(), thumbnail_uri.split("/")[-1]
    )
    thumbnail_blob.download_to_filename(thumbnail_local_path)
    # we just fetched the thumbnail, so it is guaranteed to be in the same state as the datastore
    cod_obj._thumbnail_synced = True
    return thumbnail_local_path


def get_instance_thumbnail_slice(
    cod_obj: "CODObject",
    thumbnail_array: np.ndarray,
    instance_uid: str,
) -> np.ndarray:
    """Get a slice of the thumbnail for a given instance.

    Args:
        cod_obj: The COD object to get the thumbnail slice for.
        thumbnail_array: The numpy array of the full series thumbnail.
        instance_uid: The UID of the instance to get the thumbnail slice for.

    Returns:
        thumbnail_slice: a numpy array of the thumbnail slice
    """
    thumbnail_metadata = cod_obj.get_metadata_field("thumbnail", dirty=not cod_obj.lock)
    # if thumbnail only contains one instance, assert that is the instance requested and return the full array
    if len(thumbnail_metadata["instances"]) == 1:
        assert (
            instance_uid in thumbnail_metadata["instances"]
        ), f"Instance UID {instance_uid} not found in thumbnail metadata"
        return thumbnail_array
    instance_frame_metadata = thumbnail_metadata["instances"][instance_uid]["frames"]
    thumbnail_indices = [frame["thumbnail_index"] for frame in instance_frame_metadata]
    # if we get here, we have a video thumbnail
    instance_slice = thumbnail_array[thumbnail_indices]
    # if the instance slice is a single frame, return the frame (i.e. squeeze the first dimension)
    if instance_slice.shape[0] == 1:
        return instance_slice[0]
    # otherwise, return the instance slice video
    return instance_slice


def get_instance_by_thumbnail_index(
    cod_obj: "CODObject", thumbnail_index: int
) -> Instance:
    """Get an instance by thumbnail index.

    Args:
        thumbnail_index: int - The index of the thumbnail from you want the instance for.

    Returns:
        instance: The instance corresponding to the thumbnail index.

    Raises:
        ValueError: if the cod object has no thumbnail metadata, or `thumbnail_index` is out of bounds
    """
    thumbnail_metadata = cod_obj.get_metadata_field("thumbnail", dirty=not cod_obj.lock)
    if not thumbnail_metadata:
        raise ValueError(f"Thumbnail metadata not found for {cod_obj}")
    thumbnail_index_to_instance_frame = thumbnail_metadata[
        "thumbnail_index_to_instance_frame"
    ]
    if (num_frames := len(thumbnail_index_to_instance_frame)) <= thumbnail_index:
        raise ValueError(
            f"Thumbnail index {thumbnail_index} is out of bounds for {cod_obj} (has {num_frames} frames)"
        )
    instance_uid, _ = thumbnail_index_to_instance_frame[thumbnail_index]
    return cod_obj.get_instance(instance_uid=instance_uid, dirty=not cod_obj.lock)


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
