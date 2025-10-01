import dataclasses
import logging
import os
from typing import TYPE_CHECKING, Tuple

import cv2
import ffmpeg
import numpy as np
import pydicom3
from google.cloud import storage
from pydicom3.pixels import (
    apply_color_lut,
    apply_modality_lut,
    apply_voi_lut,
    apply_windowing,
    convert_color_space,
)

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.config import get_child_logger
from cloud_optimized_dicom.instance import Instance

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject

logger = get_child_logger("THUMBNAIL")

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
    cv2.imwrite(output_path, frame)


def _convert_frames_to_mp4(
    frames: list[np.ndarray], output_path: str, fps: int = DEFAULT_FPS
):
    """Convert `frames` to an mp4 and save to `output_path`"""
    if len(frames) == 0:
        raise ValueError("Cannot save frames as mp4 because frame list is empty.")

    # Assume all frames are the same shape
    height, width = frames[0].shape[:2]
    if any(frame.shape[:2] != (height, width) for frame in frames):
        raise ValueError("All frames must have the same shape.")

    # Create ffmpeg process
    process = (
        ffmpeg.input(
            "pipe:",
            format="rawvideo",
            pix_fmt="bgr24",
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
            process.stdin.write(frame.tobytes())
        process.stdin.close()
        process.wait()
    except Exception as e:
        process.kill()
        raise RuntimeError(f"Failed to write video: {str(e)}")


def _resize(frame: np.ndarray, max_dim=128) -> np.ndarray:
    """Resize a frame so the longer dimension is `max_dim` and the shorter is proportional"""
    resize_logger = logger.getChild("RESIZE")
    height, width = frame.shape[:2]
    large_size = max(width, height)
    if large_size == max_dim:
        resize_logger.debug(f"No resize needed. Longer dimension is already {max_dim}")
        return frame
    else:
        ratio = large_size / max_dim
        # Note: It"s important to use cv2 here, the numpy resize function returns something that looks like noise
        # Note: this resize function has backward width and height from numpy arrays for some reason, so this is correct
        frame = cv2.resize(
            frame,
            dsize=(int(width / ratio), int(height / ratio)),
            interpolation=cv2.INTER_LINEAR,
        )
        resize_logger.debug(
            f"Resized from {height}x{width} to {frame.shape[0]}x{frame.shape[1]}"
        )
        return frame


def _apply_pydicom_handling(
    frame: np.ndarray,
    ds: pydicom3.Dataset,
    apply_modality=True,
    apply_voi=True,
    apply_window=True,
):
    """
    Apply pydicom grayscale transforms in the correct order:
        1) Modality LUT / Rescale (if requested)
        2) VOI transform: VOI LUT preferred; else WC/WW (if requested)
    Skips non-monochrome images.
    """
    windowing_logger = logger.getChild("WINDOWING")

    def _apply(func, arr, dataset, **kwargs):
        # Attempt to apply function
        try:
            new_arr = func(arr, dataset, **kwargs)
        except Exception as e:
            windowing_logger.exception(f"Function '{func.__name__}' failed: {e}")
            return arr
        # Conditional debug logging
        if windowing_logger.isEnabledFor(logging.DEBUG):
            old_minmax = (np.min(arr), np.max(arr))
            new_minmax = (np.min(new_arr), np.max(new_arr))
            if new_minmax != old_minmax:
                windowing_logger.debug(
                    f"Func '{func.__name__}' altered frame min/max: {old_minmax} -> {new_minmax}"
                )
            elif not np.array_equal(new_arr, arr):
                windowing_logger.debug(
                    f"Func '{func.__name__}' made changes without min/max shift"
                )
            else:
                windowing_logger.debug(f"Func '{func.__name__}' made NO changes")
        # Return the new array with the function applied
        return new_arr

    # Extract photometric interpretation
    phot_interp = ds.get("PhotometricInterpretation").upper().strip()
    if phot_interp is None:
        raise ValueError("PhotometricInterpretation not found in dataset")

    # If PALLETE_COLOR, apply color LUT and return
    if phot_interp == "PALETTE COLOR":
        windowing_logger.debug("Applying color LUT (PALETTE_COLOR)")
        frame = _apply(apply_color_lut, frame, ds)
        return frame

    # If not MONOCHROME1 or MONOCHROME2, skip windowing and return
    if phot_interp not in ("MONOCHROME1", "MONOCHROME2"):
        windowing_logger.debug(
            f"Skipping windowing (non-monochrome PhotometricInterpretation '{phot_interp}')"
        )
        return frame

    # First apply modality LUT
    if apply_modality:
        frame = _apply(apply_modality_lut, frame, ds)

    # Next apply VOI LUT
    # Default behavior (if both `apply_voi` and `apply_window` are true): a single call to `apply_voi_lut`
    # uses VOI LUT if present, otherwise falls back to WC/WW
    if apply_voi:
        frame = _apply(apply_voi_lut, frame, ds)
    # If user asked to skip VOI LUT and do WC/WW only (apply_voi is false and apply_window is true)
    elif apply_window:
        frame = _apply(apply_windowing, frame, ds)

    return frame


def _normalize_and_convert(
    frame: np.ndarray, ds: pydicom3.Dataset, invert_monochrome1: bool = True
) -> np.ndarray:
    """Performs the following operations in sequence:
        1) Normalize the frame between 0 and 255

        2) (if `invert_monochrome1=True` and frame is MONOCHROME1) Invert the frame

        3) Convert the frame data type to uint8

    Args:
        frame: The frame to normalize and convert (np array)
        ds: The DICOM dataset
        invert_monochrome1: Whether to invert the frame if MONOCHROME1 (default: `True`)

    Returns:
        A numpy array of the frame in uint8 format.

    Raises:
        ValueError: If the frame is blank (i.e. all pixels have the same value).
    """

    normalize_logger = logger.getChild("NORMALIZE")
    min, max = np.min(frame), np.max(frame)
    if min == 0 and max == 255:
        normalize_logger.debug(
            "No normalization needed (frame already ranges from 0-255)"
        )
        return frame
    normalize_logger.debug(f"Normalizing frame: ({min},{max}) -> (0,255)")
    if max == min:
        raise ValueError(f"Frame is blank (all pixels have value {max})")
    frame = ((frame - min) / (max - min)) * 255
    phot_interp = ds.get("PhotometricInterpretation").upper().strip()
    if phot_interp == "MONOCHROME1" and invert_monochrome1:
        normalize_logger.debug("Inverting frame (MONOCHROME1)")
        frame = 255 - frame
    return frame.astype(np.uint8)


def _convert_to_bgr(frame: np.ndarray) -> np.ndarray:
    """Convert a frame to BGR (what openCV expects).
    Note: For multi-sample data, we assume the frame is in RGB format, which it should be because pydicom3.iter_pixels converts YBR to RGB.

    Returns:
        A numpy array of the frame in BGR format.

    Raises:
        ValueError: If the frame array shape is unexpected.
    """
    conversion_logger = logger.getChild("CONVERT_TO_BGR")
    match len(frame.shape):
        case 2:
            conversion_logger.debug("Converting grayscale -> BGR")
            return cv2.cvtColor(frame, cv2.COLOR_GRAY2BGR)
        case 3:
            num_samples = frame.shape[2]
            if num_samples == 3:
                conversion_logger.debug("Converting RGB -> BGR")
                return cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
            elif num_samples == 4:
                conversion_logger.debug("Converting RGBA -> BGR")
                return cv2.cvtColor(frame, cv2.COLOR_RGBA2BGR)
            else:
                raise ValueError(
                    f"Unexpected number of samples for frame: {frame.shape}"
                )
        case _:
            raise ValueError(f"Unexpected frame array shape: {frame.shape}")


def _pad(
    frame: np.ndarray, original_width: int, original_height: int
) -> Tuple[np.ndarray, dict]:
    """Pad a frame to make it square.

    Args:
        frame: The frame to pad (np array)
        original_width: The width of the original frame
        original_height: The height of the original frame

    Returns:
        A numpy array of the frame with padding.
        A dictionary of anchor points mapping between original and thumbnail coordinates

    Raises:
        ValueError: If the frame array shape is unexpected.
    """
    pad_logger = logger.getChild("PAD")
    if frame.ndim not in (2, 3):
        raise ValueError(
            f"Expected frame to have 2 or 3 dimensions but got {frame.ndim}"
        )
    # Extract width and height
    frame_height, frame_width = frame.shape[:2]
    # apply padding if frame is not square
    if frame_width != frame_height:
        # compute padding
        max_dim = max(frame_width, frame_height)
        if max_dim == frame_width:
            pad_w = (0, 0)
            total_height_pad = max_dim - frame_height
            pad_h = (total_height_pad // 2, total_height_pad - total_height_pad // 2)
            pad_logger.debug(f"Adding top/bottom padding {pad_h[0]}/{pad_h[1]}")
        else:
            total_width_pad = max_dim - frame_width
            pad_w = (total_width_pad // 2, total_width_pad - total_width_pad // 2)
            pad_h = (0, 0)
            pad_logger.debug(f"Adding left/right padding {pad_w[0]}/{pad_w[1]}")

        # construct padding array
        pad = [pad_h, pad_w]
        if frame.ndim == 3:  # do not pad the samples dimension
            pad.append((0, 0))
        # apply padding
        frame = np.pad(frame, pad, mode="constant", constant_values=0)
    else:
        pad_logger.debug(
            f"No padding needed (frame is already square with side length {frame_width})"
        )
        pad_h = (0, 0)
        pad_w = (0, 0)
    # compute anchor points
    y_offset = pad_h[0]
    x_offset = pad_w[0]
    scale = original_width / frame_width
    anchors = {
        "original_size": {"width": original_width, "height": original_height},
        "thumbnail_upper_left": {"row": y_offset, "col": x_offset},
        "thumbnail_bottom_right": {
            "row": y_offset + frame_height,
            "col": x_offset + frame_width,
        },
        "scale_factor": scale,
    }
    return frame, anchors


def _generate_thumbnail_frame_and_anchors(
    frame: np.ndarray, ds: pydicom3.Dataset, thumbnail_size: int
) -> Tuple[np.ndarray, dict]:
    """
    Given a DICOM frame (from `pydicom.pixels.iter_pixels`), create a thumbnail and record
    the mapping information between original and thumbnail coordinates.

    Args:
        frame: A numpy array from `pydicom.pixels.iter_pixels`, either (rows, columns) for
                    single sample data or (rows, columns, samples) for multi-sample data
        thumbnail_size: The size of the thumbnail to generate.

    Returns:
        Tuple containing:
        - The thumbnail as a numpy array (always thumbnail_size x thumbnail_size)
        - A dictionary of anchor points mapping between original and thumbnail coordinates
    """
    height, width = frame.shape[:2]
    # step 1: resize
    frame = _resize(frame, max_dim=thumbnail_size)
    # step 2: apply modality, voi LUT/windowing, color LUT, etc.
    frame = _apply_pydicom_handling(frame, ds)
    # step 3: normalize between 0 and 255
    frame = _normalize_and_convert(frame, ds)
    # step 4: convert to BGR (what openCV expects)
    frame = _convert_to_bgr(frame)
    # step 5: pad to make it square
    frame, anchors = _pad(frame, original_width=width, original_height=height)
    # Return result
    return frame, anchors


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

    Args:
        cod_obj: The COD object to generate a thumbnail for.
        uid_to_instance: A dictionary mapping instance UIDs to instances.
        thumbnail_size: The size of the thumbnail to generate.

    Returns:
        all_frames: list of thumbnail frames, in the form of raw numpy ndarrays
        thumbnail_metadata: metadata for the thumbnail
    """
    all_frames = []
    thumbnail_instance_metadata = {}
    thumbnail_index_to_instance_frame = []
    for instance_uid, instance in uid_to_instance.items():
        with instance.open() as f:
            ds = pydicom3.dcmread(f, defer_size=1024)
            instance_frame_metadata = []
            for instance_frame_index, frame in enumerate(pydicom3.iter_pixels(ds)):
                thumbnail_frame, anchors = _generate_thumbnail_frame_and_anchors(
                    frame, ds, thumbnail_size
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
