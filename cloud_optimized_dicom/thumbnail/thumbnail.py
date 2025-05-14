import logging
import os
from typing import TYPE_CHECKING

import numpy as np
import pydicom3
from google.cloud import storage

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.thumbnail.utils import (
    _convert_frame_to_jpg,
    _convert_frames_to_mp4,
    _generate_thumbnail_frame_and_anchors,
)
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
        "uri": None,  # will be set later
        "thumbnail_index_to_instance_frame": thumbnail_index_to_instance_frame,
        "instances": thumbnail_instance_metadata,
    }
    return all_frames, thumbnail_metadata


def _generate_thumbnail_bytes(
    cod_obj: "CODObject", all_frames: list[np.ndarray]
) -> bytes:
    """Given the frames of a thumbnail, convert to mp4 or jpg as appropriate and upload to datastore.

    Returns:
        thumbnail_bytes: the bytes of the thumbnail
    """
    if len(all_frames) == 0:
        raise NoExtractablePixelDataError(
            f"Failed to extract pixel data from all {str(len(cod_obj._metadata.instances))} instances for {cod_obj}"
        )
    thumbnail_name = "thumbnail.mp4" if len(all_frames) > 1 else "thumbnail.jpg"
    temp_path = os.path.join(cod_obj.temp_dir.name, thumbnail_name)
    if len(all_frames) == 1:
        _convert_frame_to_jpg(all_frames[0], output_path=temp_path)
    else:
        _convert_frames_to_mp4(all_frames, output_path=temp_path)
    with open(temp_path, "rb") as f:
        thumbnail_bytes = f.read()
    return thumbnail_bytes


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
        overwrite_existing: Whether to overwrite the existing thumbnail.
        dirty: Whether to dirty the COD object.
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
    thumbnail_bytes = _generate_thumbnail_bytes(cod_obj, all_frames)
    cod_obj.add_custom_tag(
        tag_name="thumbnail",
        tag_value=thumbnail_metadata,
        overwrite_existing=True,
        dirty=dirty,
    )
    return thumbnail_bytes, thumbnail_metadata
