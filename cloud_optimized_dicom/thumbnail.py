import logging

import pydicom3

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.img_utils import decode_pixel_data
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.metrics import SERIES_MISSING_PIXEL_DATA

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
    cod_obj: CODObject, instances: list[Instance]
) -> list[Instance]:
    """Remove instances that do not have pixel data."""
    num_instances = len(instances)
    instances = [instance for instance in instances if instance.has_pixeldata]
    if len(instances) == 0:
        SERIES_MISSING_PIXEL_DATA.inc()
        raise SeriesMissingPixelDataError(
            f"None of the {num_instances} instances have pixel data for cod object {cod_obj}"
        )
    return instances


def _generate_thumbnail_frames(cod_obj: CODObject, instances: list[Instance]):
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
            instance_uid = instance.get_instance_uid(hashed=cod_obj.hashed_uids)
            instance_frame_metadata = []
            for instance_frame_index, frame in enumerate(pydicom3.iter_pixels(f)):
                thumbnail_frame, anchors = resize_pad_and_anchor_frame(frame)
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
    return all_frames, thumbnail_instance_metadata, thumbnail_index_to_instance_frame


def generate_thumbnail(cod_obj: CODObject, dirty: bool = False):
    """Generate a thumbnail for a COD object."""
    # fetch the tar, if it's not already fetched
    if cod_obj.tar_is_empty:
        cod_obj.pull_tar(dirty=dirty)

    instances = cod_obj.get_metadata(dirty=dirty).instances.values()
    assert len(instances) > 0, "COD object has no instances"
    instances = _remove_instances_without_pixeldata(cod_obj, instances)
    instances = _sort_instances(instances)
    all_frames, thumbnail_instance_metadata, thumbnail_index_to_instance_frame = (
        _generate_thumbnail_frames(cod_obj, instances)
    )
    if len(all_frames) == 0:
        raise NoExtractablePixelDataError(
            f"Failed to extract pixel data from all {str(len(instances))} instances that have some for {cod_obj}"
        )
    elif len(all_frames) == 1:
        return (
            all_frames,
            thumbnail_instance_metadata,
            thumbnail_index_to_instance_frame,
        )
    else:
        # TODO: implement thumbnail generation
        pass
    return all_frames, thumbnail_instance_metadata, thumbnail_index_to_instance_frame
