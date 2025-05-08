import logging

import pydicom3

from cloud_optimized_dicom.cod_object import CODObject
from cloud_optimized_dicom.instance import Instance

logger = logging.getLogger(__name__)

SORTING_ATTRIBUTES = {"InstanceNumber": "00200013", "SliceLocation": "00201041"}


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


def generate_thumbnail(cod_obj: CODObject, dirty: bool = False):
    """Generate a thumbnail for a COD object."""
    # fetch the tar, if it's not already fetched
    if cod_obj.tar_is_empty:
        cod_obj.pull_tar(dirty=dirty)

    # fetch list of instances
    instances = cod_obj.get_metadata(dirty=dirty).instances.values()
    # filter out instances without pixel data
    instances = [instance for instance in instances if instance.has_pixeldata]
    instances = _sort_instances(instances)
    with instances[0].open() as f:
        ds = pydicom3.dcmread(f)
        print(ds.StudyInstanceUID)
