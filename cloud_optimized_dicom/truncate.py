import os
from typing import TYPE_CHECKING

from cloud_optimized_dicom.append import _create_or_append_tar, _handle_create_metadata
from cloud_optimized_dicom.instance import Instance

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject

import logging

logger = logging.getLogger(__name__)


def _skip_missing_instances(
    cod_object: "CODObject",
    remove_requests: list[Instance],
    instances_in_cod: list[Instance],
) -> list[Instance]:
    """
    Skip any instances that are not in the cod object.
    """
    to_remove = []
    for instance in remove_requests:
        if instance not in instances_in_cod:
            logger.warning(
                f"{cod_object} does not contain instance: {instance} - skipping removal"
            )
            continue
        to_remove.append(instance)
    return to_remove


def _extract_instances_to_keep(
    instances_to_keep: list[Instance], temp_dir: str
) -> list[Instance]:
    """
    Extract the instances to keep from the tar file.
    """
    local_instances = []
    for instance in instances_to_keep:
        instance_temp_path = os.path.join(temp_dir, f"{instance.instance_uid()}.dcm")
        with instance.open() as f, open(instance_temp_path, "wb") as f_out:
            f_out.write(f.read())
        local_instance = Instance(
            dicom_uri=instance_temp_path,
            dependencies=instance.dependencies,
            hints=instance.hints,
            uid_hash_func=instance.uid_hash_func,
            _original_path=instance._original_path,
        )
        local_instances.append(local_instance)
    return local_instances


def remove(cod_object: "CODObject", instances: list[Instance], dirty: bool = False):
    # validate the presence of instance to remove in COD
    instances_in_cod = cod_object.get_metadata(dirty=dirty).instances.values()
    to_remove = _skip_missing_instances(cod_object, instances, instances_in_cod)

    # early exit if no instances to remove
    if len(to_remove) == 0:
        return

    # determine what instances will be kept (if any)
    instances_to_keep = [
        instance for instance in instances_in_cod if instance not in to_remove
    ]
    if len(instances_to_keep) == 0:
        raise NotImplementedError("Deletion of ALL instances is not yet supported")

    # pull the tar if we don't have it already
    if not cod_object._tar_synced:
        cod_object.pull_tar(dirty=dirty)

    instances_to_keep = _extract_instances_to_keep(
        instances_to_keep, cod_object.get_temp_dir().name
    )
    # because tar files do not support removal, we need to create a new tar with all the instances we want to keep
    new_tar_path = os.path.join(
        cod_object.get_temp_dir().name, f"{cod_object.series_uid}_with_removals.tar"
    )
    appended_instances = _create_or_append_tar(
        cod_object, instances_to_keep, new_tar_path
    )
    assert len(appended_instances) == len(
        instances_to_keep
    ), "Failed to create new tar with instances not getting removed"

    # wipe old metadata
    cod_object._metadata = {}


def truncate(
    cod_object: "CODObject",
    instances: list[Instance],
    treat_metadata_diffs_as_same: bool = False,
    max_instance_size: float = 10,
    max_series_size: float = 100,
    delete_local_origin: bool = False,
    dirty: bool = False,
):
    """
    Truncate a cod object by replacing any/all preexisting instances with the given instances.
    Essentially, a wrapper for deleting a COD Object and then appending the given instances.
    """
    # delete all instances from the cod object, except for any that happen to be in the new list to append
    instances_to_delete = [
        instance
        for instance in cod_object.get_metadata(dirty=dirty).instances.values()
        if instance not in instances
    ]
    cod_object.remove(instances_to_delete, dirty=dirty)

    # append the new instances
    cod_object.append(
        instances=instances,
        treat_metadata_diffs_as_same=treat_metadata_diffs_as_same,
        max_instance_size=max_instance_size,
        max_series_size=max_series_size,
        delete_local_origin=delete_local_origin,
        dirty=dirty,
    )
