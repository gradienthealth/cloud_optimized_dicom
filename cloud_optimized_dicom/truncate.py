import os
from typing import TYPE_CHECKING

from cloud_optimized_dicom.append import AppendResult
from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.instance import Instance

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject


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
    cod_object: "CODObject", instances_to_keep: list[Instance]
) -> list[Instance]:
    """
    Extract the instances to keep from the tar file.
    """

    # pull the tar if we don't have it already
    if cod_object.tar_is_empty and not cod_object._tar_synced:
        cod_object.pull_tar(dirty=not cod_object.lock)

    local_instances = []
    for instance in instances_to_keep:
        instance_temp_path = os.path.join(
            cod_object.get_temp_dir(), f"{instance.instance_uid()}.dcm"
        )
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
    logger.info(f"Extracted {len(local_instances)} instance(s) to keep")
    return local_instances


def remove(
    cod_object: "CODObject", instances: list[Instance], dirty: bool = False
) -> AppendResult:
    """
    Remove instances from a cod object. Because tar files do not natively support removal,
    this method just determines a list of instances to keep (if any) and calls truncate.

    Returns:
        AppendResult of the truncate operation (i.e. what's left in the cod object)
    Raises:
        ValueError if all instances are removed (i.e. cod_obj.delete() should be used instead)
    """
    # validate the presence of instance to remove in COD
    instances_in_cod = list(cod_object.get_metadata(dirty=dirty).instances.values())
    to_remove = _skip_missing_instances(cod_object, instances, instances_in_cod)

    # early exit if no instances to remove
    if len(to_remove) == 0:
        return AppendResult(new=instances_in_cod)

    # determine what instances will be kept (if any)
    instances_to_keep = [
        instance for instance in instances_in_cod if instance not in to_remove
    ]
    if len(instances_to_keep) == 0:
        raise ValueError(
            "Cannot remove all instances... did you mean to cod_obj.delete()?"
        )

    # extract instances we want to keep to disk
    instances_to_keep = _extract_instances_to_keep(cod_object, instances_to_keep)

    # call truncate with the instances we want to keep
    return cod_object.truncate(instances_to_keep, dirty=dirty)


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
    # determine what instances will be kept (if any)
    instances_in_cod = cod_object.get_instances(
        strict_sorting=False, dirty=dirty
    ).values()
    instances_to_keep = [
        instance for instance in instances_in_cod if instance in instances
    ]
    new_instances = [
        instance for instance in instances if instance not in instances_in_cod
    ]

    # extract instances to keep to disk
    instances_to_keep = _extract_instances_to_keep(cod_object, instances_to_keep)

    # for our append, we want to do all the instanes we want to keep, plus any new instances
    instances_to_append = instances_to_keep + new_instances

    # wipe the local tar, index, and metadata
    cod_object._wipe_local()

    # append the instances to keep
    return cod_object.append(
        instances=instances_to_append,
        treat_metadata_diffs_as_same=treat_metadata_diffs_as_same,
        max_instance_size=max_instance_size,
        max_series_size=max_series_size,
        delete_local_origin=delete_local_origin,
        dirty=dirty,
    )
