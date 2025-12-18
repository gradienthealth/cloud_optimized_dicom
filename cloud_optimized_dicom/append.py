import os
import tarfile
from typing import TYPE_CHECKING, NamedTuple, Optional

from ratarmountcore import open as rmc_open

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.errors import HintMismatchError
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.utils import is_remote

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject

BYTES_PER_GB = 1024 * 1024 * 1024


# define namedtuple for append results
class AppendResult(NamedTuple):
    new: list[Instance] = []
    same: list[Instance] = []
    conflict: list[Instance] = []
    errors: list[tuple[Instance, Exception]] = []


class StateChange(NamedTuple):
    new: list[tuple[Instance, Optional[SeriesMetadata], Optional[str]]]
    same: list[tuple[Instance, Optional[SeriesMetadata], Optional[str]]]
    diff: list[tuple[Instance, Optional[SeriesMetadata], Optional[str]]]


def append(
    cod_object: "CODObject",
    instances: list[Instance],
    delete_local_origin: bool = False,
    treat_metadata_diffs_as_same: bool = False,
    max_instance_size: float = None,
    max_series_size: float = None,
    compress: bool = True,
) -> AppendResult:
    """Append a list of instances to the COD object.

    Args:
        cod_object (CODObject): The COD object to append to
        instances (list): list of instances to append
        delete_local_origin (bool): whether to delete instance origin files after successful append (if local, remote origins are never deleted)
        treat_metadata_diffs_as_same (bool): if True, when a diff hash dupe is found, compute & compare the hashes of JUST the pixel data. If they match, treat the dupe as same.
        max_instance_size (float): maximum size of an instance to append, in gb
        max_series_size (float): maximum size of the series to append, in gb
        compress (bool): whether to transcode instances to JPEG2000Lossless syntax before appending to tar
    Returns: an AppendResult; a namedtuple with the following fields:
        new (list): list of new instances that were added successfully
        same (list): list of instances that were perfect duplicates of existing instances
        conflict (list): list of instances that were the same instance UID but different hashes
        errors (list): list of instance, error tuples that occurred during the append process
    """
    append_result = AppendResult(new=[], same=[], conflict=[], errors=[])
    # remove overlarge instances
    instances, append_result = _assert_not_too_large(
        cod_object, instances, max_instance_size, max_series_size, append_result
    )
    # remove duplicates from input
    instances, append_result = _dedupe(instances, append_result)
    # remove instances that do not belong to the COD object
    instances, append_result = _assert_instances_belong_to_cod_obj(
        cod_object, instances, append_result
    )
    # Calculate state change as a result of instances added by this group
    state_change, append_result = _calculate_state_change(
        cod_object, instances, treat_metadata_diffs_as_same, append_result
    )
    # handle same
    append_result = _handle_same(state_change.same, append_result)
    # Edge case: no NEW or DIFF state changes -> return early
    if not state_change.new and not state_change.diff:
        logger.warning(f"No new instances: {cod_object}")
        metrics.SERIES_DUPE_COUNTER.inc()
        return append_result
    # handle diff
    append_result = _handle_diff(cod_object, state_change.diff, append_result)
    # Edge case: no NEW state changes, but some DIFFs -> return early
    if not state_change.new:
        return append_result
    # handle new
    append_result = _handle_new(
        cod_object, state_change.new, append_result, compress=compress
    )
    # increment metrics
    metrics.APPEND_CONFLICTS.inc(len(append_result.conflict))
    metrics.APPEND_DUPLICATES.inc(len(append_result.same))
    metrics.APPEND_FAILS.inc(len(append_result.errors))
    metrics.APPEND_SUCCESSES.inc(len(append_result.new))
    metrics.TOTAL_FILES_PROCESSED.inc(len(instances))
    metrics.TAR_SUCCESS_COUNTER.inc()
    metrics.TAR_BYTES_PROCESSED.inc(os.path.getsize(cod_object.tar_file_path))
    return append_result


def _assert_not_too_large(
    cod_object: "CODObject",
    instances: list[Instance],
    max_instance_size: float,
    max_series_size: float,
    append_result: AppendResult,
) -> tuple[list[Instance], AppendResult]:
    """Performs 2 size validations:
    1. None of the individual instances are too large
    2. The overall series size is not too large

    Args:
        cod_object (CODObject): The COD object to append to
        instances (list): list of instances to validate
        max_instance_size (float): maximum size of an instance to append, in gb
        max_series_size (float): maximum size of the series to append, in gb
        append_result (AppendResult): current append result to update
    Returns:
        tuple of (filtered_instances, updated_append_result)
    Raises:
        ValueError: if the series is too large
    """
    grouping_size = 0
    errors = []
    filtered_instances = []
    for instance in instances:
        # first get the size. If hints were not provided, this may cause an error if instance fetch/validation fails
        try:
            cur_size = instance.size(trust_hints_if_available=True)
        except Exception as e:
            logger.exception(e)
            errors.append((instance, e))
            continue
        # now that we have the size, filter instance if overlarge
        if cur_size > max_instance_size * BYTES_PER_GB:
            overlarge_msg = f"Overlarge instance: {instance} ({cur_size} bytes) exceeds max_instance_size: {max_instance_size}gb"
            logger.warning(overlarge_msg)
            errors.append((instance, ValueError(overlarge_msg)))
        else:
            filtered_instances.append(instance)
            grouping_size += cur_size
    # add size of any pre-existing instances
    if cod_object._metadata:
        grouping_size += sum(
            instance.size() for instance in cod_object._metadata.instances.values()
        )
    # raise an error if overall series is too large (to be caught by caller)
    if grouping_size > max_series_size * BYTES_PER_GB:
        raise ValueError(
            f"Overlarge series: {cod_object} ({grouping_size} bytes) exceeds max_series_size: {max_series_size}gb"
        )
    # update append result
    append_result = AppendResult(
        new=append_result.new,
        same=append_result.same,
        conflict=append_result.conflict,
        errors=append_result.errors + errors,
    )
    return filtered_instances, append_result


def _dedupe(
    instances: list[Instance], append_result: AppendResult
) -> tuple[list[Instance], AppendResult]:
    """
    We expect uniqueness of instance ids within the input series.
    This method removes and records the paths to any duplicate instance files.
    ALL duplicates are removed, but dupe paths are only recorded if they are remote.
    Returns:
        tuple of (deduped_instances, updated_append_result)
    """
    instance_id_to_instance: dict[str, Instance] = {}
    same, conflict, errors = [], [], []
    for instance in instances:
        try:
            instance_id = instance.instance_uid(trust_hints_if_available=True)
            # handle duplicate instance id case
            if instance_id in instance_id_to_instance:
                preexisting_instance = instance_id_to_instance[instance_id]
                # if two instances share a UID AND the same URI, we don't have a duplicate - we have two versions of the same file.
                # In this case, hints cannot be trusted (which version of the file is more recent?).
                # Solution: keep the instance in the dict, but throw out the hints.
                if instance.dicom_uri == preexisting_instance.dicom_uri:
                    logger.warning(
                        f"Input contains multiple instances with the same URI: {instance.dicom_uri}. Keeping a single version and throwing out hints"
                    )
                    preexisting_instance.remove_hints()
                    preexisting_instance.validate()
                    continue
                if (
                    instance.crc32c(trust_hints_if_available=True)
                    != preexisting_instance.crc32c()
                ):
                    conflict.append(instance)
                    if is_remote(instance.dicom_uri):
                        preexisting_instance.append_diff_hash_dupe(instance.dicom_uri)
                    logger.warning(f"Removing diff hash dupe from input: {instance}")
                else:
                    same.append(instance)
                    logger.warning(f"Removing true duplicate from input: {instance}")
                continue
            # if we make it here, we have a unique instance id
            instance_id_to_instance[instance_id] = instance
        except Exception as e:
            logger.exception(f"Error deduping instance: {instance}: {e}")
            errors.append((instance, e))
    # update append result
    append_result = AppendResult(
        new=append_result.new,
        same=append_result.same + same,
        conflict=append_result.conflict + conflict,
        errors=append_result.errors + errors,
    )
    return list(instance_id_to_instance.values()), append_result


def _assert_instances_belong_to_cod_obj(
    cod_object: "CODObject", instances: list[Instance], append_result: AppendResult
) -> tuple[list[Instance], AppendResult]:
    """
    Assert that all instances belong to the COD object.
    Returns:
        tuple of (instances_in_series, updated_append_result)
    """
    instances_in_series = []
    errors = []
    for instance in instances:
        # deliberately try/catch assertion to add error instances to append result
        try:
            cod_object.assert_instance_belongs_to_cod_object(instance)
            instances_in_series.append(instance)
        except Exception as e:
            logger.exception(e)
            errors.append((instance, e))

    append_result = AppendResult(
        new=append_result.new,
        same=append_result.same,
        conflict=append_result.conflict,
        errors=append_result.errors + errors,
    )
    return instances_in_series, append_result


def _get_instance_uid_for_comparison(
    cod_object: "CODObject", instance: Instance, trust_hints_if_available: bool = False
) -> str:
    """
    Get the instance uid for comparison. If the cod object uses hashed uids,
    return the hashed uid, otherwise return the standard uid.
    """
    return (
        instance.hashed_instance_uid(trust_hints_if_available=trust_hints_if_available)
        if cod_object.hashed_uids
        else instance.instance_uid(trust_hints_if_available=trust_hints_if_available)
    )


def _calculate_state_change(
    cod_object: "CODObject",
    instances: list[Instance],
    treat_metadata_diffs_as_same: bool,
    append_result: AppendResult,
) -> tuple[StateChange, AppendResult]:
    """For each file in the grouping, determine if it is NEW, SAME, or DIFF
    compared to the current series metadata json which contains instance_uid and crc32c values

    Args:
        cod_object (CODObject): The COD object to append to
        instances (list): list of instances to calculate state change for
        treat_metadata_diffs_as_same (bool): if True, when a diff hash dupe is found, compute & compare the hashes of JUST the pixel data. If they match, treat the dupe as same.
        append_result (AppendResult): current append result to update

    Returns:
        tuple of (state_change, updated_append_result)
    """
    state_change = StateChange(new=[], same=[], diff=[])
    errors = []
    # If there is no preexisting series metadata, all files are new
    if len(cod_object._metadata.instances) == 0:
        for instance in instances:
            state_change.new.append((instance, None, None))
        return state_change, append_result

    # we will need to fetch the remote tar in order to compute pixeldata hash
    if (
        treat_metadata_diffs_as_same
        and len(cod_object._metadata.instances) > 0
        and any(
            new_inst.get_instance_uid(
                hashed=cod_object.hashed_uids,
                trust_hints_if_available=True,
            )
            in cod_object._metadata.instances
            for new_inst in instances
        )
    ):
        logger.info("PULLING_TAR:DUPE_UID_FOUND_SO_PIXELDATA_HASH_MUST_BE_COMPUTED")
        cod_object.pull_tar(dirty=not cod_object.lock)

    # Calculate state change for each file in the new series
    for new_instance in instances:
        try:
            # if deid instance id isn't in existing metadata dict, this file must be new
            instance_uid = _get_instance_uid_for_comparison(
                cod_object, new_instance, trust_hints_if_available=True
            )
            if instance_uid not in cod_object._metadata.instances:
                state_change.new.append((new_instance, None, None))
                continue

            # if we make it here, the instance id is in the existing metadata
            existing_instance = cod_object._metadata.instances[instance_uid]
            # if the crc32c is the same, we have a true duplicate
            if new_instance.crc32c(
                trust_hints_if_available=True
            ) == existing_instance.crc32c() or (
                treat_metadata_diffs_as_same
                and existing_instance.has_pixeldata
                and new_instance.has_pixeldata
                and new_instance.get_pixeldata_hash()
                == existing_instance.get_pixeldata_hash()
            ):
                state_change.same.append(
                    (
                        new_instance,
                        cod_object._metadata,
                        instance_uid,
                    )
                )
            # if the crc32c is different, we have a diff hash duplicate
            else:
                state_change.diff.append(
                    (
                        new_instance,
                        cod_object._metadata,
                        instance_uid,
                    )
                )
        except Exception as e:
            logger.exception(e)
            errors.append((new_instance, e))

    append_result = AppendResult(
        new=append_result.new,
        same=append_result.same,
        conflict=append_result.conflict,
        errors=append_result.errors + errors,
    )
    return state_change, append_result


def _handle_same(
    same_state_changes: list[tuple[Instance, Optional[SeriesMetadata], Optional[str]]],
    append_result: AppendResult,
) -> AppendResult:
    """Log a warning for each instance that is the same as a previous instance, and update append result"""
    for dupe_instance, series_metadata, deid_instance_uid in same_state_changes:
        existing_path = series_metadata.instances[deid_instance_uid].dicom_uri
        logger.warning(
            f"Skipping duplicate instance (same hash): {dupe_instance} (duplicate of {existing_path})"
        )
    # update append result
    return AppendResult(
        new=append_result.new,
        same=append_result.same + [same for same, _, _ in same_state_changes],
        conflict=append_result.conflict,
        errors=append_result.errors,
    )


def _handle_diff(
    cod_object: "CODObject",
    diff_state_changes: list[tuple[Instance, Optional[SeriesMetadata], Optional[str]]],
    append_result: AppendResult,
) -> AppendResult:
    """Log a warning for each file that is a repeat instance UID with a different hash,
    add file URIs to that instance's diff_hash_dupe_paths in the series metadata,
    and update append result
    """
    for dupe_instance, series_metadata, deid_instance_uid in diff_state_changes:
        existing_instance = series_metadata.instances[deid_instance_uid]
        # add novel (not already in diff_hash_dupe_paths), remote dupe uris to diff_hash_dupe_paths
        logger.warning(
            f"Skipping duplicate instance (diff hash): {dupe_instance} (duplicate of {existing_instance.dicom_uri})"
        )
        if existing_instance.append_diff_hash_dupe(dupe_instance._original_path):
            # metadata is now desynced because we added to diff_hash_dupe_paths
            cod_object._metadata_synced = False

    # update append result
    return AppendResult(
        new=append_result.new,
        same=append_result.same,
        conflict=append_result.conflict + [diff for diff, _, _ in diff_state_changes],
        errors=append_result.errors,
    )


def _validate_new_instances(instances: list[Instance]):
    """Validate the instances; if hints are incorrect, we must know or else we risk corrupting the datastore"""
    validated_instances: list[Instance] = []
    errors: list[tuple[Instance, Exception]] = []
    for instance in instances:
        try:
            instance.validate()
            validated_instances.append(instance)
        except HintMismatchError as e:
            logger.exception(f"Hint mismatch for instance: {instance}: {e}")
            errors.append((instance, e))
        except Exception as e:
            logger.exception(f"Unexpected error validating instance: {instance}: {e}")
            errors.append((instance, e))
    return validated_instances, errors


def _compress_instances(instances: list[Instance]):
    """Attempt to compress instances and record any errors"""
    compressed_instances: list[Instance] = []
    errors: list[tuple[Instance, Exception]] = []
    for instance in instances:
        try:
            instance.compress()
            compressed_instances.append(instance)
        except Exception as e:
            logger.exception(
                f"Error compressing instance (dicom is likely corrupt): {instance}: {e}"
            )
            errors.append((instance, e))
    return compressed_instances, errors


def _handle_new(
    cod_object: "CODObject",
    new_state_changes: list[tuple[Instance, Optional[SeriesMetadata], Optional[str]]],
    append_result: AppendResult,
    compress: bool = True,
) -> AppendResult:
    """
    Compress instances if specified; create/append to tar & upload; add to series metadata & upload.
    Returns:
        updated_append_result
    """
    # Step 1: validate new instances (if hints are wrong we throw them out)
    validated_instances, validation_errors = _validate_new_instances(
        [new for new, _, _ in new_state_changes]
    )
    # Step 2: compress instances (if specified)
    compressed_instances, compression_errors = _compress_instances(validated_instances)

    if not compressed_instances:
        logger.warning(
            "Entire series failed compression/validation; no instances to add to tar"
        )
        return AppendResult(
            new=append_result.new,
            same=append_result.same,
            conflict=append_result.conflict,
            errors=append_result.errors + validation_errors + compression_errors,
        )

    # Step 3: create/append to tar
    instances_added_to_tar, tar_errors = _handle_create_tar(
        cod_object, compressed_instances
    )
    # Step 4: add to series metadata
    _handle_create_metadata(cod_object, instances_added_to_tar)
    # Step 5: return updated append result
    return AppendResult(
        new=append_result.new + instances_added_to_tar,
        same=append_result.same,
        conflict=append_result.conflict,
        errors=append_result.errors
        + validation_errors
        + tar_errors
        + compression_errors,
    )


def _handle_create_tar(
    cod_object: "CODObject",
    instances_to_add: list[Instance],
):
    """
    Create/append to tar + index.sqlite locally
    Returns:
        instances_added_to_tar (list): list of instances that got added to the tar successfully
        errors (list): list of instance, error tuples that occurred during the tar creation process
    """
    # If a tarball already exists (and this is a clean append), download it (no need to get index, will be recalculated anyways)
    if len(cod_object._metadata.instances) > 0:
        cod_object.pull_tar(dirty=not cod_object.lock)

    instances_added_to_tar, errors = _create_or_append_tar(cod_object, instances_to_add)
    _create_sqlite_index(cod_object)
    return instances_added_to_tar, errors


def _create_or_append_tar(cod_object: "CODObject", instances_to_add: list[Instance]):
    """Create/append to `cod_object.tar_file_path` all instances in `instances_to_add`

    Returns:
        instances_added_to_tar (list): instances that were successfully added to the tar
    Raises:
        ValueError: if no instances were successfully added to the tar
    """
    # validate that at least one instance is being added
    assert len(instances_to_add) > 0, "No instances to add to tar"
    # create/append to tar
    instances_added_to_tar: list[Instance] = []
    errors: list[tuple[Instance, Exception]] = []
    with tarfile.open(cod_object.tar_file_path, "a") as tar:
        for instance in instances_to_add:
            try:
                instance.append_to_series_tar(tar)
                instances_added_to_tar.append(instance)
            except Exception as e:
                logger.exception(e)
                errors.append((instance, e))
    # Edge case: no instances were successfully added to the tar
    if len(instances_added_to_tar) == 0:
        uri_str = "\n".join([instance.dicom_uri for instance in instances_to_add])
        raise ValueError(f"GRADIENT_STATE_LOGS:FAILED_TO_TAR_ALL_INSTANCES:{uri_str}")
    logger.info(
        f"GRADIENT_STATE_LOGS:POPULATED_TAR:{cod_object.tar_file_path} ({os.path.getsize(cod_object.tar_file_path)} bytes)"
    )
    # tar has been altered, so it is no longer in sync with the datastore
    cod_object._tar_synced = False
    return instances_added_to_tar, errors


def _create_sqlite_index(cod_object: "CODObject"):
    """
    Given a tar on disk, open it with ratarmountcore and save the index to `cod_object.index_file_path`.
    """
    # index needs to be recreated if it already exists
    if os.path.exists(cod_object.index_file_path):
        os.remove(cod_object.index_file_path)
    # explicitly bypass property getter to avoid AttributeError: does not exist
    with rmc_open(
        cod_object.tar_file_path,
        writeIndex=True,
        indexFilePath=cod_object.index_file_path,
    ):
        pass


def _handle_create_metadata(
    cod_object: "CODObject",
    instances_added_to_tar: list[Instance],
):
    """Update metadata locally with new instances.
    Do not catch errors; any exceptions here should bubble up as they represent a desync between tar and metadata
    """
    # Add new instances to metadata
    for instance in instances_added_to_tar:
        # get hashed uid if series is hashed, standard if not
        uid = instance.get_instance_uid(hashed=cod_object.hashed_uids)
        output_uri = f"{cod_object.tar_uri}://instances/{uid}.dcm"
        instance.extract_metadata(output_uri)
        # compress metadata immediately to avoid ballooning memory usage
        instance._dicom_metadata.compress()
        cod_object._metadata.instances[uid] = instance
    # if we added any instances, metadata is now desynced
    cod_object._metadata_synced = False if len(instances_added_to_tar) > 0 else True
