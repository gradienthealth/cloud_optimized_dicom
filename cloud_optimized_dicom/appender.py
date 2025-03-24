import logging
import os
from typing import TYPE_CHECKING, NamedTuple, Optional

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.series_metadata import SeriesMetadata

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject

BYTES_PER_GB = 1024 * 1024 * 1024


# define namedtuple for append results
class AppendResult(NamedTuple):
    new: list[Instance]
    same: list[Instance]
    conflict: list[Instance]
    errors: list[tuple[Instance, Exception]]


class StateChange(NamedTuple):
    new: list[tuple[Instance, Optional[SeriesMetadata], Optional[str]]]
    same: list[tuple[Instance, Optional[SeriesMetadata], Optional[str]]]
    diff: list[tuple[Instance, Optional[SeriesMetadata], Optional[str]]]


class CODAppender:
    """Class for appending DICOM files to a COD object.
    Designed to be instantiated by CODObject.append() and used once."""

    def __init__(self, cod_object: "CODObject"):
        self.cod_object = cod_object
        self.append_result = AppendResult(new=[], same=[], conflict=[], errors=[])

    def append(
        self,
        instances: list[Instance],
        delete_local_origin: bool = False,
        max_instance_size: float = None,
        max_series_size: float = None,
    ) -> AppendResult:
        """Append a list of instances to the COD object.
        Args:
            instances (list): list of instances to append
            delete_local_origin (bool): whether to delete instance origin files after successful append (if local, remote origins are never deleted)
            max_instance_size (float): maximum size of an instance to append, in gb.
            max_series_size (float): maximum size of the series to append, in gb
        Returns: an AppendResult; a namedtuple with the following fields:
            new (list): list of new instances that were added successfully
            same (list): list of instances that were perfect duplicates of existing instances
            conflict (list): list of instances that were the same instance UID but different hashes
            errors (list): list of instance, error tuples that occurred during the append process
        """
        self.append_result = AppendResult(new=[], same=[], conflict=[], errors=[])
        # remove overlarge instances
        instances = self._assert_not_too_large(
            instances, max_instance_size, max_series_size
        )
        # remove duplicates from input
        instances = self._dedupe(instances)
        # Calculate state change as a result of instances added by this group
        state_change = self._calculate_state_change(instances)
        # handle same
        self._handle_true_duplicates(state_change.same)
        # Edge case: no NEW or DIFF state changes -> return early
        if not len(state_changes["NEW"]) and not len(state_changes["DIFF"]):
            logger.warning(f"GRADIENT_STATE_LOGS:NO_NEW_INSTANCES:{self.as_log}")
            metrics.SERIES_DUPE_COUNTER.inc()
            return self.append_result
        # handle diff
        self._handle_diff_hash_duplicates(state_changes["DIFF"])
        # Edge case: no NEW state changes, but some DIFFs -> return early
        if not len(state_changes["NEW"]):
            return self.append_result
        # handle new
        self._handle_new(state_changes["NEW"])
        metrics.TAR_SUCCESS_COUNTER.inc()
        metrics.TAR_BYTES_PROCESSED.inc(os.path.getsize(self.cod_object.local_tar_path))
        return self.append_result

    def _assert_not_too_large(
        self,
        instances: list[Instance],
        max_instance_size: float,
        max_series_size: float,
    ) -> list[Instance]:
        """Performs 2 size validations:
        1. None of the individual instances are too large
        2. The overall series size is not too large

        Args:
            instances (list): list of instances to validate
            max_instance_size (float): maximum size of an instance to append, in gb
            max_series_size (float): maximum size of the series to append, in gb
        Returns:
            filtered_instances (list): list of instances that are not too large
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
            # now that we have the size, filter instance if overlarge
            if cur_size > max_instance_size * BYTES_PER_GB:
                overlarge_msg = f"Overlarge instance: {instance.as_log} ({cur_size} bytes) exceeds max_instance_size: {max_instance_size}gb"
                logger.warning(overlarge_msg)
                errors.append((instance, ValueError(overlarge_msg)))
            else:
                filtered_instances.append(instance)
                grouping_size += cur_size
        # add size of any pre-existing instances
        if self.cod_object._metadata:
            grouping_size += sum(
                instance.size()
                for instance in self.cod_object._metadata.instances.values()
            )
        # raise an error if overall series is too large (to be caught by caller)
        if grouping_size > max_series_size * BYTES_PER_GB:
            raise ValueError(
                f"Overlarge series: {self.cod_object.as_log} ({grouping_size} bytes) exceeds max_series_size: {max_series_size}gb"
            )
        # update append result
        self.append_result.errors.extend(errors)
        return filtered_instances

    def _dedupe(self, instances: list[Instance]) -> list[Instance]:
        """
        We expect uniqueness of instance ids within the input series.
        This method removes and records the paths to any duplicate instance files.
        ALL duplicates are removed, but dupe paths are only recorded if they are remote.
        (modifies instances list in place!)
        Returns:
            deduped_instances (list): list of instances with duplicates removed
        """
        instance_id_to_instance: dict[str, Instance] = {}
        same, conflict, errors = [], [], []
        for instance in instances:
            try:
                instance_id = instance.instance_uid(trust_hints_if_available=True)
                # handle duplicate instance id case
                if instance_id in instance_id_to_instance:
                    preexisting_instance = instance_id_to_instance[instance_id]
                    if (
                        instance.crc32c(trust_hints_if_available=True)
                        != preexisting_instance.crc32c()
                    ):
                        conflict.append(instance)
                        if instance.is_remote:
                            preexisting_instance.append_diff_hash_dupe(
                                instance.dicom_uri
                            )
                        logger.warning(
                            f"Removing diff hash dupe from input: {instance.as_log}"
                        )
                    else:
                        same.append(instance)
                        logger.warning(
                            f"Removing true duplicate from input: {instance.as_log}"
                        )
                    continue
                # if we make it here, we have a unique instance id
                instance_id_to_instance[instance_id] = instance
            except Exception as e:
                logger.exception(f"Error deduping instance: {instance.as_log}: {e}")
                errors.append((instance, e))
        # update append result
        self.append_result.same.extend(same)
        self.append_result.conflict.extend(conflict)
        self.append_result.errors.extend(errors)
        return list(instance_id_to_instance.values())

    def _calculate_state_change(self, instances: list[Instance]) -> StateChange:
        """For each file in the grouping, determine if it is NEW, SAME, or DIFF
        compared to the current series metadata json which contains instance_uid and crc32c values

        Returns:
            state_change (StateChange): namedtuple with the following fields:
                new (list): list of instance, series metadata, and deid instance uid tuples
                same (list): list of instance, series metadata, and deid instance uid tuples
                diff (list): list of instance, series metadata, and deid instance uid tuples
        """
        # TODO namedtuple?
        state_change = StateChange(new=[], same=[], diff=[])
        errors = []
        # If there is no preexisting series metadata, all files are new
        if len(self.cod_object._metadata.instances) == 0:
            for instance in instances:
                state_change.new.append((instance, None, None))
            return state_change, errors

        # Calculate state change for each file in the new series
        for new_instance in instances:
            try:
                # if deid instance id isn't in existing metadata dict, this file must be new
                instance_uid = new_instance.instance_uid(trust_hints_if_available=True)
                if instance_uid not in self.cod_object._metadata.instances:
                    state_change.new.append((new_instance, None, None))
                    continue

                # if we make it here, the instance id is in the existing metadata
                existing_instance = self.cod_object._metadata.instances[instance_uid]
                # if the crc32c is the same, we have a true duplicate
                if (
                    new_instance.crc32c(trust_hints_if_available=True)
                    == existing_instance.crc32c()
                ):
                    metrics.TRUE_DUPE_COUNTER.inc()
                    state_change.same.append(
                        (
                            new_instance,
                            self.cod_object._metadata,
                            instance_uid,
                        )
                    )
                # if the crc32c is different, we have a diff hash duplicate
                else:
                    metrics.DIFFHASH_DUPE_COUNTER.inc()
                    state_change.diff.append(
                        (
                            new_instance,
                            self.cod_object._metadata,
                            instance_uid,
                        )
                    )
            except Exception as e:
                logger.exception(e)
                errors.append((new_instance, e))
        # update append result
        self.append_result.errors.extend(errors)
        return state_change
