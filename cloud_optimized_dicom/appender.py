import logging
import os
from typing import TYPE_CHECKING, NamedTuple

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.instance import Instance

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


class CODAppender:
    """Class for appending DICOM files to a COD object.
    Designed to be instantiated by CODObject.append() and used once."""

    def __init__(self, cod_object: "CODObject"):
        self.cod_object = cod_object
        self.append_result = None

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
        instances, size_errors = self._assert_not_too_large(
            instances, max_instance_size, max_series_size
        )
        # remove duplicates from input
        instances, same_dedupes, conflict_dedupes, dedupe_errors = self._dedupe(
            instances
        )
        # Calculate state change as a result of instances added by this group
        state_changes, state_change_errors = self._calculate_state_change(instances)
        # handle same
        self._handle_true_duplicates(state_changes["SAME"])
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
    ) -> tuple[list[Instance], list[tuple[Instance, Exception]]]:
        """Performs 2 size validations:
        1. None of the individual instances are too large
        2. The overall series size is not too large

        Args:
            instances (list): list of instances to validate
            max_instance_size (float): maximum size of an instance to append, in gb
            max_series_size (float): maximum size of the series to append, in gb
        Returns:
            filtered_instances (list): list of instances that are not too large
            errors (list): list of instance, error tuples (likely overlarge instances,
            but there could be fetching-related errors)
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
        return filtered_instances, errors
