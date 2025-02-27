import logging
import os
from typing import TYPE_CHECKING, NamedTuple

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.instance import Instance

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject


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
        self, instances: list[Instance], delete_local_origin: bool = False
    ) -> AppendResult:
        """Append a list of instances to the COD object.
        Args:
            instances (list): list of instances to append
            delete_local_origin (bool): whether to delete instance origin files after successful append (if local, remote origins are never deleted)
        Returns: an AppendResult; a namedtuple with the following fields:
            new (list): list of new instances that were added successfully
            same (list): list of instances that were perfect duplicates of existing instances
            conflict (list): list of instances that were the same instance UID but different hashes
            errors (list): list of instance, error tuples that occurred during the append process
        """
        self.append_result = AppendResult(new=[], same=[], conflict=[], errors=[])
        # remove overlarge instances
        instances, size_errors = self._assert_not_too_large(instances)
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
