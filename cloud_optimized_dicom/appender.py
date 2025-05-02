import logging
import os
import tarfile
from typing import TYPE_CHECKING, NamedTuple, Optional

from ratarmountcore import open as rmc_open

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.utils import is_remote

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
        # remove instances that do not belong to the COD object
        instances = self._assert_instances_belong_to_cod_obj(instances)
        # Calculate state change as a result of instances added by this group
        state_change = self._calculate_state_change(instances)
        # handle same
        self._handle_same(state_change.same)
        # Edge case: no NEW or DIFF state changes -> return early
        if not state_change.new and not state_change.diff:
            logger.warning(f"No new instances: {self.cod_object.as_log}")
            metrics.SERIES_DUPE_COUNTER.inc()
            return self.append_result
        # handle diff
        self._handle_diff(state_change.diff)
        # Edge case: no NEW state changes, but some DIFFs -> return early
        if not state_change.new:
            return self.append_result
        # handle new
        self._handle_new(state_change.new)
        metrics.TAR_SUCCESS_COUNTER.inc()
        metrics.TAR_BYTES_PROCESSED.inc(os.path.getsize(self.cod_object.tar_file_path))
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
                        if is_remote(instance.dicom_uri):
                            preexisting_instance.append_diff_hash_dupe(
                                instance.dicom_uri
                            )
                        logger.warning(
                            f"Removing diff hash dupe from input: {instance}"
                        )
                    else:
                        same.append(instance)
                        logger.warning(
                            f"Removing true duplicate from input: {instance}"
                        )
                    continue
                # if we make it here, we have a unique instance id
                instance_id_to_instance[instance_id] = instance
            except Exception as e:
                logger.exception(f"Error deduping instance: {instance}: {e}")
                errors.append((instance, e))
        # update append result
        self.append_result.same.extend(same)
        self.append_result.conflict.extend(conflict)
        self.append_result.errors.extend(errors)
        return list(instance_id_to_instance.values())

    def _assert_instances_belong_to_cod_obj(self, instances: list[Instance]):
        """
        Assert that all instances belong to the COD object.
        """
        instances_in_series = []
        for instance in instances:
            # deliberately try/catch assertion to add error instances to append result
            try:
                self.cod_object.assert_instance_belongs_to_cod_object(instance)
                instances_in_series.append(instance)
            except Exception as e:
                logger.exception(e)
                self.append_result.errors.append((instance, e))
        return instances_in_series

    def _get_instance_uid_for_comparison(
        self, instance: Instance, trust_hints_if_available: bool = False
    ) -> str:
        """
        Get the instance uid for comparison. If the cod object uses hashed uids,
        return the hashed uid, otherwise return the standard uid.
        """
        return (
            instance.hashed_instance_uid(
                trust_hints_if_available=trust_hints_if_available
            )
            if self.cod_object.hashed_uids
            else instance.instance_uid(
                trust_hints_if_available=trust_hints_if_available
            )
        )

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
            return state_change

        # Calculate state change for each file in the new series
        for new_instance in instances:
            try:
                # if deid instance id isn't in existing metadata dict, this file must be new
                instance_uid = self._get_instance_uid_for_comparison(
                    new_instance, trust_hints_if_available=True
                )
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

    def _handle_same(
        self,
        same_state_changes: list[
            tuple[Instance, Optional[SeriesMetadata], Optional[str]]
        ],
    ):
        """Log a warning for each instance that is the same as a previous instance, and update append result"""
        for dupe_instance, series_metadata, deid_instance_uid in same_state_changes:
            existing_path = series_metadata.instances[deid_instance_uid].dicom_uri
            logger.warning(
                f"Skipping duplicate instance (same hash): {dupe_instance} (duplicate of {existing_path})"
            )
        # update append result
        self.append_result.same.extend([same for same, _, _ in same_state_changes])

    def _handle_diff(
        self,
        diff_state_changes: list[
            tuple[Instance, Optional[SeriesMetadata], Optional[str]]
        ],
    ):
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
            if existing_instance.append_diff_hash_dupe(dupe_instance):
                # metadata is now desynced because we added to diff_hash_dupe_paths
                self.cod_object._metadata_synced = False

        # update append result
        self.append_result.conflict.extend([diff for diff, _, _ in diff_state_changes])

    def _handle_new(
        self,
        new_state_changes: list[
            tuple[Instance, Optional[SeriesMetadata], Optional[str]]
        ],
    ):
        """
        Create/append to tar & upload; add to series metadata & upload.
        Returns:
            new (list): list of new instances that were added successfully
        """
        instances_added_to_tar = self._handle_create_tar(new_state_changes)
        self._handle_create_metadata(instances_added_to_tar)
        # update append result
        self.append_result.new.extend(instances_added_to_tar)

    def _handle_create_tar(
        self, new_state_changes: list[tuple[Instance, SeriesMetadata, str]]
    ) -> list[Instance]:
        """
        Create/append to tar + index.sqlite locally
        Returns:
            instances_added_to_tar (list): list of instances that got added to the tar successfully
        """
        # If a tarball already exists (and this is a clean append), download it (no need to get index, will be recalculated anyways)
        if len(self.cod_object._metadata.instances) > 0 and self.cod_object.lock:
            self.cod_object._force_fetch_tar(fetch_index=False)

        instances_added_to_tar = self._create_or_append_tar(
            [new for new, _, _ in new_state_changes]
        )
        self._create_sqlite_index()
        return instances_added_to_tar

    def _create_or_append_tar(self, instances_to_add: list[Instance]) -> list[Instance]:
        """Create/append to `cod_object.tar_file_path` all instances in `instances_to_add`

        Returns:
            instances_added_to_tar (list): instances that were successfully added to the tar
        Raises:
            ValueError: if no instances were successfully added to the tar
        """
        # validate that at least one instance is being added
        assert len(instances_to_add) > 0, "No instances to add to tar"
        # create/append to tar
        instances_added_to_tar, errors = [], []
        with tarfile.open(self.cod_object.tar_file_path, "a") as tar:
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
            raise ValueError(
                f"GRADIENT_STATE_LOGS:FAILED_TO_TAR_ALL_INSTANCES:{uri_str}"
            )
        logger.info(
            f"GRADIENT_STATE_LOGS:POPULATED_TAR:{self.cod_object.tar_file_path} ({os.path.getsize(self.cod_object.tar_file_path)} bytes)"
        )
        # tar has been altered, so it is no longer in sync with the datastore
        self.cod_object._tar_synced = False
        # update append result
        self.append_result.errors.extend(errors)
        return instances_added_to_tar

    def _create_sqlite_index(self):
        """
        Given a tar on disk, open it with ratarmountcore and save the index to `cod_object.index_file_path`.
        """
        # index needs to be recreated if it already exists
        if os.path.exists(self.cod_object.index_file_path):
            os.remove(self.cod_object.index_file_path)
        # explicitly bypass property getter to avoid AttributeError: does not exist
        with rmc_open(
            self.cod_object.tar_file_path,
            writeIndex=True,
            indexFilePath=self.cod_object.index_file_path,
        ):
            pass

    def _handle_create_metadata(
        self,
        instances_added_to_tar: list[Instance],
    ):
        """Update metadata locally with new instances.
        Do not catch errors; any exceptions here should bubble up as they represent a desync between tar and metadata
        """
        # Add new instances to metadata
        for instance in instances_added_to_tar:
            # get hashed uid if series is hashed, standard if not
            uid = (
                instance.hashed_instance_uid()
                if self.cod_object.hashed_uids
                else instance.instance_uid()
            )
            # TODO: deid?
            output_uri = f"{self.cod_object.tar_uri}://instances/{uid}.dcm"
            instance.extract_metadata(output_uri)
            instance.dicom_uri = output_uri
            self.cod_object._metadata.instances[uid] = instance
        # if we added any instances, metadata is now desynced
        self.cod_object._metadata_synced = (
            False if len(instances_added_to_tar) > 0 else True
        )
