"""Edit-mode helpers for CODObject (mode='e').

Called from CODObject.__exit__ when mode='e' to validate the instance set is
unchanged, repack the tar, rebuild the sqlite index, and refresh SeriesMetadata
in-memory. Actual upload is handled by the caller via CODObject._sync().
"""

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from cloud_optimized_dicom.append import _pack_and_index, _refresh_per_instance_metadata
from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.errors import EditSetChangedError
from cloud_optimized_dicom.thumbnail import DEFAULT_SIZE, generate_thumbnail

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject
    from cloud_optimized_dicom.instance import Instance


@dataclass(frozen=True)
class _InstanceSnapshot:
    """Per-instance state captured on edit-mode context enter, used on exit
    to detect what changed during the edit."""

    crc32c: str
    has_pixeldata: bool


@dataclass
class EditState:
    """State maintained for the duration of a `mode='e'` context.

    Populated in `CODObject.__enter__` (snapshots taken from each instance
    immediately after the tar is fetched, before extraction to local temp
    files) and consumed in `_validate_and_repack_for_edit` on exit.
    """

    snapshots: dict[str, _InstanceSnapshot] = field(default_factory=dict)
    pixeldata_changed: bool = False

    @classmethod
    def snapshot(cls, instances: dict[str, "Instance"]) -> "EditState":
        """Capture pre-edit state from each instance."""
        return cls(
            snapshots={
                uid: _InstanceSnapshot(
                    crc32c=inst.crc32c(), has_pixeldata=inst.has_pixeldata
                )
                for uid, inst in instances.items()
            }
        )

    def compute_pixeldata_changed(self, instances: dict[str, "Instance"]) -> bool:
        """Did any instance with pixeldata get a new file-level crc32c since the
        snapshot was taken? (Conservative: any byte-level change to a DICOM that
        contains PixelData counts, even if only tags were edited.)"""
        return any(
            inst.crc32c() != self.snapshots[uid].crc32c
            and self.snapshots[uid].has_pixeldata
            for uid, inst in instances.items()
        )


def _assert_local_files_present(instances: dict[str, "Instance"]) -> None:
    """Each instance's local file must still exist (the user can't `rm` a file
    extracted into the edit-mode temp dir and expect the repack to silently skip it)."""
    for uid, instance in instances.items():
        if not os.path.exists(instance.dicom_uri):
            raise EditSetChangedError(
                f"Instance {uid} local file missing on edit-mode exit: {instance.dicom_uri}"
            )


def _revalidate_instances_from_disk(instances: dict[str, "Instance"]) -> None:
    """Clear cached per-instance fields and re-run `validate()` so crc32c, size,
    has_pixeldata, and the three UIDs reflect whatever the user just wrote to disk."""
    for instance in instances.values():
        instance._crc32c = None
        instance._size = None
        instance._instance_uid = None
        instance._series_uid = None
        instance._study_uid = None
        instance._has_pixeldata = None
        instance._dicom_metadata = None
        instance.validate()


def _assert_uid_set_unchanged(
    cod_object: "CODObject",
    edit_state: EditState,
    instances: dict[str, "Instance"],
) -> None:
    """No instance UID may have changed, and the dict-key set must match the snapshot.

    Per-instance: the instance's freshly-read UID must still equal the metadata key
    it lives under (and study/series UIDs must still belong to this CODObject).
    Set-level: nobody added or removed an instance — `mode='e'` blocks `append()`
    so this is mostly a defensive check against direct `_metadata.instances` mutation.
    """
    for uid, instance in instances.items():
        new_uid = instance.get_instance_uid(
            hashed=cod_object.hashed_uids, trust_hints_if_available=False
        )
        if new_uid != uid:
            raise EditSetChangedError(
                f"Instance UID changed during edit: was {uid}, file now reports {new_uid} "
                f"(dicom_uri={instance.dicom_uri})"
            )
        cod_object.assert_instance_belongs_to_cod_object(
            instance, trust_hints_if_available=False
        )

    original_uids = set(edit_state.snapshots.keys())
    current_uids = set(instances.keys())
    if original_uids != current_uids:
        raise EditSetChangedError(
            f"Instance set changed during edit. "
            f"Added={current_uids - original_uids}, Removed={original_uids - current_uids}"
        )


def _repack_tar_and_index(
    cod_object: "CODObject", instances: dict[str, "Instance"]
) -> None:
    """Delete the existing local tar + sqlite index, then re-pack from the (already-
    validated) instances. Strict — any per-instance failure bubbles, since instances
    are known-good on disk by the time we get here, so anything else is a real desync.
    """
    if os.path.exists(cod_object.tar_file_path):
        os.remove(cod_object.tar_file_path)
    if os.path.exists(cod_object.index_file_path):
        os.remove(cod_object.index_file_path)
    _pack_and_index(cod_object, instances.values(), tolerate_per_instance_errors=False)
    logger.info(
        f"GRADIENT_STATE_LOGS:EDIT_MODE_REPACKED_TAR:{cod_object.tar_file_path} "
        f"({os.path.getsize(cod_object.tar_file_path)} bytes)"
    )


def _maybe_regen_thumbnail(cod_object: "CODObject", edit_state: EditState) -> None:
    """Regenerate the thumbnail iff one exists and pixeldata changed."""
    thumb_meta = cod_object._get_metadata_field("thumbnail")
    if thumb_meta is not None and edit_state.pixeldata_changed:
        thumbnail_size = thumb_meta.get("size", DEFAULT_SIZE)
        generate_thumbnail(
            cod_obj=cod_object,
            overwrite_existing=True,
            thumbnail_size=thumbnail_size,
        )


def _validate_and_repack_for_edit(cod_object: "CODObject") -> None:
    """Validate the instance set is unchanged since context enter, re-read each
    modified DICOM from its local path, repack the tar, regenerate the sqlite
    index, refresh per-instance metadata, and (optionally) regenerate the thumbnail.

    Does NOT upload — that is _sync()'s job, which the caller invokes after this returns.

    Raises:
        EditSetChangedError: if any instance's local file has been deleted, or if the
            instance UID set (or study/series UID) on disk no longer matches what was
            loaded from metadata on context enter.
    """
    edit_state = cod_object._edit_state
    assert edit_state is not None, (
        "Edit-mode exit called without __enter__ having been called. "
        "CODObject instances in mode='e' must be used as a context manager."
    )

    instances = cod_object._get_instances(strict_sorting=False)

    _assert_local_files_present(instances)
    _revalidate_instances_from_disk(instances)
    _assert_uid_set_unchanged(cod_object, edit_state, instances)
    edit_state.pixeldata_changed = edit_state.compute_pixeldata_changed(instances)

    _repack_tar_and_index(cod_object, instances)
    # bulk-data refs in the rebuilt metadata must use the REMOTE per-instance URI,
    # not the local tar path that _pack_and_index just set as instance.dicom_uri.
    _refresh_per_instance_metadata(cod_object, instances.values())
    _maybe_regen_thumbnail(cod_object, edit_state)

    # tar and metadata have both been rewritten locally; flag for upload by _sync()
    cod_object._tar_synced = False
    cod_object._metadata_synced = False
