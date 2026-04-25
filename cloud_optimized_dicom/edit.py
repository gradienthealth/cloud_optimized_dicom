"""Edit-mode helpers for CODObject (mode='e').

Called from CODObject.__exit__ when mode='e' to validate the instance set is
unchanged, repack the tar, rebuild the sqlite index, and refresh SeriesMetadata
in-memory. Actual upload is handled by the caller via CODObject._sync().
"""

import os
import tarfile
from typing import TYPE_CHECKING

from cloud_optimized_dicom.append import _create_sqlite_index
from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.errors import EditSetChangedError
from cloud_optimized_dicom.thumbnail import DEFAULT_SIZE, generate_thumbnail

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject


def _validate_and_repack_for_edit(cod_object: "CODObject") -> None:
    """Validate the instance set is unchanged since context enter, re-read each modified
    DICOM from its local path, repack the tar, regenerate the sqlite index, refresh
    per-instance metadata, and (optionally) regenerate the thumbnail.

    Does NOT upload — that is _sync()'s job, which the caller invokes after this returns.

    Raises:
        EditSetChangedError: if any instance's local file has been deleted, or if the
            instance UID set (or study/series UID) on disk no longer matches what was
            loaded from metadata on context enter.
    """
    original_state = cod_object._edit_original_state
    assert original_state is not None, (
        "Edit-mode exit called without __enter__ having been called. "
        "CODObject instances in mode='e' must be used as a context manager."
    )

    instances = cod_object._get_instances(strict_sorting=False)

    # 1) existence check: user must not have deleted any local file
    for uid, instance in instances.items():
        if not os.path.exists(instance.dicom_uri):
            raise EditSetChangedError(
                f"Instance {uid} local file missing on edit-mode exit: {instance.dicom_uri}"
            )

    # 2) re-validate each instance from disk (recomputes crc32c, size, UIDs, has_pixeldata)
    for uid, instance in instances.items():
        instance._crc32c = None
        instance._size = None
        instance._instance_uid = None
        instance._series_uid = None
        instance._study_uid = None
        instance._has_pixeldata = None
        instance._dicom_metadata = None
        instance.validate()

    # 3) UID set must not have changed. Check per-instance UID against the metadata key
    #    (hashed if cod_object.hashed_uids) and study/series UIDs against cod_object.
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

    original_uids = set(original_state.keys())
    current_uids = set(instances.keys())
    if original_uids != current_uids:
        # Can only happen if someone reached into _metadata.instances directly — mode='e'
        # blocks append() so there's no public API that would do this. Defensive check.
        raise EditSetChangedError(
            f"Instance set changed during edit. "
            f"Added={current_uids - original_uids}, Removed={original_uids - current_uids}"
        )

    # 4) detect pixeldata change (file-level crc32c delta on any instance with pixeldata)
    cod_object._edit_pixeldata_changed = any(
        instance.crc32c() != original_state[uid]["crc32c"]
        and original_state[uid]["has_pixeldata"]
        for uid, instance in instances.items()
    )

    # 5) repack the tar from scratch (can't just overwrite entries — file sizes differ).
    #    Open in "a" (append) mode rather than "w", since Instance.append_to_series_tar
    #    reads back from the tar fileobj to locate the DICOM preamble — a write-only
    #    handle raises io.UnsupportedOperation. Removing the file first and relying on
    #    the tar_file_path property to recreate an empty tar gives us the same fresh
    #    state as "w" while remaining readable.
    if os.path.exists(cod_object.tar_file_path):
        os.remove(cod_object.tar_file_path)
    if os.path.exists(cod_object.index_file_path):
        os.remove(cod_object.index_file_path)
    with tarfile.open(cod_object.tar_file_path, "a") as tar:
        for instance in instances.values():
            instance.append_to_series_tar(tar)
    _create_sqlite_index(cod_object)
    logger.info(
        f"GRADIENT_STATE_LOGS:EDIT_MODE_REPACKED_TAR:{cod_object.tar_file_path} "
        f"({os.path.getsize(cod_object.tar_file_path)} bytes)"
    )

    # 6) rebuild per-instance DICOM metadata from the freshly-repacked tar. After
    #    append_to_series_tar, each instance's dicom_uri has been updated to point at
    #    the tar, and _byte_offsets reflect the new layout — so extract_metadata reads
    #    from the correct place.
    for instance in instances.values():
        instance._dicom_metadata = None
        instance.extract_metadata()

    # 7) thumbnail handling — only regenerate if caller opted in AND a thumbnail
    #    currently exists AND pixeldata actually changed.
    thumb_meta = cod_object._get_metadata_field("thumbnail")
    if (
        thumb_meta is not None
        and cod_object._regen_thumbnail_on_pd_change
        and cod_object._edit_pixeldata_changed
    ):
        thumbnail_size = thumb_meta.get("size", DEFAULT_SIZE)
        generate_thumbnail(
            cod_obj=cod_object,
            overwrite_existing=True,
            thumbnail_size=thumbnail_size,
        )

    # 8) flip sync flags — tar and metadata have both been rewritten locally and
    #    need to be uploaded. (_metadata_synced must be flipped AFTER the thumbnail
    #    step, since generate_thumbnail also flips it to False.)
    cod_object._tar_synced = False
    cod_object._metadata_synced = False
