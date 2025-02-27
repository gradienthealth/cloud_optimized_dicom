import logging

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

from cloud_optimized_dicom.errors import LockAcquisitionError, LockVerificationError

logger = logging.getLogger(__name__)

# Handy way of keeping pylance happy without circular imports
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject

# TODO this should be generic for cod library, but for our existing codebase we need .gradient.lock... what to do?
LOCK_FILE_NAME = ".cod.lock"


class CODLocker:
    """Class for managing the lock file for a COD object.

    Args:
        cod_object (CODObject): The COD object to lock.
        lock_generation (int): (optional) The generation of the lock file to re-acquire if the lock was already known.
    """

    def __init__(self, cod_object: "CODObject", lock_generation: int = None):
        self.cod_object = cod_object
        self.lock_generation = lock_generation

    def acquire(self):
        """Upload a lock file (to prevent concurrent access to the COD object)."""
        # if the lock already exists, assert generation matches (re-acquisition case)
        if (lock_blob := self.get_lock_blob()).exists():
            lock_blob.reload()
            if lock_blob.generation != self.lock_generation:
                raise LockAcquisitionError(
                    "COD:LOCK:ACQUISITION_FAILED:DIFF_GEN_LOCK_ALREADY_EXISTS"
                )
            logger.info(
                f"COD:LOCK:REACQUIRED:gs://{lock_blob.bucket.name}/{lock_blob.name} (generation: {self.lock_generation})"
            )
            return

        # Step 1: fetch metadata
        self.cod_object.get_metadata()

        # Step 2: Try to create the lock file
        lock_blob.content_encoding = "gzip"
        try:
            lock_blob.upload_from_string(
                self.cod_object._metadata.to_gzipped_json(),
                content_type="application/json",
                if_generation_match=0,
            )
        except PreconditionFailed:
            raise LockAcquisitionError(
                "COD:LOCK:ACQUISITION_FAILED:STOLEN_DURING_METADATA_FETCH"
            )

        # Step 3: record lock generation
        self.lock_generation = lock_blob.generation
        logger.info(
            f"COD:LOCK:ACQUIRED:gs://{lock_blob.bucket.name}/{lock_blob.name} (generation: {self.lock_generation})"
        )

    def verify(self) -> storage.Blob:
        """Verify that the lock file still exists and has the same generation."""
        if not (lock_blob := self.get_lock_blob()).exists():
            msg = "COD:LOCK:MISSING_ON_VERIFY"
            logger.critical(msg)
            raise LockVerificationError(msg)
        lock_blob.reload()
        if lock_blob.generation != self.lock_generation:
            msg = f"COD:LOCK:GEN_MISMATCH_ON_VERIFY:FOUND:{lock_blob.generation} != EXPECTED:{self.lock_generation}"
            logger.critical(msg)
            raise LockVerificationError(msg)
        return lock_blob

    def release(self):
        """Release the lock by deleting the lock file."""
        try:
            lock_blob = self.verify()
        except LockVerificationError as e:
            logger.critical(f"COD:LOCK:RELEASE:VERIFICATION_ERROR:{e}")
            raise e
        lock_blob.delete()
        self._lock_generation = None
        logger.info(
            f"COD:LOCK:RELEASE:SUCCESS:gs://{lock_blob.bucket.name}/{lock_blob.name}"
        )

    def get_lock_blob(self) -> storage.Blob:
        """Get the lock blob for this series."""
        return storage.Blob.from_string(
            uri=f"{self.cod_object.datastore_path}/{self.cod_object.study_uid}/{self.cod_object.series_uid}/{LOCK_FILE_NAME}",
            client=self.cod_object.client,
        )
