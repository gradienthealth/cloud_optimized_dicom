# Handy way of keeping pylance happy without circular imports
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from google.api_core.exceptions import PreconditionFailed

from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.errors import LockAcquisitionError, LockVerificationError
from cloud_optimized_dicom.series_metadata import SeriesMetadata

if TYPE_CHECKING:
    from cloud_optimized_dicom.cod_object import CODObject

LOCK_FILE_NAME = ".gradient.lock"


class CODLocker:
    """Class for managing the lock file for a COD object.

    Args:
        cod_object (CODObject): The COD object to lock.
    """

    def __init__(self, cod_object: "CODObject"):
        self.cod_object = cod_object

    def acquire(
        self, create_if_missing: bool = True, empty_lock_override_age: float = None
    ):
        """Upload a lock file (to prevent concurrent access to the COD object).

        Args:
            create_if_missing (bool): Passthrough for CODObject.get_metadata (see documentation there)
            empty_lock_override_age (float): The age (in hours) of a lock file to consider "stale" and override (if it is empty)
        """
        blob_store = self.cod_object.blob_store
        lock_uri = self.lock_uri
        if (existing_lock := blob_store.stat(lock_uri)) is not None:
            # No override case
            if not empty_lock_override_age:
                raise LockAcquisitionError(
                    f"COD:LOCK:ACQUISITION_FAILED:DIFF_GEN_LOCK_ALREADY_EXISTS:{lock_uri} (generation: {existing_lock.generation})"
                )
            # Attempt override -> check whether lock old enough to override
            if datetime.now(timezone.utc) - existing_lock.updated < timedelta(
                hours=empty_lock_override_age
            ):
                raise LockAcquisitionError(
                    f"COD:LOCK:ACQUISITION_FAILED:LOCK_TOO_RECENT_TO_OVERRIDE:{lock_uri} (updated: {existing_lock.updated})"
                )
            # Attempt override -> check whether lock is empty
            lock_data = SeriesMetadata.from_bytes(blob_store.read_bytes(lock_uri))
            if len(lock_data.instances) > 0:
                raise LockAcquisitionError(
                    f"COD:LOCK:ACQUISITION_FAILED:CANNOT_OVERRIDE_NON_EMPTY_LOCK:{lock_uri} (instances: {len(lock_data.instances)})"
                )
            # If we make it here, we can safely override the lock
            blob_store.delete(lock_uri)
            logger.warning(f"COD:LOCK:OVERRIDING_STALE_LOCK:{lock_uri}")

        # Step 1: fetch metadata
        self.cod_object.get_metadata(create_if_missing=create_if_missing)

        # Step 2: Try to create the lock file
        try:
            written = blob_store.upload_bytes(
                lock_uri,
                self.cod_object._metadata.to_gzipped_json(),
                content_type="application/json",
                content_encoding="gzip",
                if_generation_match=0,
            )
        except PreconditionFailed:
            raise LockAcquisitionError(
                "COD:LOCK:ACQUISITION_FAILED:STOLEN_DURING_METADATA_FETCH"
            )

        # Step 3: record lock generation
        self.cod_object.lock_generation = written.generation
        logger.info(
            f"COD:LOCK:ACQUIRED:{lock_uri} (generation: {self.cod_object.lock_generation})"
        )

    def verify(self):
        """Verify that the lock file still exists and has the same generation."""
        if (lock := self.cod_object.blob_store.stat(self.lock_uri)) is None:
            msg = "COD:LOCK:MISSING_ON_VERIFY"
            logger.critical(msg)
            raise LockVerificationError(msg)
        if lock.generation != self.cod_object.lock_generation:
            msg = f"COD:LOCK:GEN_MISMATCH_ON_VERIFY:FOUND:{lock.generation} != EXPECTED:{self.cod_object.lock_generation}"
            logger.critical(msg)
            raise LockVerificationError(msg)

    def release(self):
        """Release the lock by deleting the lock file."""
        try:
            self.verify()
        except LockVerificationError as e:
            logger.critical(f"COD:LOCK:RELEASE:VERIFICATION_ERROR:{e}")
            raise e
        self.cod_object.blob_store.delete(self.lock_uri)
        self.cod_object.lock_generation = None
        logger.info(f"COD:LOCK:RELEASE:SUCCESS:{self.lock_uri}")

    @property
    def lock_uri(self) -> str:
        """The URI of the lock file for this series."""
        return f"{self.cod_object.datastore_series_uri}/{LOCK_FILE_NAME}"
