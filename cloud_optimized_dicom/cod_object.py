import logging

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

from cloud_optimized_dicom.errors import (
    CleanOpOnUnlockedCODObjectError,
    CODObjectNotFoundError,
    LockAcquisitionError,
    LockVerificationError,
)
from cloud_optimized_dicom.series_metadata import SeriesMetadata

logger = logging.getLogger(__name__)

# TODO this should be generic for cod library, but for our existing codebase we need .gradient.lock... what to do?
LOCK_FILE_NAME = ".cod.lock"


def public_method(func):
    """Decorator for public CODObject methods.
    Enforces that clean operations require a lock, and warns about dirty operations on locked objects.
    """

    def wrapper(self, *args, **kwargs):
        dirty = kwargs.get("dirty", False)
        if not dirty:
            if not self.lock:
                raise CleanOpOnUnlockedCODObjectError(
                    "Cannot perform clean operation on unlocked CODObject"
                )
        elif self.lock:
            logger.warning(f"Performing dirty operation on locked CODObject: {self}")
        return func(self, *args, **kwargs)

    return wrapper


class CODObject:
    """
    A Logical representation of a DICOM series stored in the cloud.

    NOTE: The UIDs provided on initialization are used directly in COD URIs (e.g. `<datastore_path>/<study_uid>/<series_uid>.tar`)
    SO, if these UIDs are supposed to be de-identified, the caller is responsible for this de-identification.

    Parameters:
        datastore_path: str - The path to the datastore file for this series.
        client: storage.Client - The client to use to interact with the datastore.
        study_uid: str - The study_uid of the series.
        series_uid: str - The series_uid of the series.
        lock: bool - If `True`, acquire a lock on initialization. If `False`, no changes made on this object will be synced to the datastore.
        create_if_missing: bool - If `False`, raise an error if series does not yet exist in the datastore.
        lock_generation: int - The generation of the lock file. Should only be set if instantiation from serialized cod object.
    """

    def __init__(
        self,
        # fields user should set
        datastore_path: str,
        client: storage.Client,
        study_uid: str,
        series_uid: str,
        lock: bool,
        create_if_missing: bool = True,
        # fields user should not set
        lock_generation: int = None,
        metadata: SeriesMetadata = None,
    ):
        self.datastore_path = datastore_path
        self.client = client
        self.study_uid = study_uid
        self.series_uid = series_uid
        self.create_if_missing = create_if_missing
        self._lock = lock
        self._lock_generation = lock_generation
        self._metadata = metadata
        self._validate_uids()
        if self._lock:
            self._acquire_lock()

    def _validate_uids(self):
        """Validate the UIDs are valid DICOM UIDs (TODO make this more robust, for now just check length)"""
        assert len(self.study_uid) >= 10, "Study UID must be 10 characters long"
        assert len(self.series_uid) >= 10, "Series UID must be 10 characters long"

    def _acquire_lock(self):
        """Upload a lock file (to prevent concurrent access to the COD object).
        Store the lock's generation for future verification"""
        # if the lock already exists, assert generation matches (re-acquisition case)
        if (lock_blob := self.get_lock_blob()).exists():
            lock_blob.reload()
            if lock_blob.generation != self._lock_generation:
                raise LockAcquisitionError(
                    "COD:LOCK:ACQUISITION_FAILED:DIFF_GEN_LOCK_ALREADY_EXISTS"
                )
            logger.info(
                f"COD:LOCK:REACQUIRED:gs://{lock_blob.bucket.name}/{lock_blob.name} (generation: {self._lock_generation})"
            )
            return
        # if lock doesn't exist, we are free to make a new one
        # Step 1: fetch metadata
        self.get_metadata()

        # Step 2: Try to create the lock file with a precondition that it must not exist
        lock_blob.content_encoding = "gzip"
        try:
            lock_blob.upload_from_string(
                self._metadata.to_gzipped_json(),
                content_type="application/json",
                if_generation_match=0,  # Only upload if the blob doesn't exist
            )
        except PreconditionFailed:
            # we specified a precondition of gen=0 (blob doesn't exist). Therefore, if we get a PreconditionFailed
            # exception, another process must have created the lock file while we were fetching metadata
            raise LockAcquisitionError(
                "COD:LOCK:ACQUISITION_FAILED:STOLEN_DURING_METADATA_FETCH"
            )

        # Step 3: record lock generation
        self._lock_generation = lock_blob.generation
        logger.info(
            f"COD:LOCK:ACQUIRED:gs://{lock_blob.bucket.name}/{lock_blob.name} (generation: {self._lock_generation})"
        )

    def _verify_lock(self) -> storage.Blob:
        """Verify that the lock file still exists and has the same generation.
        Returns the lock blob on successful verification"""
        if not (lock_blob := self.get_lock_blob()).exists():
            msg = "COD:LOCK:MISSING_ON_VERIFY"
            logger.critical(msg)
            raise LockVerificationError(msg)
        lock_blob.reload()
        if lock_blob.generation != self._lock_generation:
            msg = f"COD:LOCK:GEN_MISMATCH_ON_VERIFY:FOUND:{lock_blob.generation} != EXPECTED:{self._lock_generation}"
            logger.critical(msg)
            raise LockVerificationError(msg)
        return lock_blob

    def _release_lock(self):
        """Release the lock by deleting the lock file"""
        try:
            lock_blob = self._verify_lock()
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
            uri=f"{self.datastore_path}/{self.study_uid}/{self.series_uid}/{LOCK_FILE_NAME}",
            client=self.client,
        )

    @public_method
    def get_metadata(self, **kwargs) -> SeriesMetadata:
        """Get the metadata for this series."""
        # early exit if metadata is already set
        if self._metadata is not None:
            return self._metadata
        # fetch metadata from datastore
        metadata_blob = storage.Blob.from_string(
            uri=self.metadata_uri,
            client=self.client,
        )
        if metadata_blob.exists():
            self._metadata = SeriesMetadata.from_blob(metadata_blob)
        elif self.create_if_missing:
            self._metadata = SeriesMetadata(
                study_uid=self.study_uid, series_uid=self.series_uid
            )
        else:
            raise CODObjectNotFoundError(
                f"COD:OBJECT_NOT_FOUND:{self.metadata_uri} (create_if_missing=False)"
            )
        return self._metadata

    @property
    def lock(self) -> bool:
        """Read-only property for lock status."""
        return self._lock

    @property
    def tar_uri(self) -> str:
        """The URI of the tar file for this series in the COD datastore."""
        return f"{self.datastore_path}/{self.study_uid}/{self.series_uid}.tar"

    @property
    def metadata_uri(self) -> str:
        """The URI of the metadata file for this series in the COD datastore."""
        return f"{self.datastore_path}/{self.study_uid}/{self.series_uid}/metadata.json"

    @property
    def index_uri(self) -> str:
        """The URI of the index file for this series in the COD datastore."""
        return f"{self.datastore_path}/{self.study_uid}/{self.series_uid}/index.sqlite"

    def __str__(self):
        return f"CODObject({self.datastore_path}/{self.study_uid}/{self.series_uid})"

    def __enter__(self):
        """Context manager entry point"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point - release the lock, clean up temp dir"""
        if self.lock:
            # If no exception occurred, release the lock
            if exc_type is None:
                self._release_lock()
            # If an exception occurred, log it and leave the lock hanging
            else:
                logger.warning(
                    f"GRADIENT_STATE_LOGS:LOCK:LEFT_HANGING_DUE_TO_EXCEPTION:{str(self)}:{exc_type} {exc_val}"
                )
        # Regardless of exception(s), we still want to clean up the temp dir
        # self.cleanup_temp_dir() TODO reimplement
        return False  # Don't suppress any exceptions
