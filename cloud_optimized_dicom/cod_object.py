import logging

from google.cloud import storage

from cloud_optimized_dicom.appender import CODAppender
from cloud_optimized_dicom.errors import CODObjectNotFoundError
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.locker import CODLocker
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.utils import public_method

logger = logging.getLogger(__name__)


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
        self._validate_uids()
        self.create_if_missing = create_if_missing
        self._metadata = metadata
        self._locker = CODLocker(self, lock_generation) if lock else None
        if self.lock:
            self._locker.acquire()

    def _validate_uids(self):
        """Validate the UIDs are valid DICOM UIDs (TODO make this more robust, for now just check length)"""
        assert len(self.study_uid) >= 10, "Study UID must be 10 characters long"
        assert len(self.series_uid) >= 10, "Series UID must be 10 characters long"

    @property
    def lock(self) -> bool:
        """Read-only property for lock status."""
        return self._locker is not None

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

    @public_method
    def append(
        self, instances: list[Instance], delete_local_origin: bool = False, **kwargs
    ):
        """Append a list of instances to the COD object.

        Args:
            instances: list[Instance] - The instances to append.
            delete_local_origin: bool - If `True`, delete the local origin of the instances after appending.
            dirty: bool - Must be `True` if the CODObject is "dirty" (i.e. `lock=False`).
        """
        CODAppender(self).append(
            instances=instances, delete_local_origin=delete_local_origin
        )

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
                self._locker.release()
            # If an exception occurred, log it and leave the lock hanging
            else:
                logger.warning(
                    f"GRADIENT_STATE_LOGS:LOCK:LEFT_HANGING_DUE_TO_EXCEPTION:{str(self)}:{exc_type} {exc_val}"
                )
        # Regardless of exception(s), we still want to clean up the temp dir
        # self.cleanup_temp_dir() TODO reimplement
        return False  # Don't suppress any exceptions
