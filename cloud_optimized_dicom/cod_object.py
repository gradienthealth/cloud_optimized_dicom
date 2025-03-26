import logging
import os
import tarfile
from tempfile import TemporaryDirectory

from google.cloud import storage
from google.cloud.storage.constants import STANDARD_STORAGE_CLASS
from google.cloud.storage.retry import DEFAULT_RETRY

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.appender import CODAppender
from cloud_optimized_dicom.errors import CODObjectNotFoundError
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.locker import CODLocker
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.utils import public_method, upload_and_count

logger = logging.getLogger(__name__)

EMPTY_TAR_SIZE = 10240


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
        # fields user must set
        datastore_path: str,
        client: storage.Client,
        study_uid: str,
        series_uid: str,
        lock: bool,
        # fields user can set but does not have to
        create_if_missing: bool = True,
        temp_dir: str = None,
        # fields user should not set
        lock_generation: int = None,
        metadata: SeriesMetadata = None,
    ):
        self.datastore_path = datastore_path
        self.client = client
        self.study_uid = study_uid
        self.series_uid = series_uid
        self._validate_uids()
        self._metadata = metadata
        self._temp_dir = temp_dir
        self._locker = CODLocker(self, lock_generation) if lock else None
        if self.lock:
            self._locker.acquire(create_if_missing=create_if_missing)
        else:
            self.get_metadata(create_if_missing=create_if_missing, dirty=True)
        self._tar_synced = False
        self._metadata_synced = True

    def _validate_uids(self):
        """Validate the UIDs are valid DICOM UIDs (TODO make this more robust, for now just check length)"""
        assert len(self.study_uid) >= 10, "Study UID must be 10 characters long"
        assert len(self.series_uid) >= 10, "Series UID must be 10 characters long"

    def _force_fetch_tar(self, fetch_index: bool = True):
        """Download the tarball (and index) from GCS.
        In some cases, like ingestion, we may not need the index as it will be recalculated.
        This method circumvents COD caching logic, which is why it's not public. Only use it if you know what you're doing.
        """
        tar_blob = storage.Blob.from_string(self.tar_uri, client=self.client)
        tar_blob.download_to_filename(self.tar_file_path)
        metrics.STORAGE_CLASS_COUNTERS["GET"][tar_blob.storage_class].inc()
        if fetch_index:
            index_blob = storage.Blob.from_string(self.index_uri, client=self.client)
            index_blob.download_to_filename(self.index_file_path)
            metrics.STORAGE_CLASS_COUNTERS["GET"][index_blob.storage_class].inc()
        # we just fetched the tar, so it is guaranteed to be in the same state as the datastore
        self._tar_synced = True

    @property
    def lock(self) -> bool:
        """Read-only property for lock status."""
        return self._locker is not None

    @property
    def as_log(self) -> str:
        """Return a string representation of the CODObject for logging purposes."""
        return f"{self.datastore_series_uri}"

    @property
    def temp_dir(self) -> TemporaryDirectory:
        """The path to the temporary directory for this series. Generates a new temp dir if it doesn't exist."""
        # make sure temp file exists
        if self._temp_dir is None:
            self._temp_dir = TemporaryDirectory(suffix=f"_{self.series_uid}")
        return self._temp_dir

    @property
    def tar_file_path(self) -> str:
        """The path to the tar file for this series in the temporary directory."""
        _tar_file_path = os.path.join(self.temp_dir.name, f"{self.series_uid}.tar")
        # create tar if it doesn't exist (needs to exist so we can open later in append mode)
        if not os.path.exists(_tar_file_path):
            with tarfile.open(_tar_file_path, "w"):
                pass
        return _tar_file_path

    @property
    def index_file_path(self) -> str:
        """The path to the index file for this series in the temporary directory."""
        return os.path.join(self.temp_dir.name, f"index.sqlite")

    @public_method
    def get_metadata(
        self, create_if_missing: bool = True, dirty: bool = False
    ) -> SeriesMetadata:
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
        elif create_if_missing:
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
        self,
        instances: list[Instance],
        max_instance_size: float = 10,
        max_series_size: float = 100,
        delete_local_origin: bool = False,
        dirty: bool = False,
    ):
        """Append a list of instances to the COD object.

        Args:
            instances: list[Instance] - The instances to append.
            max_instance_size: float - The maximum size of an instance to append, in gb.
            max_series_size: float - The maximum size of the series to append, in gb.
            delete_local_origin: bool - If `True`, delete the local origin of the instances after appending.
            dirty: bool - Must be `True` if the CODObject is "dirty" (i.e. `lock=False`).
        """
        return CODAppender(self).append(
            instances=instances,
            delete_local_origin=delete_local_origin,
            max_instance_size=max_instance_size,
            max_series_size=max_series_size,
        )

    @public_method
    def sync(self, tar_storage_class: str = STANDARD_STORAGE_CLASS):
        """Sync tar+index and/or metadata to GCS, as needed

        Args:
            tar_storage_class: str - Storage class to use for the tar file (default: `STANDARD`).
            See `google.cloud.storage.constants` for options.
        """
        # prior to sync, make some assertions
        if self._tar_synced and self._metadata_synced:
            logger.warning(f"Nothing to sync: {self.as_log}")
            return
        # design choice: it's worth the API call to verify lock prior to sync
        # TODO consider removing this if we never see lock changes in the wild
        self._locker.verify()
        # sync metadata
        if not self._metadata_synced:
            assert (
                self._metadata
            ), "Metadata sync attempted but CODObject has no metadata"
            self._gzip_and_upload_metadata()
            self._metadata_synced = True
        # sync tar
        if not self._tar_synced:
            if os.path.getsize(self.tar_file_path) == EMPTY_TAR_SIZE:
                logger.warning(f"Skipping tar sync - tar is empty: {self.as_log}")
                return
            assert os.path.exists(
                self.index_file_path
            ), "Tar sync attempted but CODObject has no index"
            tar_blob = storage.Blob.from_string(self.tar_uri, client=self.client)
            tar_blob.storage_class = tar_storage_class
            index_blob = storage.Blob.from_string(self.index_uri, client=self.client)
            upload_and_count(index_blob, self.index_file_path)
            upload_and_count(tar_blob, self.tar_file_path)
            self._tar_synced = True
        # single overall sync message
        logger.info(f"GRADIENT_STATE_LOGS:SYNCED_SUCCESSFULLY:{self.as_log}")

    def _gzip_and_upload_metadata(self):
        """
        Given a SeriesMetadata object and a blob to upload it to, convert the object to JSON, gzip it,
        and upload it to the blob
        """
        metadata_blob = storage.Blob.from_string(self.metadata_uri, client=self.client)
        metadata_blob.content_encoding = "gzip"
        compressed_metadata = self._metadata.to_gzipped_json()
        metadata_blob.upload_from_string(
            compressed_metadata, content_type="application/json", retry=DEFAULT_RETRY
        )
        metrics.STORAGE_CLASS_COUNTERS["CREATE"][metadata_blob.storage_class].inc()

    @property
    def datastore_series_uri(self) -> str:
        """The URI of the series in the COD datastore."""
        return os.path.join(
            self.datastore_path, "studies", self.study_uid, "series", self.series_uid
        )

    @property
    def tar_uri(self) -> str:
        """The URI of the tar file for this series in the COD datastore."""
        return f"{self.datastore_series_uri}.tar"

    @property
    def metadata_uri(self) -> str:
        """The URI of the metadata file for this series in the COD datastore."""
        return os.path.join(self.datastore_series_uri, "metadata.json")

    @property
    def index_uri(self) -> str:
        """The URI of the index file for this series in the COD datastore."""
        return os.path.join(self.datastore_series_uri, "index.sqlite")

    def __str__(self):
        return f"CODObject({self.datastore_series_uri})"

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
