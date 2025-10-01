import os
import shutil
import tarfile
from tempfile import mkdtemp
from typing import Callable, Optional, Union

import numpy as np
from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud.storage.constants import STANDARD_STORAGE_CLASS
from google.cloud.storage.retry import DEFAULT_RETRY
from ratarmountcore import open as rmc_open

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.append import append
from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.errors import (
    CODObjectNotFoundError,
    ErrorLogExistsError,
    HashMismatchError,
    InstanceValidationError,
    TarMissingInstanceError,
    TarValidationError,
)
from cloud_optimized_dicom.instance import Instance
from cloud_optimized_dicom.locker import CODLocker
from cloud_optimized_dicom.series_metadata import SeriesMetadata
from cloud_optimized_dicom.thumbnail import (
    DEFAULT_SIZE,
    fetch_thumbnail,
    generate_thumbnail,
    get_instance_by_thumbnail_index,
    get_instance_thumbnail_slice,
)
from cloud_optimized_dicom.truncate import remove, truncate
from cloud_optimized_dicom.utils import (
    generate_ptr_crc32c,
    is_remote,
    public_method,
    read_thumbnail_into_array,
    upload_and_count_file,
)


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
        hashed_uids: bool - Flag whether UIDs are hashed. If `True`, Instances appended to this CODObject must have a `uid_hash_func`.
        create_if_missing: bool - If `False`, raise an error if series does not yet exist in the datastore.
        temp_dir: str - If a temp_dir with data pertaining to this series already exists, provide it here.
        override_errors: bool - If `True`, delete any existing error.log and upload a new one.
        empty_lock_override_age: float - If `None`, do not override a stale lock if it exists. If `float`, override a stale lock if it exists and is older than the given age (in hours).
        lock_generation: int - The generation of the lock file. Should only be set if instantiation from serialized cod object.
    """

    # Constructor and basic validation
    def __init__(
        self,
        # fields user must set
        datastore_path: str,
        client: storage.Client,
        study_uid: str,
        series_uid: str,
        lock: bool,
        # fields user can set but does not have to
        hashed_uids: bool = False,
        create_if_missing: bool = True,
        temp_dir: str = None,
        override_errors: bool = False,
        empty_lock_override_age: float = None,
        # fields user should not set
        lock_generation: int = None,
        metadata: SeriesMetadata = None,
        _tar_synced: bool = False,
        _metadata_synced: bool = True,
        _thumbnail_synced: bool = False,
    ):
        self.datastore_path = datastore_path
        self.client = client
        self.study_uid = study_uid
        self.series_uid = series_uid
        self._validate_uids()
        self.hashed_uids = hashed_uids
        self._metadata = metadata
        self.temp_dir = temp_dir
        self.override_errors = override_errors
        self.lock_generation = lock_generation
        # check for error.log existence - if it exists, fail initialization
        if (
            error_log_blob := storage.Blob.from_string(
                self.error_log_uri, client=self.client
            )
        ).exists():
            if self.override_errors:
                error_log_blob.delete()
                logger.warning(f"Deleted existing error log: {self.error_log_uri}")
            else:
                raise ErrorLogExistsError(
                    f"Cannot initialize; error log exists: {self.error_log_uri}"
                )
        self._locker = CODLocker(self) if lock else None
        if self.lock:
            self._locker.acquire(
                create_if_missing=create_if_missing,
                empty_lock_override_age=empty_lock_override_age,
            )
        else:
            self.get_metadata(create_if_missing=create_if_missing, dirty=True)
        self._tar_synced = _tar_synced
        self._metadata_synced = _metadata_synced
        # if the thumbnail exists, it is not synced (we did not fetch it)
        self._thumbnail_synced = (
            self.get_metadata_field("thumbnail", dirty=not lock) is None
        )

    def _validate_uids(self):
        """Validate the UIDs are valid DICOM UIDs (TODO make this more robust, for now just check length)"""
        assert len(self.study_uid) >= 10, "Study UID must be 10 characters long"
        assert len(self.series_uid) >= 10, "Series UID must be 10 characters long"

    # Core properties and getters
    @property
    def lock(self) -> bool:
        """Read-only property for lock status."""
        return self._locker is not None

    # Temporary directory management
    def get_temp_dir(self) -> str:
        """The path to the temporary directory for this series. Generates a new temp dir if it doesn't exist."""
        # make sure temp dir exists
        if self.temp_dir is None or not os.path.exists(self.temp_dir):
            self.temp_dir = mkdtemp(suffix=f"_{self.series_uid}")
        return self.temp_dir

    @property
    def tar_file_path(self) -> str:
        """The path to the tar file for this series in the temporary directory."""
        _tar_file_path = os.path.join(self.get_temp_dir(), f"{self.series_uid}.tar")
        # create tar if it doesn't exist (needs to exist so we can open later in append mode)
        if not os.path.exists(_tar_file_path):
            with tarfile.open(_tar_file_path, "w"):
                pass
        return _tar_file_path

    @property
    def tar_is_empty(self) -> bool:
        """Check if the tar file is empty."""
        with tarfile.open(self.tar_file_path, "r") as tar:
            return len(tar.getmembers()) == 0

    @property
    def index_file_path(self) -> str:
        """The path to the index file for this series in the temporary directory."""
        return os.path.join(self.get_temp_dir(), f"index.sqlite")

    # URI properties
    @property
    def datastore_series_uri(self) -> str:
        """The URI of the series in the COD datastore; i.e. `gs://<datastore_path>/studies/<study_uid>/series/<series_uid>`"""
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

    @property
    def error_log_uri(self) -> str:
        """The URI of the error log file for this series in the COD datastore."""
        return os.path.join(self.datastore_series_uri, "error.log")

    # Core public operations
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
                study_uid=self.study_uid,
                series_uid=self.series_uid,
                hashed_uids=self.hashed_uids,
            )
        else:
            raise CODObjectNotFoundError(
                f"COD:OBJECT_NOT_FOUND:{self.metadata_uri} (create_if_missing=False)"
            )
        return self._metadata

    @public_method
    def get_instances(self, strict_sorting: bool = True, dirty: bool = False):
        """Get a dictionary mapping instance UIDs to instances. These instance UIDs are hashed if `hashed_uids=True`, otherwise they are the original UIDs.
        COD will attempt to sort this dictionary so that instances appear in the proper order.

        Args:
            strict_sorting: bool - If `True`, raise an error if sorting fails (log a warning if `False`).
            dirty: bool - Must be `True` if the CODObject is "dirty" (i.e. `lock=False`).
        """
        metadata = self.get_metadata(dirty=dirty)
        metadata.sort_instances(strict=strict_sorting)
        return metadata.instances

    @public_method
    def get_instance(self, instance_uid: str, dirty: bool = False) -> Instance:
        """Get an instance by uid. `instance_uid` should be hashed if `hashed_uids=True`, otherwise it should be the original UID."""
        return self.get_instances(strict_sorting=False, dirty=dirty)[instance_uid]

    @public_method
    def get_instance_by_index(self, index: int, dirty: bool = False) -> Instance:
        """Get an instance by index.

        Args:
            index: int - The index of the instance to get.
            dirty: bool - Must be `True` if the CODObject is "dirty" (i.e. `lock=False`).
        """
        # for access by index, we require strict sorting
        return list(self.get_instances(strict_sorting=True, dirty=dirty).values())[
            index
        ]

    @public_method
    def get_instance_by_thumbnail_index(
        self, thumbnail_index: int, dirty: bool = False
    ) -> Instance:
        """Get an instance by thumbnail index.

        Args:
            thumbnail_index: int - The index of the thumbnail from you want the instance for.
            dirty: bool - Must be `True` if the CODObject is "dirty" (i.e. `lock=False`).

        Returns:
            instance: The instance corresponding to the thumbnail index.

        Raises:
            ValueError: if the cod object has no thumbnail metadata, or `thumbnail_index` is out of bounds
        """
        return get_instance_by_thumbnail_index(self, thumbnail_index)

    @public_method
    def open_instance(self, instance: Union[Instance, str, int], dirty: bool = False):
        """Open an instance (first fetches the series tar if necessary). For convenience, the instance parameter can be one of:
            - `Instance`: An actual instance object to open.
            - `str`: An instance UID to open (hashed if `hashed_uids=True`).
            - `int`: The index of an instance to open.

        Args:
            instance: Instance | str | int - The instance to open
            dirty: bool - Must be `True` if the CODObject is "dirty" (i.e. `lock=False`).

        Returns:
            A file pointer to the instance.

        Raises:
            ValueError: If the instance parameter is invalid.
            FileNotFoundError: If the instance is not found in the CODObject.
        """
        # validate instance parameter
        if isinstance(instance, Instance):
            if (
                instance.get_instance_uid(
                    hashed=self.hashed_uids, trust_hints_if_available=True
                )
                not in self.get_metadata(dirty=dirty).instances
            ):
                raise FileNotFoundError(f"Instance not found in CODObject: {instance}")
        elif isinstance(instance, str):
            instance = self.get_instance(instance, dirty=dirty)
        elif isinstance(instance, int):
            instance = self.get_instance_by_index(instance, dirty=dirty)
        else:
            raise ValueError(
                f"Invalid instance parameter: {instance} (must be Instance, str, or int)"
            )
        # pull the tar file if necessary
        if not self._tar_synced:
            self.pull_tar(dirty=dirty)
        return instance.open()

    @public_method
    def append(
        self,
        instances: list[Instance],
        treat_metadata_diffs_as_same: bool = False,
        max_instance_size: float = 10,
        max_series_size: float = 100,
        delete_local_origin: bool = False,
        compress: bool = True,
        dirty: bool = False,
    ):
        """Append a list of instances to the COD object.

        Args:
            instances: list[Instance] - The instances to append.
            treat_metadata_diffs_as_same: bool - If `True`, when a diff hash dupe is found, compute & compare the hashes of JUST the pixel data. If they match, treat the dupe as same.
            max_instance_size: float - The maximum size of an instance to append, in gb.
            max_series_size: float - The maximum size of the series to append, in gb.
            delete_local_origin: bool - If `True`, delete the local origin of the instances after appending.
            compress: bool - If `True`, transcodes instances to JPEG2000Lossless during append to save space.
            dirty: bool - Must be `True` if the CODObject is "dirty" (i.e. `lock=False`).
        """
        return append(
            cod_object=self,
            instances=instances,
            delete_local_origin=delete_local_origin,
            treat_metadata_diffs_as_same=treat_metadata_diffs_as_same,
            max_instance_size=max_instance_size,
            max_series_size=max_series_size,
            compress=compress,
        )

    @public_method
    def remove(self, instances: list[Instance], dirty: bool = False):
        """
        Remove instances from a cod object. Because tar files do not natively support removal,
        this method just determines a list of instances to keep (if any) and calls truncate.
        Returns the AppendResult of the truncate operation (i.e. what's left in the cod object)
        """
        return remove(cod_object=self, instances=instances, dirty=dirty)

    @public_method
    def truncate(
        self,
        instances: list[Instance],
        treat_metadata_diffs_as_same: bool = False,
        max_instance_size: float = 10,
        max_series_size: float = 100,
        delete_local_origin: bool = False,
        dirty: bool = False,
    ):
        """Truncate the COD object to the given instances."""
        return truncate(
            cod_object=self,
            instances=instances,
            treat_metadata_diffs_as_same=treat_metadata_diffs_as_same,
            max_instance_size=max_instance_size,
            max_series_size=max_series_size,
            delete_local_origin=delete_local_origin,
            dirty=dirty,
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
            logger.warning(f"Nothing to sync: {self}")
            return
        # design choice: it's worth the API call to verify lock prior to sync
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
            if self.tar_is_empty:
                logger.warning(f"Skipping tar sync - tar is empty: {self}")
                return
            assert os.path.exists(
                self.index_file_path
            ), "Tar sync attempted but CODObject has no index"
            tar_blob = storage.Blob.from_string(self.tar_uri, client=self.client)
            tar_blob.storage_class = tar_storage_class
            index_blob = storage.Blob.from_string(self.index_uri, client=self.client)
            upload_and_count_file(index_blob, self.index_file_path)
            upload_and_count_file(tar_blob, self.tar_file_path)
            self._tar_synced = True
        # handle thumbnail sync if necessary
        self._sync_thumbnail()
        # now that the tar has been synced,
        # single overall sync message
        logger.info(f"GRADIENT_STATE_LOGS:SYNCED_SUCCESSFULLY:{self}")

    def _sync_thumbnail(self):
        """Sync the thumbnail to the datastore if it exists"""
        if self._thumbnail_synced:
            logger.info(f"Skipping thumbnail sync - thumbnail already synced: {self}")
            return
        thumbnail_metadata = self.get_metadata_field("thumbnail")
        if thumbnail_metadata is None:
            logger.info(f"Skipping thumbnail sync - thumbnail does not exist: {self}")
            return
        # get thumbnail path
        thumbnail_file_name = os.path.basename(thumbnail_metadata["uri"])
        thumbnail_local_path = os.path.join(self.get_temp_dir(), thumbnail_file_name)
        if not os.path.exists(thumbnail_local_path):
            logger.info(f"Skipping thumbnail sync - thumbnail does not exist: {self}")
            return
        # upload thumbnail to datastore
        thumbnail_blob = storage.Blob.from_string(
            thumbnail_metadata["uri"], client=self.client
        )
        thumbnail_blob.upload_from_filename(thumbnail_local_path)
        # we just synced the thumbnail, so it is guaranteed to be in the same state as the datastore
        self._thumbnail_synced = True

    @public_method
    def add_metadata_field(
        self,
        field_name: str,
        field_value: dict,
        overwrite_existing: bool = True,
        dirty: bool = False,
    ):
        """Add a custom field to the metadata"""
        self.get_metadata(dirty=dirty)._add_metadata_field(
            field_name, field_value, overwrite_existing
        )
        # modifying metadata means it is not synced to the datastore
        self._metadata_synced = False

    @public_method
    def get_metadata_field(
        self, field_name: str, dirty: bool = False
    ) -> Optional[dict]:
        """Get a custom field from the metadata. Returns `None` if the field does not exist."""
        return self.get_metadata(dirty=dirty).metadata_fields.get(field_name, None)

    @public_method
    def remove_metadata_field(self, field_name: str, dirty: bool = False):
        """Remove a custom field from the metadata"""
        field_was_present = self.get_metadata(dirty=dirty)._remove_metadata_field(
            field_name
        )
        # if the field was present, and we removed it, the metadata is now desynced
        self._metadata_synced = not field_was_present

    @public_method
    def get_thumbnail(
        self,
        generate_if_missing: bool = True,
        instance_uid: Optional[str] = None,
        dirty: bool = False,
        thumbnail_size: int = DEFAULT_SIZE,
    ) -> np.ndarray:
        """Get the thumbnail for a COD object, in the form of a numpy array.

        Args:
            generate_if_missing: Whether to generate a thumbnail if it does not exist, or is stale.
            instance_uid: If provided, only return the slice of the thumbnail corresponding to the given instance UID.
            dirty: Whether the operation is dirty.
            thumbnail_size: The size of the thumbnail to generate (default: 128px).

        Returns:
            The thumbnail as a numpy array.

        Raises:
            ValueError: If the thumbnail does not exist and `generate_if_missing=False`, or if opening the thumbnail fails for any reason.
        """
        thumbnail_metadata = self.get_metadata_field("thumbnail", dirty=dirty)
        # Cases where we need to generate a new thumbnail:
        # 1. The thumbnail metadata does not exist (i.e. the thumbnail has never been generated)
        # 2. The thumbnail metadata exists but the number of instances it contains does not match the cod object (i.e. the thumbnail is stale)
        # 3. The thumbnail metadata exists but the thumbnail does not exist in GCS (i.e. the thumbnail is missing)
        if (
            thumbnail_metadata is None
            or len(thumbnail_metadata["instances"])
            != len(self.get_instances(strict_sorting=False, dirty=dirty))
            or not storage.Blob.from_string(
                thumbnail_metadata["uri"], client=self.client
            ).exists()
        ):
            if not generate_if_missing:
                raise ValueError(
                    f"Thumbnail either stale or not found for {self} (and generate_if_missing=False)"
                )
            generate_thumbnail(
                cod_obj=self, overwrite_existing=True, thumbnail_size=thumbnail_size
            )
            thumbnail_metadata = self.get_metadata_field("thumbnail", dirty=dirty)
        # thumbnail metadata guaranteed to be populated at this point
        thumbnail_file_name = os.path.basename(thumbnail_metadata["uri"])
        thumbnail_local_path = os.path.join(self.get_temp_dir(), thumbnail_file_name)
        # Fetch case: we have thumbnail metadata but the thumbnail does not exist on disk, so we just have to fetch it
        if not os.path.exists(thumbnail_local_path):
            fetch_thumbnail(cod_obj=self)
        # thumbnail guaranteed to be on disk at this point -> read and return it (or slice it if instance UID is provided)
        thumbnail_array = read_thumbnail_into_array(thumbnail_local_path)
        # return the raw array if no instance UIDs are provided
        if instance_uid is None:
            return thumbnail_array
        # otherwise, return the slice(s) of the thumbnail corresponding to the given instance UIDs
        return get_instance_thumbnail_slice(
            cod_obj=self,
            thumbnail_array=thumbnail_array,
            instance_uid=instance_uid,
        )

    @public_method
    def upload_error_log(self, message: str):
        """To be used by caller in except block to upload an error.log to the datastore explaining what's wrong with this cod object"""
        error_blob = storage.Blob.from_string(self.error_log_uri, client=self.client)
        if error_blob.exists():
            # because an error.log existing should cause cod objects to fail initialization,
            # it should be impossible for error.log to exist when this method is called
            msg = f"GRADIENT_STATE_LOGS:ERROR_LOG_ALREADY_EXISTS:{self}"
            logger.critical(msg)
            raise ErrorLogExistsError(msg)
        logger.warning(f"GRADIENT_STATE_LOGS:UPLOADING_ERROR_LOG:{self}:{message}")
        error_blob.upload_from_string(message)

    @public_method
    def integrity_check(self):
        """
        Check the integrity of the CODObject by verifying that the tar and metadata are consistent.
        """
        # fetch the tar and index
        self._force_fetch_tar(fetch_index=True)
        # Attempt to open the tar using the index file. If they do not match, this will raise an error and we will know there's a desync
        with rmc_open(self.tar_file_path, indexFile=self.index_file_path) as archive:
            # generate a dict of instance_uid -> crc32c for each instance in the tar
            tar_instances = {}
            for instance in archive.listDir("instances"):
                file_info = archive.getFileInfo(f"instances/{instance}")
                with archive.open(file_info) as f:
                    tar_instances[os.path.splitext(instance)[0]] = generate_ptr_crc32c(
                        f
                    )
        # Verify that each instance in the CODObject's metadata matches the tar (including crc32c)
        for instance_uid, instance in self._metadata.instances.items():
            if instance_uid not in tar_instances:
                metrics.DEPS_MISSING_FROM_TAR.inc()
                raise TarMissingInstanceError(
                    f"Instance UID found in metadata but not in tar: {instance_uid} (options: {tar_instances.keys()})"
                )
            if tar_instances[instance_uid] != instance.crc32c():
                metrics.TAR_METADATA_CRC32C_MISMATCH.inc()
                raise HashMismatchError(
                    f"Hash mismatch between tar and metadata: {instance}"
                )
        # Sanity check: tar and metadata must have the same number of instances
        if len(tar_instances) != len(self._metadata.instances):
            raise TarValidationError(
                f"Different number of instances found in tar vs. metadata: {len(tar_instances)} != {len(self._metadata.instances)}"
            )

    @public_method
    def delete_dependencies(
        self, dryrun=False, dirty=False, validate_blob_hash=False
    ) -> list[str]:
        """Run an integrity check, loop over all instances, delete their dependencies. Lock is REQUIRED (`dirty=False` always).

        Raises an error if validation fails (to be try/caught by the caller);
        the idea being that such a failure will leave a hanging lock, effectively bricking the CODObject until a developer
        goes in manually and figures out what went wrong.

        If `validate_blob_hash=False`, blobs to delete will NOT have their crc32c checked prior to deletion (save $$$).

        Returns:
            The list of URIs that got deleted
        """
        self.integrity_check()
        # If we get here all checks have passed and it is safe to delete the dependencies
        deleted_dependencies = []
        for instance in self._metadata.instances.values():
            instance.transport_params = dict(client=self.client)
            deleted_dependencies.extend(
                instance.delete_dependencies(
                    dryrun=dryrun, validate_blob_hash=validate_blob_hash
                )
            )
        # log the dependencies that were deleted
        if dryrun:
            logger.info(
                f"GRADIENT_STATE_LOGS:DRYRUN:WOULD_DELETE:{deleted_dependencies}"
            )
        else:
            logger.info(f"GRADIENT_STATE_LOGS:DELETED:{deleted_dependencies}")
        return deleted_dependencies

    @public_method
    def extract_locally(self, dirty: bool = False):
        """
        Extract the tar and index to the local temp dir, and set the dicom_uri of each instance to the local path.
        """
        self.pull_tar(dirty=dirty)
        for instance_uid, instance in self.get_instances(dirty=dirty).items():
            instance._extract_from_local_tar()

    @public_method
    def pull_tar(self, dirty: bool = False):
        """Pull tar and index from GCS to local temp dir,
        modify local origin path of instances to point to local tar.
        Ensure multiple instance.open within the series won't result in multiple GCS GET operations.
        """
        self._force_fetch_tar(fetch_index=True)
        for instance_uid, instance in self.get_metadata(
            create_if_missing=False, dirty=dirty
        ).instances.items():
            instance.dicom_uri = f"{self.tar_file_path}://instances/{instance_uid}.dcm"

    # Internal operations
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

    def _wipe_local(self):
        """
        Delete the local tar and index files, and set metadata to an empty SeriesMetadata object.
        Update tar and metadata sync flags accordingly.
        """
        if os.path.exists(self.tar_file_path):
            os.remove(self.tar_file_path)
            # if the tar existed, we changed it, so we know for sure it is not synced
            self._tar_synced = False
        if os.path.exists(self.index_file_path):
            os.remove(self.index_file_path)
        # if we had any instances in the metadata, the metadata is no longer synced
        if len(self._metadata.instances) > 0:
            self._metadata_synced = False
        self._metadata = SeriesMetadata(
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            hashed_uids=self.hashed_uids,
        )
        logger.info(f"Wiped local tar, index, and metadata: {self}")

    def _set_dicom_uris_to_datastore(self) -> dict[str, str]:
        """Set the dicom_uri of each instance to the datastore URI.

        Returns:
            A dict mapping the instance UID to the local path of the instance, so we can set it back after the sync
        """
        instance_uid_to_local_path = {}
        for uid, instance in self._metadata.instances.items():
            # skip remote .tar instances as they are already in the datastore
            if is_remote(instance.dicom_uri) and instance.is_nested_in_tar:
                continue
            uri = f"{self.tar_uri}://instances/{uid}.dcm"
            instance_uid_to_local_path[uid] = instance.dicom_uri
            instance.dicom_uri = uri
        return instance_uid_to_local_path

    def _gzip_and_upload_metadata(self):
        """
        Given a SeriesMetadata object and a blob to upload it to, convert the object to JSON, gzip it,
        and upload it to the blob
        """
        instance_uid_to_local_path = self._set_dicom_uris_to_datastore()
        metadata_blob = storage.Blob.from_string(self.metadata_uri, client=self.client)
        metadata_blob.content_encoding = "gzip"
        compressed_metadata = self._metadata.to_gzipped_json()
        metadata_blob.upload_from_string(
            compressed_metadata, content_type="application/json", retry=DEFAULT_RETRY
        )
        metrics.STORAGE_CLASS_COUNTERS["CREATE"][metadata_blob.storage_class].inc()
        # set the dicom_uri of each instance back to the local path
        for uid, local_path in instance_uid_to_local_path.items():
            self._metadata.instances[uid].dicom_uri = local_path

    def assert_instance_belongs_to_cod_object(
        self, instance: Instance, trust_hints_if_available: bool = True
    ):
        """Compare relevant instance study/series UIDS (hashed if uid_hash_func provided, standard if not) to COD object study/series UIDs.
        By default, we trust hints here, but if trust_hints_if_available is False, we will not trust hints and will use the true UIDs.

        Returns:
            True if the instance belongs to the COD object

        Raises an InstanceValidationError if any of the following are true:
            - CODObject DOES have hashed UIDs but instance does NOT have a uid_hash_func
            - CODObject does NOT have hashed UIDs but instance DOES have a uid_hash_func
            - instance study/series UIDs don't match COD object study/series UIDs
        """
        # Retrieve relevant Study/Series UIDs based on whether the CODObject has hashed UIDs
        if self.hashed_uids:
            if not instance.uid_hash_func:
                raise InstanceValidationError(
                    f"CODObject {self} has hashed UIDs but instance is missing uid_hash_func: {instance}"
                )
            relevant_study_uid = instance.hashed_study_uid(
                trust_hints_if_available=trust_hints_if_available
            )
            relevant_series_uid = instance.hashed_series_uid(
                trust_hints_if_available=trust_hints_if_available
            )
        else:
            if instance.uid_hash_func:
                raise InstanceValidationError(
                    f"CODObject {self} does not have hashed UIDs but instance has uid_hash_func: {instance}"
                )
            relevant_study_uid = instance.study_uid(
                trust_hints_if_available=trust_hints_if_available
            )
            relevant_series_uid = instance.series_uid(
                trust_hints_if_available=trust_hints_if_available
            )
        # Compare the retrieved UIDs with the CODObject's UIDs
        if (
            relevant_study_uid != self.study_uid
            or relevant_series_uid != self.series_uid
        ):
            raise InstanceValidationError(
                f"Instance {instance} does not belong to COD object {self}"
            )
        return True

    # Serialization methods
    def serialize(self) -> dict:
        """Serialize the object into a dict"""
        state = self.__dict__.copy()
        # remove client (cannot pickle)
        del state["client"]
        # remove locker (will be recreated on deserialization)
        del state["_locker"]
        # use metadata's to_dict() method to serialize
        state["_metadata"] = self._metadata.to_dict()
        return state

    @classmethod
    def deserialize(
        cls,
        serialized_obj: dict,
        client: storage.Client,
        uid_hash_func: Optional[Callable] = None,
    ) -> "CODObject":
        metadata_dict = serialized_obj.pop("_metadata")
        # if lock_generation is not None, the serialized object had a lock
        lock = True if serialized_obj["lock_generation"] is not None else False
        cod_object = CODObject(**serialized_obj, client=client, lock=lock)
        cod_object._metadata = SeriesMetadata.from_dict(
            metadata_dict, uid_hash_func=uid_hash_func
        )
        return cod_object

    def cleanup_temp_dir(self):
        """Clean temp dir (if not done already)"""
        # clean up temp dir
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            self.temp_dir = None

    def __del__(self):
        self.cleanup_temp_dir()

    # Magic methods
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

    @classmethod
    def from_uri(
        cls,
        uri: str,
        client: storage.Client,
        lock: bool,
        hashed_uids: bool,
        create_if_missing: bool,
    ):
        """Create a CODObject from a URI"""
        if not is_remote(uri) or "/studies/" not in uri or "/series/" not in uri:
            raise ValueError(f"Invalid COD URI: {uri}")
        datastore_uri, overflow = uri.split("/studies/", 1)
        study_uid, series_and_overflow = overflow.split("/series/", 1)
        # remove all possible overflow from the series uid: subfile names, tar extension
        series_uid = series_and_overflow.split("/", 1)[0].rstrip(".tar")
        return cls(
            datastore_path=datastore_uri,
            client=client,
            study_uid=study_uid,
            series_uid=series_uid,
            lock=lock,
            hashed_uids=hashed_uids,
            create_if_missing=create_if_missing,
        )
