from google.cloud import storage


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
        create_if_missing: bool - If `False`, raise an error if series does not yet exist in the datastore.
        lock: bool - If `True`, acquire a lock on initialization. If `False`, no changes made on this object will be synced to the datastore.
    """

    def __init__(
        self,
        datastore_path: str,
        client: storage.Client,
        study_uid: str,
        series_uid: str,
        create_if_missing: bool = True,
        lock: bool = True,
    ):
        self.datastore_path = datastore_path
        self.client = client
        self.study_uid = study_uid
        self.series_uid = series_uid
        self.create_if_missing = create_if_missing
        self._lock = lock
        self._validate_uids()

    def _validate_uids(self):
        """Validate the UIDs are valid DICOM UIDs (TODO make this more robust, for now just check length)"""
        assert len(self.study_uid) >= 10, "Study UID must be 10 characters long"
        assert len(self.series_uid) >= 10, "Series UID must be 10 characters long"

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
