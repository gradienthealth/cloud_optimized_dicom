import os
import tempfile

from smart_open import open as smart_open

REMOTE_IDENTIFIERS = ["http", "s3://", "gs://"]


class Instance:
    """
    Object representing a single DICOM instance.
    """

    def __init__(self, dicom_uri: str, transport_params: dict = {}):
        self.dicom_uri = dicom_uri
        self.transport_params = transport_params
        if self.is_remote():
            self._local_path = None
        else:
            self._local_path = dicom_uri

    def is_remote(self) -> bool:
        """
        Return whether self.dicom_uri begins with any of the `REMOTE_IDENTIFIERS`.
        """
        return any(
            self.dicom_uri.startswith(identifier) for identifier in REMOTE_IDENTIFIERS
        )

    def fetch(self):
        """
        Fetch the DICOM instance from the remote source and save it to a temporary file.
        """
        # Early exit condition: self._local_path exists already
        if self._local_path is not None and os.path.exists(self._local_path):
            return

        # Sanity check: only remote instances should be fetchable
        assert self.is_remote(), "Cannot fetch local DICOM instance"

        self._temp_file = tempfile.NamedTemporaryFile(suffix=".dcm", delete=False)
        self._local_path = self._temp_file.name

        # read remote file into local temp file
        with open(self._local_path, "wb") as local_file:
            with smart_open(
                self.dicom_uri, transport_params=self.transport_params
            ) as source:
                local_file.write(source.read())

    def open(self):
        """
        Open an instance and return a file pointer to its bytes, which can be given to pydicom.dcmread()
        """
        self.fetch()
        return open(self._local_path, "rb")

    def cleanup(self):
        """
        Delete the temporary file, if it exists.
        """
        if hasattr(self, "_temp_file"):
            self._temp_file.close()

    def __del__(self):
        """
        Custom destructor that calls self.cleanup()
        """
        self.cleanup()
