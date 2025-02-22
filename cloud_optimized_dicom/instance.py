import logging
import os
import tarfile
import tempfile
from dataclasses import dataclass, field

import pydicom
from smart_open import open as smart_open

from cloud_optimized_dicom.utils import DICOM_PREAMBLE, find_pattern

logger = logging.getLogger(__name__)

REMOTE_IDENTIFIERS = ["http", "s3://", "gs://"]


@dataclass
class Instance:
    """
    Object representing a single DICOM instance.
    """

    dicom_uri: str
    transport_params: dict = field(default_factory=dict)

    # private/cached internal fields
    _instance_uid: str = None
    _series_uid: str = None
    _study_uid: str = None

    def __post_init__(self):
        if self.is_remote():
            self._local_path = None
        else:
            self._local_path = self.dicom_uri

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
                uri=self.dicom_uri, mode="rb", transport_params=self.transport_params
            ) as source:
                local_file.write(source.read())
        self.validate()

    def validate(self):
        """
        Open the instance, read the internal fields, and (TODO) validate they match hints if provided
        """
        with self.open() as f:
            with pydicom.dcmread(f) as ds:
                self._instance_uid = ds.SOPInstanceUID
                self._series_uid = ds.SeriesInstanceUID
                self._study_uid = ds.StudyInstanceUID

    @property
    def instance_uid(self):
        """
        Getter for self._instance_uid. Populates by calling self.validate() if necessary.
        """
        # fetch if necessary
        if self._instance_uid is None:
            self.validate()
        return self._instance_uid

    @property
    def series_uid(self):
        """
        Getter for self._series_uid. Populates by calling self.validate() if necessary.
        """
        if self._series_uid is None:
            self.validate()
        return self._series_uid

    @property
    def study_uid(self):
        """
        Getter for self._study_uid. Populates by calling self.validate() if necessary.
        """
        if self._study_uid is None:
            self.validate()
        return self._study_uid

    def open(self):
        """
        Open an instance and return a file pointer to its bytes, which can be given to pydicom.dcmread()
        """
        self.fetch()
        return open(self._local_path, "rb")

    def append_to_series_tar(
        self, tar: tarfile.TarFile, delete_local_on_completion: bool = False
    ):
        """
        Append the instance to a series tar file. Intended use case:
        ```
        with tarfile.open("series.tar", "w") as tar:
            instance.append_to_series_tar(tar)
        ```
        Args:
            tar: tarfile.TarFile to append to
            delete_local_on_completion: if True and dicom_uri is local, delete the local instance file on completion
        """
        # do actual appending
        f = tar.fileobj
        begin_offset = f.tell()
        tar.add(self._local_path, arcname=f"/instances/{self.deid_instance_uid}.dcm")
        end_offset = f.tell()
        f.seek(begin_offset)
        # TODO: if index is always 1536, we can skip the find pattern
        index = find_pattern(f, DICOM_PREAMBLE)
        if index != 1536:
            logger.warning(f"Unexpected DICOM tar header size {index}")
        if index == -1:
            raise ValueError("Not a Valid DICOM")
        start_offset = begin_offset + index
        stop_offset = start_offset + self.size
        # Seek back to the end offset to read the file from the tar
        f.seek(end_offset)

        # set byte offsets
        self.byte_offsets = (start_offset, stop_offset)
        # cleanup temp file if exists (no longer needed)
        self.cleanup()
        # delete local origin if flag is set
        if delete_local_on_completion and not self.is_remote:
            os.remove(self._local_path)
        # point local_origin_file within the local tar
        self._local_path = f"{tar.name}://instances/{self.deid_instance_uid}.dcm"

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
