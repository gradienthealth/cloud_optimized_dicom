import logging
import os
import tarfile
import tempfile
from dataclasses import dataclass, field
from io import BufferedReader
from typing import Callable

import pydicom
from smart_open import open as smart_open

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.custom_offset_tables import get_multiframe_offset_tables
from cloud_optimized_dicom.hints import Hints
from cloud_optimized_dicom.utils import (
    DICOM_PREAMBLE,
    _delete_gcs_dep,
    find_pattern,
    generate_ptr_crc32c,
    is_remote,
)

logger = logging.getLogger(__name__)

TAR_IDENTIFIER = ".tar://"
ZIP_IDENTIFIER = ".zip://"


@dataclass
class Instance:
    """Object representing a single DICOM instance.

    Required args:
        `dicom_uri: str` - The URI of the DICOM file.
        `hints: Hints` - values already known or suspected about the instance (size, hash, etc. - see hints.py).
    Optional args:
        `dependencies: list[str]` - A list of URIs of files that were required to generate `dicom_uri`.
        `transport_params: dict` - A smart_open transport_params dict.
    """

    # public fields that the user might provide
    dicom_uri: str
    dependencies: list[str] = field(default_factory=list)
    hints: Hints = field(default_factory=Hints)
    transport_params: dict = field(default_factory=dict)

    # private internal fields
    _metadata: dict = None
    _custom_offset_tables: dict = None
    # uids/cached values
    _instance_uid: str = None
    _series_uid: str = None
    _study_uid: str = None
    _size: int = None
    _crc32c: str = None

    def __post_init__(self):
        if self.is_remote():
            self._local_path = None
        else:
            self._local_path = self.dicom_uri

    @property
    def is_remote(self) -> bool:
        """
        Return whether self.dicom_uri begins with any of the `REMOTE_IDENTIFIERS`.
        """
        return is_remote(self.dicom_uri)

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
        """Open the instance, read the internal fields, and (TODO) validate they match hints if provided.

        Returns:
            bool - True if the instance is valid
        Raises:
            AssertionError if the instance is invalid.
        """
        # populate all true values
        with self.open() as f:
            with pydicom.dcmread(f) as ds:
                self._instance_uid = getattr(ds, "SOPInstanceUID")
                self._series_uid = getattr(ds, "SeriesInstanceUID")
                self._study_uid = getattr(ds, "StudyInstanceUID")
            # seek back to beginning of file to calculate crc32c
            f.seek(0)
            self._crc32c = generate_ptr_crc32c(f)
        self._size = os.path.getsize(self.local_path)
        # validate hints
        self.hints.validate(
            true_size=self._size,
            true_crc32c=self._crc32c,
            true_instance_uid=self._instance_uid,
            true_series_uid=self._series_uid,
            true_study_uid=self._study_uid,
        )
        return True

    @property
    def metadata(self):
        """
        Getter for self._metadata. Populates by calling self.extract_metadata() if necessary.
        """
        if self._metadata is None:
            self.extract_metadata()
        return self._metadata

    @property
    def local_path(self):
        """
        Getter for self._local_path. Populates by calling self.fetch() if necessary.
        """
        if self._local_path is None:
            self.fetch()
        return self._local_path

    def size(self, trust_hints_if_available: bool = False):
        """
        Getter for self._size. Populates by calling self.validate() if necessary.
        """
        if trust_hints_if_available and self.hints.size is not None:
            return self.hints.size
        if self._size is None:
            self.validate()
        return self._size

    def crc32c(self, trust_hints_if_available: bool = False):
        """
        Getter for self._crc32c. Populates by calling self.validate() if necessary.
        """
        if trust_hints_if_available and self.hints.crc32c is not None:
            return self.hints.crc32c
        if self._crc32c is None:
            self.validate()
        return self._crc32c

    def instance_uid(self, trust_hints_if_available: bool = False):
        """
        Getter for self._instance_uid. Populates by calling self.validate() if necessary.
        """
        if trust_hints_if_available and self.hints.instance_uid is not None:
            return self.hints.instance_uid
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

    def open(self) -> BufferedReader:
        """
        Open an instance and return a file pointer to its bytes, which can be given to pydicom.dcmread()
        """
        self.fetch()
        return open(self._local_path, "rb")

    def append_to_series_tar(
        self,
        tar: tarfile.TarFile,
        uid_generator: Callable[[str], str] = lambda x: x,
        delete_local_on_completion: bool = False,
    ):
        """
        Append the instance to a series tar file. Intended use case:
        ```
        with tarfile.open("series.tar", "w") as tar:
            instance.append_to_series_tar(tar)
        ```
        Args:
            tar: tarfile.TarFile to append to
            uid_generator: function to call on instance UIDs to generate tar path (e.g. to anonymize)
            delete_local_on_completion: if True and dicom_uri is local, delete the local instance file on completion
        """
        uid_for_uri = uid_generator(self.instance_uid())
        # do actual appending
        f = tar.fileobj
        begin_offset = f.tell()
        tar.add(self.local_path, arcname=f"/instances/{uid_for_uri}.dcm")
        end_offset = f.tell()
        f.seek(begin_offset)
        # TODO: if index is always 1536, we can skip the find pattern
        index = find_pattern(f, DICOM_PREAMBLE)
        if index != 1536:
            logger.warning(f"Unexpected DICOM tar header size {index}")
        if index == -1:
            raise ValueError("Not a Valid DICOM")
        start_offset = begin_offset + index
        stop_offset = start_offset + self.size()
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
        self._local_path = f"{tar.name}://instances/{uid_for_uri}.dcm"

    def extract_metadata(self, output_uri: str):
        """
        Extract metadata from the instance, populating self._metadata and self._custom_offset_tables
        """
        with self.open() as f:
            with pydicom.dcmread(f, defer_size=1024) as ds:
                # set custom offset tables
                self._custom_offset_tables = get_multiframe_offset_tables(ds)

                # define custom bulk data handler to include the head 512 bytes of overlarge elements
                def bulk_data_handler(el: pydicom.DataElement) -> str:
                    """Given a bulk data element, return a dict containing this instance's output_uri
                    and the head 512 bytes of the element"""
                    # TODO would be nice to find a way to include the tail 512 bytes as well
                    with self.open() as dcm_file:
                        dcm_file.seek(el.file_tell)
                        element_head = dcm_file.read(512)
                    return {
                        "uri": output_uri,
                        "head": element_head.decode("utf-8", errors="replace"),
                    }

                # extract json dict using custom bulk data handler
                try:
                    ds_dict = ds.to_json_dict(
                        bulk_data_element_handler=bulk_data_handler
                    )
                except Exception as e:
                    logger.warning(
                        f"Instance {self.as_log} metadata extraction error: {e}\nRetrying with suppress_invalid_tags=True"
                    )
                    # TODO: check if supress will still provide bad tags in utf-8 encoded binary (this way we still can preview something)
                    # Will likely be related to pydicom 3.0; sometimes we get birthday in MMDDYYYY rather than YYYYMMDD as per spec
                    # So suppression should ideally still provide back the binary data just in utf8 encoded data.
                    ds_dict = ds.to_json_dict(
                        bulk_data_element_handler=bulk_data_handler,
                        suppress_invalid_tags=True,
                    )
                # make sure to include file_meta
                ds_dict.update(
                    ds.file_meta.to_json_dict(
                        bulk_data_element_handler=bulk_data_handler
                    )
                )
                # populate self._metadata
                self._metadata = ds_dict

    @property
    def as_log(self):
        """
        Return a string representation of the instance for logging purposes.
        """
        return f"(uri={self.dicom_uri}, instance_uid={self._instance_uid}, dependencies={self.dependencies})"

    def delete_dependencies(
        self, dryrun: bool = False, validate_blob_hash: bool = True
    ) -> list[str]:
        """Delete all dependencies. If `validate_blob_hash==True` and an instance has only one dependency (must be the dcm P10),
        validate the crc32c of the blob before deleting. This costs us a GET per instance, which could be expensive,
        so this check can be disabled by setting `validate_blob_hash=False`.
        Returns a list of dependencies that were deleted (or would have been deleted if in dryrun mode).
        """
        if dryrun:
            logger.info(f"COD_STATE_LOGS:DRYRUN:WOULD_DELETE:{self.dependencies}")
            return self.dependencies
        deleted_dependencies = []
        for uri in self.dependencies:
            # we do not handle nested dependencies (e.g. a dcm within a zip)
            if TAR_IDENTIFIER in uri or ZIP_IDENTIFIER in uri:
                raise NotImplementedError("Nested dependency deletion is not supported")
            if uri.startswith("gs://"):
                assert (
                    "client" in self.transport_params
                ), "client must be provided for GCS dependencies"
                client = self.transport_params["client"]
                # If only 1 dep, assume it's a dicom p10 -> check hash, ensure it matches
                # TODO: turn deps list into {uri: crc32c} dict. Then this assumption can be avoided
                # and we can check the hash of any dependency
                if validate_blob_hash and len(self.dependencies) == 1:
                    _delete_gcs_dep(uri=uri, client=client, expected_crc32c=self.crc32c)
                else:
                    _delete_gcs_dep(uri=uri, client=client)
                deleted_dependencies.append(uri)
            elif os.path.exists(uri):
                os.remove(uri)
                deleted_dependencies.append(uri)
            else:
                logger.warning(f"DEPENDENCY_DELETION:SKIP:FILE_DOES_NOT_EXIST:{uri}")
                continue
        # We don't want to spend GET requests to calculate exact deleted size. Instead we estimate with instance size
        metrics.BYTES_DELETED_COUNTER.inc(self.size)
        return deleted_dependencies

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
