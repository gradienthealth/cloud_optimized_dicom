import os
import tarfile
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

import pydicom3
import pydicom3.uid
from ratarmountcore import open as rmc_open
from smart_open import open as smart_open

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.custom_offset_tables import get_multiframe_offset_tables
from cloud_optimized_dicom.hints import Hints
from cloud_optimized_dicom.instance_metadata import DicomMetadata
from cloud_optimized_dicom.utils import (
    DICOM_PREAMBLE,
    _delete_gcs_dep,
    file_is_dicom,
    find_pattern,
    generate_ptr_crc32c,
    is_remote,
    parse_uids_from_metadata,
)
from cloud_optimized_dicom.virtual_file import VirtualFile

TAR_IDENTIFIER = ".tar://"
ZIP_IDENTIFIER = ".zip://"


@dataclass
class Instance:
    """Object representing a single DICOM instance.

    Required args:
        `dicom_uri: str` - The URI of the DICOM file.
    Optional args:
        `dependencies: list[str]` - A list of URIs of files that were required to generate `dicom_uri`.
        `hints: Hints` - values already known or suspected about the instance (size, hash, etc. - see hints.py).
        `transport_params: dict` - A smart_open transport_params dict (if the instance is remote, and credentials are needed to retrieve it).
        `uid_hash_func: Callable[[str], str]` - A function that takes a UID and returns a new UID
    """

    # public fields that the user might provide
    dicom_uri: str
    dependencies: list[str] = field(default_factory=list)
    hints: Hints = field(default_factory=Hints)
    transport_params: dict = field(default_factory=dict)
    uid_hash_func: Callable[[str], str] = None

    # private internal fields
    _temp_file_path: str = None
    _dicom_metadata: Optional[DicomMetadata] = None
    _custom_offset_tables: dict = None
    _diff_hash_dupe_paths: list[str] = field(default_factory=list)
    _modified_datetime: str = datetime.now().isoformat()
    _original_path: str = None
    _byte_offsets: tuple[int, int] = None
    # uids/cached values
    _instance_uid: str = None
    _series_uid: str = None
    _study_uid: str = None
    _size: int = None
    _crc32c: str = None
    _has_pixeldata: bool = None

    def __post_init__(self):
        # if original_path is not set, set it to dicom_uri
        if not self._original_path:
            self._original_path = self.dicom_uri

    @property
    def is_nested_in_tar(self) -> bool:
        """
        Return whether self.dicom_uri is nested in a tar file.
        """
        return TAR_IDENTIFIER in self.dicom_uri

    def fetch(self):
        """
        Fetch the DICOM instance from the remote source and save it to a temporary file.
        """
        # Early exit condition: self.dicom_uri is local
        if not is_remote(self.dicom_uri):
            return

        # raise an error if the instance is nested in a tar
        if self.is_nested_in_tar:
            raise ValueError(
                f"Direct fetching of instances in remote tars is not supported... use CODObject.open_instance() instead"
            )

        # we store the path, not the file object, so that instances can be pickled (allows them to be passed between beam.DoFns)
        with tempfile.NamedTemporaryFile(suffix=".dcm", delete=False) as temp_file:
            self._temp_file_path = temp_file.name

        # read remote file into local temp file
        with open(self._temp_file_path, "wb") as local_file:
            with smart_open(
                uri=self.dicom_uri, mode="rb", transport_params=self.transport_params
            ) as source:
                local_file.write(source.read())
        # after writing, dicom_uri is now local
        self.dicom_uri = self._temp_file_path
        self.validate()

    def _extract_from_local_tar(self):
        """
        Extract the instance from a local tar file.
        """
        assert self.is_nested_in_tar and not is_remote(
            self.dicom_uri
        ), f"extract_from_local_tar expected local tar uri but got: {self.dicom_uri}"
        # create a temp file to store the instance
        with tempfile.NamedTemporaryFile(suffix=".dcm", delete=False) as temp_file:
            self._temp_file_path = temp_file.name
        # read the instance from the tar into the temp file
        with self.open() as f_in:
            with open(self._temp_file_path, "wb") as f_out:
                f_out.write(f_in.read())
        # after writing, dicom_uri is now local
        self.dicom_uri = self._temp_file_path
        self.validate()

    def validate(self):
        """Open the instance, read the internal fields, and validate they match hints if provided.

        Returns:
            bool - True if the instance is valid
        Raises:
            HintMismatchError if the instance is invalid.
        """
        # populate all true values
        with self.open() as f:
            # defer loading all tags (every tag is > 1 byte) bc we only need UIDs and pixeldata existence
            # stop_before_pixels=True cannot be used here bc it prevents us from checking for pixeldata existence
            with pydicom3.dcmread(f, defer_size=1) as ds:
                self._instance_uid = getattr(ds, "SOPInstanceUID")
                self._series_uid = getattr(ds, "SeriesInstanceUID")
                self._study_uid = getattr(ds, "StudyInstanceUID")
                self._has_pixeldata = hasattr(ds, "PixelData")
            # seek back to beginning of file to calculate crc32c
            f.seek(0)
            self._crc32c = generate_ptr_crc32c(f)
        # compute size if not already set (if it's a tar, _open_tar will have set it)
        if not self._size:
            self._size = os.path.getsize(self.dicom_uri)
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
        Getter for self._dicom_metadata. Populates by calling self.extract_metadata() if necessary.
        """
        if self._dicom_metadata is None:
            self.extract_metadata()
        return self._dicom_metadata.get_dicom_metadata()

    @property
    def has_pixeldata(self):
        """
        Getter for self._has_pixeldata. Populates by calling self.validate() if necessary.
        """
        if self._has_pixeldata is None:
            self.validate()
        return self._has_pixeldata

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

    def hashed_instance_uid(self, trust_hints_if_available: bool = False):
        """
        Getter for `self.uid_hash_func(self._instance_uid)`. Populates by calling self.validate() if necessary.
        """
        if self.uid_hash_func is None:
            raise ValueError(
                f"hashed_instance_uid called on instance with no uid_hash_func: {self}"
            )
        return self.uid_hash_func(
            self.instance_uid(trust_hints_if_available=trust_hints_if_available)
        )

    def series_uid(self, trust_hints_if_available: bool = False):
        """
        Getter for self._series_uid. Populates by calling self.validate() if necessary.
        """
        if trust_hints_if_available and self.hints.series_uid is not None:
            return self.hints.series_uid
        if self._series_uid is None:
            self.validate()
        return self._series_uid

    def hashed_series_uid(self, trust_hints_if_available: bool = False):
        """
        Getter for `self.uid_hash_func(self._series_uid)`. Populates by calling self.validate() if necessary.
        """
        if self.uid_hash_func is None:
            raise ValueError(
                f"hashed_series_uid called on instance with no uid_hash_func: {self}"
            )
        return self.uid_hash_func(
            self.series_uid(trust_hints_if_available=trust_hints_if_available)
        )

    def study_uid(self, trust_hints_if_available: bool = False):
        """
        Getter for self._study_uid. Populates by calling self.validate() if necessary.
        """
        if trust_hints_if_available and self.hints.study_uid is not None:
            return self.hints.study_uid
        if self._study_uid is None:
            self.validate()
        return self._study_uid

    def hashed_study_uid(self, trust_hints_if_available: bool = False):
        """
        Getter for `self.uid_hash_func(self._study_uid)`. Populates by calling self.validate() if necessary.
        """
        if self.uid_hash_func is None:
            raise ValueError(
                f"hashed_study_uid called on instance with no uid_hash_func: {self}"
            )
        return self.uid_hash_func(
            self.study_uid(trust_hints_if_available=trust_hints_if_available)
        )

    def get_instance_uid(self, hashed: bool, trust_hints_if_available: bool = False):
        """instance_uid getter method that returns the instance_uid or hashed_instance_uid, dpepending on the `hashed` flag"""
        return (
            self.hashed_instance_uid(trust_hints_if_available=trust_hints_if_available)
            if hashed
            else self.instance_uid(trust_hints_if_available=trust_hints_if_available)
        )

    def open(self):
        """
        Open an instance and return a file pointer to its bytes, which can be given to pydicom.dcmread()
        """
        self.fetch()
        if self.is_nested_in_tar:
            ptr = self._open_tar()
        else:
            ptr = open(self.dicom_uri, "rb")
        assert file_is_dicom(ptr), f"File is not a valid DICOM: {self.dicom_uri}"
        return ptr

    def _open_tar(self):
        """Return a pointer to the instance (within a tar)"""
        assert self.is_nested_in_tar, f"_open_tar called on non-tar: {self.dicom_uri}"
        # if byte_offsets are not set, we need to find the file in the tar
        tar_path, internal_path = self.dicom_uri.split(TAR_IDENTIFIER)
        # if origin_uri is a tar, we need to find the file in the tar
        if not self._byte_offsets:
            options = {}
            if os.path.exists(f"{tar_path}.index.sqlite"):
                options = {"indexFilePath": f"{tar_path}.index.sqlite"}
            with rmc_open(f"{tar_path}.tar", **options) as archive:
                internal_file_info = archive.getFileInfo(internal_path)
                if not internal_file_info:
                    raise FileNotFoundError(f"File not found in tar: {internal_path}")
                # set size if necessary
                if not self._size:
                    self._size = internal_file_info.size
                # with size guaranteed, we can compute byte offsets
                start_byte = internal_file_info.userdata[0].offset
                self._byte_offsets = start_byte, start_byte + self._size - 1
                # set crc32c if necessary
                if not self._crc32c:
                    with archive.open(internal_file_info) as instance_file:
                        self._crc32c = generate_ptr_crc32c(instance_file)
        # with byte_offsets guaranteed, we can now return a file pointer
        master_file_pointer = open(f"{tar_path}.tar", "rb")
        # Add 1 to end byte to get stop position
        start, stop = self._byte_offsets[0], self._byte_offsets[1] + 1
        return VirtualFile(master_file_pointer, start, stop)

    def compress(self, syntax: pydicom3.uid.UID = pydicom3.uid.JPEG2000Lossless):
        """Compress the instance to the given syntax. Fails if used prior to fetching, or if the local instance is nested in a tar.

        Args:
            syntax: pydicom3.uid.UID to transcode to. Defaults to JPEG2000Lossless.
        """
        if self.is_nested_in_tar or is_remote(self.dicom_uri):
            raise ValueError(
                f"Attemtped to transcode an instance that is nested in a tar or is remote (unsupported): {self}"
            )
        if not self.has_pixeldata:
            logger.info(f"Skipping transcode ({self} has no pixeldata to transcode)")
            return

        # open the original instance
        with self.open() as f:
            # read the instance
            with pydicom3.dcmread(f, defer_size=1024) as ds:
                if ds.file_meta.TransferSyntaxUID.is_compressed:
                    logger.info(f"Skipping transcode ({self} is already compressed)")
                    return
                ds.compress(syntax)
                # make a new temp file to write the transcoded instance to
                with tempfile.NamedTemporaryFile(
                    suffix=".dcm", delete=False
                ) as temp_file:
                    ds.save_as(temp_file.name)
                    # if we were tracking a temp file, delete it
                    if self._temp_file_path:
                        # delete the old temp file (obsolete)
                        os.remove(self._temp_file_path)
                    # both temp file and dicom_uri point to the new temp file
                    self.dicom_uri = temp_file.name
                    self._temp_file_path = temp_file.name
                    # old size, crc32c (and hints) are now invalid
                    self.hints.crc32c = None
                    self.hints.size = None
                    self._size = None
                    self._crc32c = None
                    # call validate to recalculate these fields
                    self.validate()

    def append_to_series_tar(
        self,
        tar: tarfile.TarFile,
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
        # decide which instance UID to use in the uri (hashed if func provided, else standard)
        uid_for_uri = (
            self.hashed_instance_uid() if self.uid_hash_func else self.instance_uid()
        )
        # do actual appending
        f = tar.fileobj
        begin_offset = f.tell()
        tar.add(self.dicom_uri, arcname=f"/instances/{uid_for_uri}.dcm")
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
        self._byte_offsets = (start_offset, stop_offset)
        # cleanup temp file if exists (no longer needed)
        self.cleanup()
        # delete local origin if flag is set
        if delete_local_on_completion and not is_remote(self._original_path):
            os.remove(self._original_path)
        # point local_origin_file within the local tar
        self.dicom_uri = f"{tar.name}://instances/{uid_for_uri}.dcm"

    def extract_metadata(self, output_uri: Optional[str] = None):
        """
        Extract metadata from the instance, populating self._dicom_metadata and self._custom_offset_tables
        """
        # output_uri depends on the cod object that the instance is being appended to.
        # Specifically, the tar_uri, as well as whether the UIDs are hashed or not.
        # It is possible for extract_metadata() to be called before the instance is appended to a cod object,
        # in which case we don't necessarily know the output_uri.
        # Use dicom_uri as fallback if output_uri is not provided
        if output_uri is None:
            output_uri = self.dicom_uri
        with self.open() as f:
            with pydicom3.dcmread(f, defer_size=1024) as ds:
                # set custom offset tables
                self._custom_offset_tables = get_multiframe_offset_tables(ds)

                # define custom bulk data handler to include the head 512 bytes of overlarge elements
                def bulk_data_handler(el: pydicom3.DataElement) -> str:
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
                # suppress_invalid_tags=True so that when bad tags are encountered, they are simply omitted
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
                # populate self._dicom_metadata with uncompressed metadata
                self._dicom_metadata = DicomMetadata(ds_dict)

    def get_pixeldata_hash(self) -> str:
        """Compute the crc32c hash of just the pixeldata"""
        with tempfile.NamedTemporaryFile(suffix="_pixeldata") as temp_file:
            with self.open() as ptr:
                ds = pydicom3.dcmread(ptr, defer_size=1024)
                # write ds.pixelData to temp_file
                temp_file.write(ds.PixelData)
                temp_file.flush()
                temp_file.seek(0)
                return generate_ptr_crc32c(temp_file)

    def __str__(self):
        """
        Return a string representation of the instance.
        """
        iuid = (
            self.hashed_instance_uid()
            if self.uid_hash_func and self._instance_uid
            else self._instance_uid
        )
        suid = (
            self.hashed_series_uid()
            if self.uid_hash_func and self._series_uid
            else self._series_uid
        )
        stuid = (
            self.hashed_study_uid()
            if self.uid_hash_func and self._study_uid
            else self._study_uid
        )
        return f"Instance(uri={self.dicom_uri}, hashed_uids={self.uid_hash_func is not None}, instance_uid={iuid}, series_uid={suid}, study_uid={stuid}, dependencies={self.dependencies})"

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
                    _delete_gcs_dep(
                        uri=uri, client=client, expected_crc32c=self.crc32c()
                    )
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
        metrics.BYTES_DELETED_COUNTER.inc(self.size())
        metrics.NUM_FILES_DELETED.inc()
        return deleted_dependencies

    def append_diff_hash_dupe(self, duplicate_path: str) -> bool:
        """Append a diff hash dupe and update `modified_datetime`. Skips appending if any of the following conditions are met:
        - `duplicate_path` is not remote (would be meaningless to store local dupe in remote datastore)
        - `duplicate_path` is already in `diff_hash_dupe_paths`

        Returns:
            bool - True if the dupe was appended, False otherwise
        """
        # do not append local diff hash dupes
        if not is_remote(duplicate_path):
            return False
        # do not append again if path already exists in dupe list
        if duplicate_path in self._diff_hash_dupe_paths:
            return False
        # if we make it here, we have a new, remote, diff hash dupe to append
        self._diff_hash_dupe_paths.append(duplicate_path)
        self._modified_datetime = datetime.now().isoformat()
        return True

    def remove_hints(self):
        """Throw out the instance's hints, setting its Hints object to be empty."""
        self.hints = Hints()

    def to_cod_dict_v1(self):
        """Convert this instance to a dict in accordance with the COD Metadata v1.0 spec"""
        # first unpack byte offsets
        if self._byte_offsets is None:
            start_byte, end_byte = None, None
        else:
            start_byte, end_byte = self._byte_offsets
        # now return dict
        if self._dicom_metadata is None:
            raise ValueError(
                "Cannot serialize instance without metadata. Call extract_metadata() first."
            )
        return {
            "metadata": self._dicom_metadata.get_dicom_metadata(),
            "uri": self.dicom_uri,
            "headers": {
                "start_byte": start_byte,
                "end_byte": end_byte,
            },
            "offset_tables": self._custom_offset_tables,
            "crc32c": self.crc32c(),
            "size": self.size(),
            "original_path": self._original_path,
            "dependencies": self.dependencies,
            "diff_hash_dupe_paths": self._diff_hash_dupe_paths,
            "version": "1.0",
            "modified_datetime": self._modified_datetime,
        }

    def to_cod_dict_v2(self):
        """Convert this instance to a dict in accordance with the COD Metadata v2.0 spec"""
        # first unpack byte offsets
        if self._byte_offsets is None:
            start_byte, end_byte = None, None
        else:
            start_byte, end_byte = self._byte_offsets
        # now return dict
        if self._dicom_metadata is None:
            raise ValueError(
                "Cannot serialize instance without metadata. Call extract_metadata() first."
            )
        # Get the compressed metadata string
        compressed_metadata = self._dicom_metadata.get_compressed_metadata()
        return {
            "instance_uid": self.instance_uid(),
            "series_uid": self.series_uid(),
            "study_uid": self.study_uid(),
            "has_pixeldata": self.has_pixeldata,
            "metadata": compressed_metadata,
            "uri": self.dicom_uri,
            "headers": {
                "start_byte": start_byte,
                "end_byte": end_byte,
            },
            "offset_tables": self._custom_offset_tables,
            "crc32c": self.crc32c(),
            "size": self.size(),
            "original_path": self._original_path,
            "dependencies": self.dependencies,
            "diff_hash_dupe_paths": self._diff_hash_dupe_paths,
            "version": "2.0",
            "modified_datetime": self._modified_datetime,
        }

    @classmethod
    def from_cod_dict_v1(
        cls, instance_dict: dict, uid_hash_func: Optional[Callable] = None
    ) -> "Instance":
        """Convert a COD Metadata v1.0 dict to an Instance."""
        if (found_version := instance_dict.get("version")) != "1.0":
            logger.warning(f"Expected version 1.0, but got {found_version}")
        byte_offsets = (
            instance_dict["headers"]["start_byte"],
            instance_dict["headers"]["end_byte"],
        )
        instance_uid, series_uid, study_uid = parse_uids_from_metadata(
            instance_dict["metadata"]
        )
        has_pixeldata = "7FE00010" in instance_dict["metadata"]
        # Create DicomMetadata from the dict (uncompressed)
        dicom_metadata = DicomMetadata(instance_dict["metadata"])
        return Instance(
            dicom_uri=instance_dict["uri"],
            uid_hash_func=uid_hash_func,
            _dicom_metadata=dicom_metadata,
            _byte_offsets=byte_offsets,
            _custom_offset_tables=instance_dict["offset_tables"],
            _size=instance_dict["size"],
            _crc32c=instance_dict["crc32c"],
            dependencies=instance_dict["dependencies"],
            _original_path=instance_dict["original_path"],
            _modified_datetime=instance_dict["modified_datetime"],
            _diff_hash_dupe_paths=instance_dict["diff_hash_dupe_paths"],
            _instance_uid=instance_uid,
            _series_uid=series_uid,
            _study_uid=study_uid,
            _has_pixeldata=has_pixeldata,
        )

    @classmethod
    def from_cod_dict_v2(
        cls, instance_dict: dict, uid_hash_func: Optional[Callable] = None
    ) -> "Instance":
        """Convert a COD Metadata v2.0 dict to an Instance."""
        if (found_version := instance_dict.get("version")) != "2.0":
            logger.warning(f"Expected version 2.0, but got {found_version}")
        byte_offsets = (
            instance_dict["headers"]["start_byte"],
            instance_dict["headers"]["end_byte"],
        )
        # Create DicomMetadata using factory method (compressed string -> CompressedDicomMetadata)
        # Does NOT decompress during deserialization. Lazy loading.
        dicom_metadata = DicomMetadata.create(instance_dict["metadata"])
        return Instance(
            dicom_uri=instance_dict["uri"],
            uid_hash_func=uid_hash_func,
            _dicom_metadata=dicom_metadata,
            _byte_offsets=byte_offsets,
            _custom_offset_tables=instance_dict["offset_tables"],
            _size=instance_dict["size"],
            _crc32c=instance_dict["crc32c"],
            dependencies=instance_dict["dependencies"],
            _original_path=instance_dict["original_path"],
            _modified_datetime=instance_dict["modified_datetime"],
            _diff_hash_dupe_paths=instance_dict["diff_hash_dupe_paths"],
            _instance_uid=instance_dict["instance_uid"],
            _series_uid=instance_dict["series_uid"],
            _study_uid=instance_dict["study_uid"],
            _has_pixeldata=instance_dict["has_pixeldata"],
        )

    def cleanup(self):
        """
        Delete the temporary file, if it exists.
        """
        if self._temp_file_path and os.path.exists(self._temp_file_path):
            os.remove(self._temp_file_path)

    def __del__(self):
        """
        Custom destructor that calls self.cleanup()
        """
        self.cleanup()

    def __eq__(self, other):
        """We say that two instances are equal if they have the same UIDs."""
        # instance cannot be equal to non-instance
        if not isinstance(other, Instance):
            return False
        # TODO: should we not trust hints here? Leaning no b/c anyone who bothers implementing hints in their pipeline is aware fo the risk of false positives
        return (
            self.instance_uid(trust_hints_if_available=True)
            == other.instance_uid(trust_hints_if_available=True)
            and self.series_uid(trust_hints_if_available=True)
            == other.series_uid(trust_hints_if_available=True)
            and self.study_uid(trust_hints_if_available=True)
            == other.study_uid(trust_hints_if_available=True)
            and self.uid_hash_func == other.uid_hash_func
        )
