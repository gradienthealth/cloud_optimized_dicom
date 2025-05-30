import dataclasses
import os
import re
from typing import Iterator, Optional

from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject


def _get_series_uid_from_blob_iterator(blobs: Iterator[storage.Blob]) -> str:
    """
    Get the series UID from the blob names.
    """
    # iterate through the blobs and attempt to extract the series uid
    while blob := next(blobs, None):
        if "/series/" not in blob.name:
            continue
        return _extract_from_uri(blob.name, "/series/").rstrip(".tar")
    # if we get to the end of the iterator, raise an error
    raise ValueError("No series UID found in blob names")


def _get_series_uid_for_study(
    datastore_uri: str, study_uid: str, client: storage.Client
) -> str:
    """Helper method that uses list_blobs to find a series uid that exists in the study"""
    # Parse the GCS URI into bucket and prefix
    study_uri = os.path.join(datastore_uri, "studies", study_uid)
    # Remove gs:// prefix and split into bucket and prefix
    path_without_prefix = study_uri.replace("gs://", "")
    bucket_name = path_without_prefix.split("/")[0]
    prefix = "/".join(path_without_prefix.split("/")[1:])

    # Use list_blobs to find a series uid that exists in the study
    bucket = client.bucket(bucket_name)
    return _get_series_uid_from_blob_iterator(bucket.list_blobs(prefix=prefix))


def is_valid_uid(uid: str) -> bool:
    """
    Validates if a string is a valid DICOM UID.
    A valid UID consists of numbers (can be multiple digits) separated by dots.
    Examples: "1.2.3", "1.234.5", "123.456.789"
    """
    pattern = r"^[0-9]+(\.[0-9]+)*$"
    return bool(re.match(pattern, uid))


def _extract_from_uri(uri: str, pattern: str) -> Optional[str]:
    """
    Helper method to extract a value from URI based on a pattern.
    Returns everything after the pattern but before the next /.
    If there is no next /, returns everything after the pattern.
    Returns None if pattern is not found.
    """
    if pattern not in uri:
        return None
    start = uri.find(pattern) + len(pattern)
    end = uri.find("/", start)
    return uri[start:end] if end != -1 else uri[start:]


# Reference: https://www.dicomstandard.org/docs/librariesprovider2/dicomdocuments/dicom/wp-content/uploads/2018/04/dicomweb-cheatsheet.pdf
STUDY_LEVEL_TAGS = [
    # Patient Module
    "00100010",  # PatientName
    "00100020",  # PatientID
    "00100030",  # PatientBirthDate
    "00100040",  # PatientSex
    # General Study Module
    "00080020",  # StudyDate
    "00080030",  # StudyTime
    "00080050",  # AccessionNumber
    "00080090",  # ReferringPhysicianName
    "0020000D",  # StudyInstanceUID
    "00081030",  # StudyDescription
]


@dataclasses.dataclass
class DicomwebRequest:
    """
    A dataclass representing a dicomweb request
    """

    datastore_uri: str
    study_uid: str
    series_uid: Optional[str] = None
    instance_uid: Optional[str] = None
    frames: Optional[list[int]] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        """
        Validate the request parameters, and raise an AssertionError if any are invalid.
        """
        assert is_valid_uid(self.study_uid), f"Invalid study UID: {self.study_uid}"
        if self.series_uid:
            assert is_valid_uid(
                self.series_uid
            ), f"Invalid series UID: {self.series_uid}"
        if self.instance_uid:
            assert is_valid_uid(
                self.instance_uid
            ), f"Invalid instance UID: {self.instance_uid}"

    def handle(self, client: storage.Client):
        """
        Handle the request and return the response.
        """
        if self.frames:
            return self._handle_frame_level_request(client)
        if self.instance_uid:
            return self._handle_instance_level_request(client)
        if self.series_uid:
            return self._handle_series_level_request(client)
        return self._handle_study_level_request(client)

    def _handle_frame_level_request(self, client: storage.Client):
        raise NotImplementedError("frame level requests not yet supported")

    def _handle_instance_level_request(self, client: storage.Client):
        """For an instance-level request, return the metadata for the instance"""
        cod_obj = CODObject(
            datastore_path=self.datastore_uri,
            client=client,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            lock=False,
            create_if_missing=False,
        )
        return cod_obj.get_metadata(dirty=True).to_dict()["cod"]["instances"][
            self.instance_uid
        ]["metadata"]

    def _handle_series_level_request(self, client: storage.Client):
        """For a series-level request, return a list of metadata for each instance"""
        cod_obj = CODObject(
            datastore_path=self.datastore_uri,
            client=client,
            study_uid=self.study_uid,
            series_uid=self.series_uid,
            lock=False,
            create_if_missing=False,
        )
        return [
            i_dict["metadata"]
            for i_dict in cod_obj.get_metadata(dirty=True)
            .to_dict()["cod"]["instances"]
            .values()
        ]

    def _handle_study_level_request(self, client: storage.Client):
        series_uid = _get_series_uid_for_study(
            self.datastore_uri, self.study_uid, client
        )
        cod_obj = CODObject(
            datastore_path=self.datastore_uri,
            client=client,
            study_uid=self.study_uid,
            series_uid=series_uid,
            lock=False,
            create_if_missing=False,
        )
        # return just the study level tags for the first instance in the series
        some_instance = next(iter(cod_obj.get_metadata(dirty=True).instances.values()))
        return {tag: some_instance.metadata.get(tag, None) for tag in STUDY_LEVEL_TAGS}

    @classmethod
    def from_uri(cls, uri: str) -> "DicomwebRequest":
        """
        Parse the URI of a dicomweb request (e.g. `{s}/studies/{study}/series/{series}`)
        and return a DicomwebRequest object.
        """
        assert uri.startswith("gs://"), "Only gs:// URIs are supported"
        assert "?" not in uri, "Query parameters are not supported"
        assert (
            "/studies/" in uri
        ), "study must be specified (expected '/studies/' in URI)"

        # Extract all fields using the helper method
        datastore_uri = uri.split("/studies/")[0]
        study_uid = _extract_from_uri(uri, "/studies/")
        series_uid = _extract_from_uri(uri, "/series/")
        instance_uid = _extract_from_uri(uri, "/instances/")
        frames_str = _extract_from_uri(uri, "/frames/")

        # Convert frames string to list of integers if present
        frames = [int(f) for f in frames_str.split(",")] if frames_str else []

        # right now, we only support metadata requests for non-frame-level requests
        if not frames:
            assert uri.endswith(
                "/metadata"
            ), "Expected /metadata suffix if request is not frame-level"

        return cls(
            datastore_uri=datastore_uri,
            study_uid=study_uid,
            series_uid=series_uid,
            instance_uid=instance_uid,
            frames=frames,
        )

    @classmethod
    def from_request(cls, request: str) -> "DicomwebRequest":
        """
        Parse the request string (e.g. `GET {s}/studies/{study}/series/{series}`)
        and return a DicomwebRequest object.
        """
        assert request.startswith("GET "), "Only GET requests are currently supported"
        uri = request.replace("GET", "").strip()
        return cls.from_uri(uri)


# public method to expose (really the only thing that should be used/imported)
def handle_request(request: str, client: storage.Client) -> dict:
    """
    Handle a dicomweb request and return the response.
    """
    return DicomwebRequest.from_request(request).handle(client)
