import dataclasses
import os
import re
from typing import Optional

from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject


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
        raise NotImplementedError("study level requests not yet supported")

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


def handle_dicomweb_request(request_str: str, client: storage.Client):
    """
    Handle a dicomweb request of format "GET {s}/studies/{study}/series/{series}"
    """
    request = DicomwebRequest.from_request(request_str)
    if request.frames:
        return _handle_frame_level_request(request, client)
    if request.instance_uid:
        return _handle_instance_level_request(
            datastore_uri=request.datastore_uri,
            study_uid=request.study_uid,
            series_uid=request.series_uid,
            instance_uid=request.instance_uid,
            client=client,
        )
    if "series" in request_dict:
        return _handle_series_level_request(
            datastore_uri=request_dict["datastore_uri"],
            study_uid=request_dict["studies"],
            series_uid=request_dict["series"],
            client=client,
        )
    if "studies" in request_dict:
        return _handle_study_level_request(
            datastore_uri=request_dict["datastore_uri"],
            study_uid=request_dict["studies"],
            client=client,
        )
    raise ValueError("Invalid request format")


def _handle_frame_level_request(request_dict: dict, client: storage.Client):
    raise NotImplementedError("frame level requests not yet supported")


def _handle_instance_level_request(
    datastore_uri: str,
    study_uid: str,
    series_uid: str,
    instance_uid: str,
    client: storage.Client,
):
    series_dict = _handle_series_level_request(
        datastore_uri, study_uid, series_uid, client
    )
    return series_dict["instances"][instance_uid]["metadata"]


def _handle_series_level_request(
    datastore_uri: str, study_uid: str, series_uid: str, client: storage.Client
):
    cod_obj = CODObject(
        datastore_path=datastore_uri,
        client=client,
        study_uid=study_uid,
        series_uid=series_uid,
        lock=False,
        create_if_missing=False,
    )
    return cod_obj.get_metadata(dirty=True).to_dict()["cod"]


def _handle_study_level_request(
    datastore_uri: str, study_uid: str, client: storage.Client
):
    # Parse the GCS URI into bucket and prefix
    study_uri = os.path.join(datastore_uri, "studies", study_uid)
    # Remove gs:// prefix and split into bucket and prefix
    path_without_prefix = study_uri.replace("gs://", "")
    bucket_name = path_without_prefix.split("/")[0]
    prefix = "/".join(path_without_prefix.split("/")[1:])

    # List blobs in the study directory
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    series_uids = [
        blob.name.split("/")[-1].rstrip(".tar")
        for blob in blobs
        if blob.name.endswith(".tar")
    ]
    study_dict = {}
    for series_uid in series_uids:
        study_dict[series_uid] = _handle_series_level_request(
            datastore_uri, study_uid, series_uid, client
        )
    return study_dict
