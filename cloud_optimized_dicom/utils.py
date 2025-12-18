DICOM_PREAMBLE = b"\x00" * 128 + b"DICM"
REMOTE_IDENTIFIERS = ["http", "s3://", "gs://"]
UID_TAGS = {
    "instance_uid": "00080018",
    "series_uid": "0020000E",
    "study_uid": "0020000D",
}

import collections
import io
from base64 import b64encode
from typing import Optional

import cv2
import filetype
import google_crc32c
import numpy as np
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

import cloud_optimized_dicom.metrics as metrics
from cloud_optimized_dicom.config import logger
from cloud_optimized_dicom.errors import CleanOpOnUnlockedCODObjectError


def find_pattern(f: io.BufferedReader, pattern: bytes, buffer_size=8192):
    """
    Finds the pattern from file like object and gives index found or returns -1
    """
    assert len(pattern) < buffer_size
    size = len(pattern)
    overlap_size = size - 1
    start_position = f.tell()
    windowed_bytes = bytearray(buffer_size)

    # Read the initial buffer
    while num_bytes := f.readinto(windowed_bytes):
        # Search for the pattern in the current byte window
        index = windowed_bytes.find(pattern)
        if index != -1:
            # found the index, return the relative position
            return f.tell() - start_position - num_bytes + index

        # If the data is smaller than buffer size, this is the last
        # loop and should break.
        if num_bytes < buffer_size:
            break

        # Back seek to allow for window overlap
        f.seek(-overlap_size, 1)
    return -1


def is_remote(uri: str) -> bool:
    """
    Check if the URI is remote.
    """
    return any(uri.startswith(prefix) for prefix in REMOTE_IDENTIFIERS)


def upload_and_count_file(blob: storage.Blob, file_path: str):
    """Wraps `blob.upload_from_filename` with the proper beam creation counter metric"""
    blob.upload_from_filename(file_path, retry=DEFAULT_RETRY)
    metrics.STORAGE_CLASS_COUNTERS["CREATE"][blob.storage_class].inc()


def upload_and_count_bytes(blob: storage.Blob, bytes: bytes):
    """Wraps `blob.upload_from_string` with the proper beam creation counter metric"""
    blob.upload_from_string(bytes, retry=DEFAULT_RETRY)
    metrics.STORAGE_CLASS_COUNTERS["CREATE"][blob.storage_class].inc()


def _delete_gcs_dep(uri: str, client: storage.Client, expected_crc32c: str = None):
    """
    Delete a dependency from GCS.
    Args:
        uri: str - The URI of the dependency to delete.
        client: storage.Client - The client to use to delete the blob.
        expected_crc32c: str - The expected CRC32C of the blob. If provided, the blob will be validated against this value before deletion.
    Returns:
        bool - Whether the blob was deleted.
    """
    blob = storage.Blob.from_string(uri, client=client)
    if not blob.exists():
        metrics.DEP_DOES_NOT_EXIST.inc()
        logger.warning(f"Skipping deletion of {uri} due to non-existence")
        return False
    # validate crc32c if expected hash was provided
    if expected_crc32c:
        blob.reload()
        if blob.crc32c != expected_crc32c:
            metrics.INSTANCE_BLOB_CRC32C_MISMATCH.inc()
            logger.warning(
                f"Skipping deletion of {uri} due to hash mismatch (expected {expected_crc32c}, got {blob.crc32c})"
            )
            return False
    # If we get here, none of the early exit conditions were met, so we can delete the file
    blob.delete(retry=DEFAULT_RETRY)
    metrics.NUM_DELETES.inc()
    return True


def file_is_dicom(instance_path: str) -> bool:
    """use filetype.guess to verify {instance_path} is indeed dicom"""
    guess_result = filetype.guess(instance_path)
    if guess_result is None or guess_result.mime != "application/dicom":
        return False
    return True


def delete_uploaded_blobs(client: storage.Client, uris_to_delete: list[str]):
    """
    Helper method used by tests to delete blobs they have created, resetting the test
    environment for a subsequent test. Takes a GCS client and a list of GCS uris to delete.
    These URIs should be folders (e.g. 'gs://siskin-172863-test-data/concat-output'), and
    this method will delete everything in the folder
    """
    from google.api_core.exceptions import NotFound

    for gcs_uri in uris_to_delete:
        bucket_name, folder_name = gcs_uri.replace("gs://", "").split("/", 1)
        for blob in client.list_blobs(bucket_name, prefix=f"{folder_name}/"):
            try:
                blob.delete()
            except NotFound:
                # Blob already deleted or doesn't exist - this is fine since
                # the goal is to ensure the blob doesn't exist
                pass


def generate_ptr_crc32c(ptr: io.BufferedReader, blocksize: int = 2**20) -> str:
    """
    Modified from stackoverflow: https://stackoverflow.com/questions/37367741/difficulty-comparing-generated-and-google-cloud-storage-provided-crc32c-checksum
    Generate a base64 encoded crc32c checksum for a file to compare with google cloud storage.

    Returns a string like "4jvPnQ=="

    Compare with a google storage blob instance:
      blob.crc32c == generate_ptr_crc32c(open("path/to/local/file.txt", "rb"))
    """
    crc = google_crc32c.Checksum()
    collections.deque(crc.consume(ptr, blocksize), maxlen=0)
    return b64encode(crc.digest()).decode("utf-8")


def parse_uids_from_metadata(
    metadata: dict[str, dict],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Given an instance metadata dict, return the instance, series, and study uids if they can be found, or none if not"""
    instance_uid = metadata.get(UID_TAGS["instance_uid"], {}).get("Value", [None])[0]
    series_uid = metadata.get(UID_TAGS["series_uid"], {}).get("Value", [None])[0]
    study_uid = metadata.get(UID_TAGS["study_uid"], {}).get("Value", [None])[0]
    return instance_uid, series_uid, study_uid


def read_thumbnail_into_array(thumbnail_path: str) -> np.ndarray:
    """Read a thumbnail from disk into a numpy array.

    Args:
        thumbnail_path: str - The path to the thumbnail on disk.

    Returns:
        np.ndarray - The thumbnail as a numpy array. The shape of the array is `(N, H, W, 3)` for a video, or `(H, W, 3)` for a single frame image.

    Raises:
        ValueError: If reading the thumbnail fails for any reason (e.g. file not found, invalid format, etc.)
    """
    if thumbnail_path.endswith(".mp4"):
        cap = cv2.VideoCapture(thumbnail_path)
        if not cap.isOpened():
            raise ValueError(f"Failed to open video at {thumbnail_path}")

        frames = []
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            frames.append(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        cap.release()

        if not frames:
            raise ValueError(f"No frames extracted from video at {thumbnail_path}")
        return np.stack(frames, axis=0)  # Shape: (N, H, W, 3)
    elif thumbnail_path.endswith(".jpg"):
        img = cv2.imread(thumbnail_path, cv2.IMREAD_COLOR)
        if img is None:
            raise ValueError(f"Failed to read image at {thumbnail_path}")
        return cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    else:
        raise ValueError(f"Unsupported thumbnail format: {thumbnail_path}")


def public_method(func):
    """Decorator for public CODObject methods.
    Enforces that clean operations require a lock, and warns about dirty operations on locked objects.
    """

    def wrapper(self, *args, **kwargs):
        dirty = kwargs.get("dirty", False)
        if not dirty:
            if not self.lock:
                raise CleanOpOnUnlockedCODObjectError(
                    "Cannot perform clean operation on unlocked CODObject"
                )
        elif self.lock:
            logger.warning(
                f"Performing dirty operation '{func.__name__}' on locked CODObject: {self}"
            )
        return func(self, *args, **kwargs)

    return wrapper
