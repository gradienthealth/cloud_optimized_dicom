DICOM_PREAMBLE = b"\x00" * 128 + b"DICM"
REMOTE_IDENTIFIERS = ["http", "s3://", "gs://"]


import io
import logging

from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

import cloud_optimized_dicom.metrics as metrics

logger = logging.getLogger(__name__)


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
        logger.warning(f"DEPENDENCY_DELETION:SKIP:FILE_DOES_NOT_EXIST:{uri}")
        return False
    # validate crc32c if expected hash was provided
    if expected_crc32c:
        blob.reload()
        if blob.crc32c != expected_crc32c:
            metrics.INSTANCE_BLOB_CRC32C_MISMATCH.inc()
            logger.warning(f"DEPENDENCY_DELETION:SKIP:FILE_HASH_MISMATCH:{uri}")
            return False
    # If we get here, none of the early exit conditions were met, so we can delete the file
    blob.delete(retry=DEFAULT_RETRY)
    metrics.NUM_DELETES.inc()
    return True
