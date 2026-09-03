try:
    from apache_beam.metrics import Metrics

    _BEAM_AVAILABLE = True
except ImportError:
    _BEAM_AVAILABLE = False

from google.cloud.storage.constants import (
    ARCHIVE_STORAGE_CLASS,
    COLDLINE_STORAGE_CLASS,
    NEARLINE_STORAGE_CLASS,
    STANDARD_STORAGE_CLASS,
)

from cloud_optimized_dicom.transcode import TranscodeOutcome


class _NoOpCounter:
    """Drop-in replacement for Beam counters when apache-beam is not installed."""

    def inc(self, n=1):
        pass


def _counter(namespace, name):
    if _BEAM_AVAILABLE:
        return Metrics.counter(namespace, name)
    return _NoOpCounter()


NAMESPACE = "cloud_optimized_dicom"

# deletion metrics
DELETION_NAMESPACE = f"{NAMESPACE}:deletion"
NUM_DELETES = _counter(DELETION_NAMESPACE, "num_deletes")
BYTES_DELETED_COUNTER = _counter(DELETION_NAMESPACE, "bytes_deleted")
DEP_DOES_NOT_EXIST = _counter(DELETION_NAMESPACE, "dep_does_not_exist")
INSTANCE_BLOB_CRC32C_MISMATCH = _counter(
    DELETION_NAMESPACE, "instance_blob_crc32c_mismatch"
)

# append metrics
APPEND_NAMESPACE = f"{NAMESPACE}:append"
APPEND_CONFLICTS = _counter(APPEND_NAMESPACE, "append_conflicts")
APPEND_DUPLICATES = _counter(APPEND_NAMESPACE, "append_duplicates")
APPEND_FAILS = _counter(APPEND_NAMESPACE, "append_fails")
APPEND_SUCCESSES = _counter(APPEND_NAMESPACE, "append_successes")
SERIES_DUPE_COUNTER = _counter(APPEND_NAMESPACE, "num_full_duplicate_series")
TAR_SUCCESS_COUNTER = _counter(APPEND_NAMESPACE, "tar_success")
TAR_BYTES_PROCESSED = _counter(APPEND_NAMESPACE, "tar_bytes_processed")
TOTAL_FILES_PROCESSED = _counter(APPEND_NAMESPACE, "total_files_processed")
# What the frame re-encode in `Instance.compress` did, one counter per
# `TranscodeOutcome`, plus the file bytes it saved across the instances it
# rewrote.
TRANSCODE_OUTCOME_COUNTERS = {
    outcome: _counter(APPEND_NAMESPACE, f"transcode_{outcome.value}")
    for outcome in TranscodeOutcome
}
TRANSCODE_BYTES_SAVED = _counter(APPEND_NAMESPACE, "transcode_bytes_saved")

# Storage class counters
STD_CREATE_COUNTER = _counter(__name__, "num_STANDARD_creates")
STD_GET_COUNTER = _counter(__name__, "num_STANDARD_gets")
NEARLINE_CREATE_COUNTER = _counter(__name__, "num_NEARLINE_creates")
NEARLINE_GET_COUNTER = _counter(__name__, "num_NEARLINE_gets")
COLDLINE_CREATE_COUNTER = _counter(__name__, "num_COLDLINE_creates")
COLDLINE_GET_COUNTER = _counter(__name__, "num_COLDLINE_gets")
ARCHIVE_CREATE_COUNTER = _counter(__name__, "num_ARCHIVE_creates")
ARCHIVE_GET_COUNTER = _counter(__name__, "num_ARCHIVE_gets")
# Storage class counter mappings
STORAGE_CLASS_COUNTERS: dict[str, dict] = {
    "GET": {
        STANDARD_STORAGE_CLASS: STD_GET_COUNTER,
        NEARLINE_STORAGE_CLASS: NEARLINE_GET_COUNTER,
        COLDLINE_STORAGE_CLASS: COLDLINE_GET_COUNTER,
        ARCHIVE_STORAGE_CLASS: ARCHIVE_GET_COUNTER,
    },
    "CREATE": {
        STANDARD_STORAGE_CLASS: STD_CREATE_COUNTER,
        NEARLINE_STORAGE_CLASS: NEARLINE_CREATE_COUNTER,
        COLDLINE_STORAGE_CLASS: COLDLINE_CREATE_COUNTER,
        ARCHIVE_STORAGE_CLASS: ARCHIVE_CREATE_COUNTER,
    },
}

# deletion metrics
DEPS_MISSING_FROM_TAR = _counter(__name__, "deps_missing_from_tar")
TAR_METADATA_CRC32C_MISMATCH = _counter(__name__, "tar_metadata_crc32c_mismatch")
DEP_DOES_NOT_EXIST = _counter(__name__, "dep_does_not_exist")
INSTANCE_BLOB_CRC32C_MISMATCH = _counter(__name__, "instance_blob_crc32c_mismatch")
INSTANCES_NOT_FOUND = _counter(__name__, "instances_not_found")
NUM_FILES_DELETED = _counter(__name__, "num_files_deleted")

# thumbnail metrics
THUMBNAIL_NAMESPACE = f"{NAMESPACE}:thumbnail"
SERIES_MISSING_PIXEL_DATA = _counter(THUMBNAIL_NAMESPACE, "series_missing_pixel_data")
THUMBNAIL_SUCCESSES = _counter(THUMBNAIL_NAMESPACE, "thumbnail_successes")
THUMBNAIL_FAILS = _counter(THUMBNAIL_NAMESPACE, "thumbnail_fails")
THUMBNAIL_BYTES_PROCESSED = _counter(THUMBNAIL_NAMESPACE, "thumbnail_bytes_processed")

# metadata caching metrics
METADATA_NAMESPACE = f"{NAMESPACE}:metadata"
METADATA_CACHE_HITS = _counter(METADATA_NAMESPACE, "cache_hits")
METADATA_CACHE_MISSES = _counter(METADATA_NAMESPACE, "cache_misses")
