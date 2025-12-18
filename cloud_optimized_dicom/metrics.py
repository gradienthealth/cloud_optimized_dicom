from apache_beam.metrics import Metrics
from google.cloud.storage.constants import (
    ARCHIVE_STORAGE_CLASS,
    COLDLINE_STORAGE_CLASS,
    NEARLINE_STORAGE_CLASS,
    STANDARD_STORAGE_CLASS,
)

NAMESPACE = "cloud_optimized_dicom"

# deletion metrics
DELETION_NAMESPACE = f"{NAMESPACE}:deletion"
NUM_DELETES = Metrics.counter(DELETION_NAMESPACE, "num_deletes")
BYTES_DELETED_COUNTER = Metrics.counter(DELETION_NAMESPACE, "bytes_deleted")
DEP_DOES_NOT_EXIST = Metrics.counter(DELETION_NAMESPACE, "dep_does_not_exist")
INSTANCE_BLOB_CRC32C_MISMATCH = Metrics.counter(
    DELETION_NAMESPACE, "instance_blob_crc32c_mismatch"
)

# append metrics
APPEND_NAMESPACE = f"{NAMESPACE}:append"
APPEND_CONFLICTS = Metrics.counter(APPEND_NAMESPACE, "append_conflicts")
APPEND_DUPLICATES = Metrics.counter(APPEND_NAMESPACE, "append_duplicates")
APPEND_FAILS = Metrics.counter(APPEND_NAMESPACE, "append_fails")
APPEND_SUCCESSES = Metrics.counter(APPEND_NAMESPACE, "append_successes")
SERIES_DUPE_COUNTER = Metrics.counter(APPEND_NAMESPACE, "num_full_duplicate_series")
TAR_SUCCESS_COUNTER = Metrics.counter(APPEND_NAMESPACE, "tar_success")
TAR_BYTES_PROCESSED = Metrics.counter(APPEND_NAMESPACE, "tar_bytes_processed")
TOTAL_FILES_PROCESSED = Metrics.counter(APPEND_NAMESPACE, "total_files_processed")

# Storage class counters
STD_CREATE_COUNTER = Metrics.counter(__name__, "num_STANDARD_creates")
STD_GET_COUNTER = Metrics.counter(__name__, "num_STANDARD_gets")
NEARLINE_CREATE_COUNTER = Metrics.counter(__name__, "num_NEARLINE_creates")
NEARLINE_GET_COUNTER = Metrics.counter(__name__, "num_NEARLINE_gets")
COLDLINE_CREATE_COUNTER = Metrics.counter(__name__, "num_COLDLINE_creates")
COLDLINE_GET_COUNTER = Metrics.counter(__name__, "num_COLDLINE_gets")
ARCHIVE_CREATE_COUNTER = Metrics.counter(__name__, "num_ARCHIVE_creates")
ARCHIVE_GET_COUNTER = Metrics.counter(__name__, "num_ARCHIVE_gets")
# Storage class counter mappings
STORAGE_CLASS_COUNTERS: dict[str, dict[str, Metrics.DelegatingCounter]] = {
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
DEPS_MISSING_FROM_TAR = Metrics.counter(__name__, "deps_missing_from_tar")
TAR_METADATA_CRC32C_MISMATCH = Metrics.counter(__name__, "tar_metadata_crc32c_mismatch")
DEP_DOES_NOT_EXIST = Metrics.counter(__name__, "dep_does_not_exist")
INSTANCE_BLOB_CRC32C_MISMATCH = Metrics.counter(
    __name__, "instance_blob_crc32c_mismatch"
)
INSTANCES_NOT_FOUND = Metrics.counter(__name__, "instances_not_found")
NUM_FILES_DELETED = Metrics.counter(__name__, "num_files_deleted")

# thumbnail metrics
THUMBNAIL_NAMESPACE = f"{NAMESPACE}:thumbnail"
SERIES_MISSING_PIXEL_DATA = Metrics.counter(
    THUMBNAIL_NAMESPACE, "series_missing_pixel_data"
)
THUMBNAIL_SUCCESSES = Metrics.counter(THUMBNAIL_NAMESPACE, "thumbnail_successes")
THUMBNAIL_FAILS = Metrics.counter(THUMBNAIL_NAMESPACE, "thumbnail_fails")
THUMBNAIL_BYTES_PROCESSED = Metrics.counter(
    THUMBNAIL_NAMESPACE, "thumbnail_bytes_processed"
)

# metadata caching metrics
METADATA_NAMESPACE = f"{NAMESPACE}:metadata"
METADATA_CACHE_HITS = Metrics.counter(METADATA_NAMESPACE, "cache_hits")
METADATA_CACHE_MISSES = Metrics.counter(METADATA_NAMESPACE, "cache_misses")
