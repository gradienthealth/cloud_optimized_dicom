from apache_beam.metrics import Metrics

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
SERIES_DUPE_COUNTER = Metrics.counter(APPEND_NAMESPACE, "num_duplicate_series")
TRUE_DUPE_COUNTER = Metrics.counter(APPEND_NAMESPACE, "num_true_duplicates")
DIFFHASH_DUPE_COUNTER = Metrics.counter(APPEND_NAMESPACE, "num_diffhash_duplicates")
TAR_SUCCESS_COUNTER = Metrics.counter(APPEND_NAMESPACE, "tar_success")
TAR_BYTES_PROCESSED = Metrics.counter(APPEND_NAMESPACE, "tar_bytes_processed")
